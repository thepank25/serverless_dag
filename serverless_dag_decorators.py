from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
import json
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import get_current_context
import logging

logger = logging.getLogger()

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='lambda_memory_based_routing_decorators',
    default_args=default_args,
    schedule_interval=None,
    catchup=False) as dag:

    list_filenames = S3ListOperator(
        task_id="list_filenames",
        bucket="credence-core-raw",
        prefix=r"serverless_dag/data",
    )

    @task(task_id='get_file_size')
    def get_file_size(aws_conn_id, bucket, filename):
        hook = S3Hook(aws_conn_id=aws_conn_id)
        logging.info(f"Getting file size for {filename}")
        return {filename: hook.get_key(filename, bucket).content_length}

    @task(task_id='add_lines')
    def total(lines):
        
        logging.info(f"Adding lines {lines}")
        return sum(int(line) for line in lines) # for line in json.loads(lines['body'])['total'])

    @task(map_index_template = """{{ filename }}""")
    def choose_lambda_function(file):        

        filename, filesize = list(file.items())[0]        
        context = get_current_context()
        context["filename"] = filename
        d= {
            "payload" :
              json.dumps({
                'bucket': list_filenames.bucket,
                'key': filename,
               'filesize': filesize})            
                             }
        if filesize < 10000:
            d['function_name'] = 'invoke_small_memory_lambda'

        elif filesize < 20000:
            d['function_name'] = 'invoke_medium_memory_lambda'
        else:
            d['function_name'] = 'invoke_large_memory_lambda'
        return d

    file_sizes = get_file_size.partial(
        aws_conn_id="aws_default", 
        bucket=list_filenames.bucket,
        # map_index_template = """{{task.parameters['filename']}}"""
        ).expand(
        filename=list_filenames.output,
        # parameters = [{'filename': filename} for filename in list_filenames.output]
    )

    with TaskGroup(group_id='process_files') as process_files:
        
        branch_task = choose_lambda_function.expand(file = file_sizes) 

        invoke_lambda_operators = LambdaInvokeFunctionOperator.partial(
            task_id = 'call_lambdas',
            aws_conn_id = 'aws_default',
        ).expand_kwargs(branch_task)

        branch_task >> invoke_lambda_operators 

    aggregate_task =  total(lines=invoke_lambda_operators.output)

    list_filenames >> file_sizes >> process_files >> aggregate_task