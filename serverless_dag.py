from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from airflow.models.xcom_arg import XComArg

def choose_lambda_function(**kwargs):
    metadata = kwargs['ti'].xcom_pull(task_ids='invoke_parse_metadata')
    memory_size = metadata.get('memory_size', 0)
    
    if memory_size < 128:
        return 'invoke_small_memory_lambda'
    elif memory_size < 512:
        return 'invoke_medium_memory_lambda'
    else:
        return 'invoke_large_memory_lambda'

def aggregate_results(**kwargs):
    ti = kwargs['ti']
    small_result = ti.xcom_pull(task_ids='invoke_small_memory_lambda')
    medium_result = ti.xcom_pull(task_ids='invoke_medium_memory_lambda')
    large_result = ti.xcom_pull(task_ids='invoke_large_memory_lambda')
    
    total = sum(filter(None, [small_result, medium_result, large_result]))
    ti.xcom_push(key='total', value=total)

def extract_files(**kwargs):
    ti = kwargs['ti']
    file_list_str = ti.xcom_pull(task_ids='get_file_list')
    file_list = json.loads(file_list_str)  # Parse the JSON string
    print(file_list)
    return file_list['files']

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='lambda_memory_based_routing',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    import json

    get_file_list = LambdaInvokeFunctionOperator(
        task_id='get_file_list',
        function_name='get_file_list',
        payload=json.dumps({"bucket": "credence-core-raw", "prefix": r"serverless_dag/data"}).encode('utf-8'),
        aws_conn_id='aws_default',
    )

    extract_files_task = PythonOperator(
        task_id='extract_files',
        python_callable=extract_files,
        provide_context=True,
    )

    files = XComArg(extract_files_task)

    with TaskGroup(group_id='process_files') as process_files:
        invoke_parse_metadata = LambdaInvokeFunctionOperator.partial(
            task_id='invoke_parse_metadata',
            function_name='lambda_function_size',
            aws_conn_id='aws_default',
        ).expand(payload=files.map(lambda file: json.dumps({"bucket": "credence-core-raw",
                                                            "key": file})))  # Ensure payload is JSON

        branch_task = BranchPythonOperator.partial(
            task_id='branch_task',
            python_callable=choose_lambda_function,
        ).expand(op_kwargs={'files': files})

        invoke_small_memory_lambda = LambdaInvokeFunctionOperator.partial(
            task_id='invoke_small_memory_lambda',
            function_name='small_memory_lambda',
            aws_conn_id='aws_default',
            trigger_rule=TriggerRule.NONE_FAILED,
        ).expand(payload=files)

        invoke_medium_memory_lambda = LambdaInvokeFunctionOperator.partial(
            task_id='invoke_medium_memory_lambda',
            function_name='medium_memory_lambda',
            aws_conn_id='aws_default',
            trigger_rule=TriggerRule.NONE_FAILED,
        ).expand(payload=files)

        invoke_large_memory_lambda = LambdaInvokeFunctionOperator.partial(
            task_id='invoke_large_memory_lambda',
            function_name='large_memory_lambda',
            aws_conn_id='aws_default',
            trigger_rule=TriggerRule.NONE_FAILED,
        ).expand(payload=files)

        aggregate_task = PythonOperator.partial(
            task_id='aggregate_results',
            python_callable=aggregate_results,
            trigger_rule=TriggerRule.ALL_DONE,
        ).expand(op_kwargs={'files': files})

        invoke_parse_metadata >> branch_task
        branch_task >> [invoke_small_memory_lambda, invoke_medium_memory_lambda, invoke_large_memory_lambda]
        [invoke_small_memory_lambda, invoke_medium_memory_lambda, invoke_large_memory_lambda] >> aggregate_task

    invoke_final_lambda = LambdaInvokeFunctionOperator(
        task_id='invoke_final_lambda',
        function_name='final_lambda',
        aws_conn_id='aws_default',
        payload='{{ ti.xcom_pull(task_ids="aggregate_results", key="total") }}',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    get_file_list >> extract_files_task >> process_files >> invoke_final_lambda