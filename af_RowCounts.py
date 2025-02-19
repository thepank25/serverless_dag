import json
import csv
import boto3
from io import StringIO

s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket = event['bucket']
    key = event['key']
    
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    
    csv_reader = csv.DictReader(StringIO(content))
    total = sum(int(row['item']) for row in csv_reader)
    
    return total
    