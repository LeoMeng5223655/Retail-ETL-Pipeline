import boto3
import time
import subprocess
from send_email import send_email
import json

def lambda_handler(event, context):
    s3_file_list = []
    
    s3_client=boto3.client('s3')
    for object in s3_client.list_objects_v2(Bucket='etl-bucket-leo')['Contents']:
        s3_file_list.append(object['Key'])
    print("s3_file_list:", s3_file_list)
    
    datestr = time.strftime("%Y%m%d")
    
    required_file_list = [f'calendar_{datestr}.csv', f'inventory_{datestr}.csv', f'product_{datestr}.csv', f'sales_{datestr}.csv', f'store_{datestr}.csv' ]
    print("required_file_list:", required_file_list)
    # scan S3 bucket
    if set(required_file_list).issubset(set(s3_file_list)):
        s3_file_url = ['s3://' + 'etl-bucket-leo/' + a for a in s3_file_list]
        print("s3_file_url:", s3_file_url)
        table_name = [a[:-13] for a in s3_file_list]  
        print("table_name:", table_name)
    
        data = json.dumps({'conf': dict(zip(table_name, s3_file_url))})
        print("data:", data)
   
   
   
    # send signal to Airflow    
        endpoint= 'http://44.215.211.26:8080/api/v1/dags/aws_project/dagRuns'
    
        subprocess.run([
            'curl',
            '-X', 
            'POST', 
            endpoint, 
            '-H'
            'Content-Type: application/json', 
            '--user',
            'airflow:airflow',
            '--data', 
            data])
        print('File are send to Airflow')
    else:
        send_email()
