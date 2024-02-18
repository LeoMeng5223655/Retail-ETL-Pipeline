import airflow
from airflow import DAG
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor



SPARK_STEPS = [
    {
        'Name': 'aws_project',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/bin/spark-submit',
                '--master', 'yarn',
                '--deploy-mode', 'cluster',
                's3://aws-project-artifacts-24/data_trans.py',
                '{{ ds }}',
                '--sales', "{{ task_instance.xcom_pull('parse_request', key='calendar') }}",
                '--inventory', "{{ task_instance.xcom_pull('parse_request', key='inventory') }}",
                '--product', "{{ task_instance.xcom_pull('parse_request', key='product') }}",
                '--sales', "{{ task_instance.xcom_pull('parse_request', key='sales') }}",
                '--store', "{{ task_instance.xcom_pull('parse_request', key='store') }}",
                
            ]
        }
    }

]

CLUSTER_ID = "j-1NDQ7COQPA0WO"

DEFAULT_ARGS = {
    'owner': 'wcd_data_engineer',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['airflow_data_eng@wcd.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

# Retrive the data from s3 bucket
def retrieve_s3_files(**kwargs):
    dag_run = kwargs['dag_run']
    conf = dag_run.conf

    for key, value in conf.items():
        kwargs['ti'].xcom_push(key=key, value=value)

# Scheduled by Lambda 
dag = DAG(
    'aws_project',
    default_args = DEFAULT_ARGS,
    dagrun_timeout = timedelta(hours=2),
    schedule_interval = None 
)

parse_request = PythonOperator(task_id = 'parse_request',
                                provide_context = True, # Airflow will pass a set of keyword arguments that can be used in the function
                                python_callable = retrieve_s3_files,
                                dag = dag
                                ) 

step_adder = EmrAddStepsOperator(
    task_id = 'add_steps',
    job_flow_id = CLUSTER_ID,
    aws_conn_id = "aws_default",
    steps = SPARK_STEPS,
    dag = dag
)
# check if the EMR is running properly
step_checker = EmrStepSensor(
    task_id = 'watch_step',
    job_flow_id = CLUSTER_ID,
    step_id = "{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
    aws_conn_id = "aws_default", 
    dag = dag
)

parse_request >> step_adder >> step_checker
