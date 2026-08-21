import os 
import requests
import boto3
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator 
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
import pendulum

project_root = "/opt/airflow/project"
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')
S3_BUCKET = os.getenv('S3_BUCKET')
DEFAULT_CATEGORY_ID = 316

def alert_on_failure(context):
    task_id = context['task_instance'].task_id
    dag_id = context['task_instance'].dag_id
    execution_date = context['ds']
    log_url = context['task_instance'].log_url

    message = {
        'text': f":red_circle: *Task Failed*\n"
                f"*DAG:* {dag_id}\n"
                f"*Task:* {task_id}\n"
                f"*Date:* {execution_date}\n"
                f"*Log:* {log_url}\n"
    }

    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL not set, skipping alert")
        return False

    try:
        requests.post(SLACK_WEBHOOK_URL, json=message, timeout=10)
    except requests.RequestException as e:
        print(f"Failed to send Slack alert: {e}")

def check_s3_data_exists(**context):
    run_date = context['ds']
    s3_key = f"bronze/tiki/{run_date}/{DEFAULT_CATEGORY_ID}.json"
    s3_client = boto3.client('s3')

    try:
        s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
        print(f"Found data at s3://{S3_BUCKET}/{s3_key}")
    except s3_client.exceptions.ClientError:
        raise AirflowFailException(
            f"No data found at s3://{S3_BUCKET}/{s3_key}. "
            f"Extract must be run locally first (Tiki API blocks Codespaces/cloud IPs with 403)."
        )
    
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': alert_on_failure,
}

with DAG(
    dag_id='tiki-pipeline-orchestrator',
    default_args=default_args,
    description='Daily Pipeline to crawl data, transform',
    start_date=pendulum.datetime(2026, 8, 17, tz='Asia/Ho_Chi_Minh'),
    schedule='0 1 * * *', # chạy 1h sáng, trễ hơn giờ chạy extract local để có buffer
    catchup=False,
) as dag:

    #task 1: extract + ingest data vào S3
    task_1 = PythonOperator(
        task_id='check_extracted_data',
        python_callable=check_s3_data_exists,
    )

    #task 2: dbt transform
    task_2 = BashOperator(
        task_id='dbt_transform',
        bash_command=f"cd {project_root}/dbt && /home/airflow/dbt_venv/bin/dbt run --profiles-dir ."
    )
    
    task_1 >> task_2
