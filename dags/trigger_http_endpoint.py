from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('backend_service_scheduler', schedule_interval='*/5 18,19,20 * * *', default_args=default_args)

task = SimpleHttpOperator(
    task_id='trigger_backend_job',
    method='GET',
    endpoint='api/1.0/purpose',
    data={"params": "value"},
    headers={},
    response_check=lambda response: response.raise_for_status(),
    dag=dag)
