from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import airflow.hooks.S3_hook
from datetime import timedelta, datetime

default_args = {
    'owner': 'damien',
    'start_date': datetime(2019, 1, 1),
    'retry_delay': timedelta(minutes=5)
}


def upload_file_to_s3_with_hook(filename, key, bucket_name):
    hook = airflow.hooks.S3_hook.S3Hook('my_S3_conn')
    hook.load_file(filename, key, bucket_name)


# Using the context manager alllows you not to duplicate the dag parameter in each operator
with DAG('S3_dag_test', default_args=default_args, schedule_interval='@once') as dag:
    start_task = DummyOperator(
        task_id='dummy_start'
    )

    upload_to_S3_task = PythonOperator(
        task_id='upload_to_S3',
        python_callable=upload_file_to_s3_with_hook,
        op_kwargs={
            'filename': 'path/to/my_file.csv',
            'key': 'my_S3_file.csv',
            'bucket_name': 'my-S3-bucket',
        })

    # Use arrows to set dependencies between tasks
    start_task >> upload_to_S3_task

# download_data -> send_data_to_processing -> monitor_processing -> generate_report -> send_email
