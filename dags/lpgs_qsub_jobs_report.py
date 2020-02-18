from datetime import timedelta
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.utils.dates import days_ago

sshHook = SSHHook(ssh_conn_id='lpgs_nci')

default_args = {
    'owner': 'damien',
    'start_date': days_ago(2),
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'testing_ssh_qstat',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

# TODO: This could filter out jobs completed outside this period
t1 = SSHOperator(
    task_id="task1",
    command='qstat -x -f -F json',
    # ssh_hook=sshHook,
    ssh_conn_id='lpgs-nci',
    dag=dag)

example_bash_task = BashOperator(
    task_id='example_bash_task',
    bash_command='echo {{ task_instance.xcom_pull(task_ids="get_files") }}',
)


def my_python_callable(*args, **kwargs):
    print('foo')


put_into_postgres = PythonOperator(
    task_id="save_qstat_to_postgres",
    python_callable=my_python_callable
)
