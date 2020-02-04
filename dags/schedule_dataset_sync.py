from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    # 'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

synced_products = 'ls8_nbar_scene,ls7_nbar_scene,ls8_nbart_scene,ls7_nbart_scene,ls8_pq_scene,ls7_pq_scene,ls8_pq_legacy_scene,ls7_pq_legacy_scene'.split(',')

SYNC_CMD = '''execute_sync --dea-module ${self:provider.environment.DEA_MODULE}
--queue ${self: provider.environment.QUEUE}
--project ${self: provider.environment.PROJECT}
--year % (time_range) s
--path % (path) s
--suffixpath % (suffixpath) s
--product % (product) s
--trasharchived % (trasharchived)
'''

INGEST = '''execute_ingest --dea-module ${self:provider.environment.DEA_MODULE}
--queue ${self: provider.environment.QUEUE}
--project ${self: provider.environment.PROJECT}
--stage ${self: custom.Stage}
--year % (year)
--product % (product)'''

ingest_products = 'ls8_nbar_albers,ls7_nbar_albers,ls8_nbart_albers,ls7_nbart_albers,ls8_pq_albers,ls7_pq_albers'

dag = DAG('schedule_sync_jobs',
          default_args=default_args,
          catchup=False,
          schedule_interval=timedelta(days=1))

for product in synced_products:
    # TODO setup workdir

    submit_sync = SSHOperator(task_id='submit_sync',
                                   ssh_conn_id='lpgs_gadi',
                                   command='qstat -xf -F json',
                                   do_xcom_push=True,
                                   dag=dag)


    # TODO Implement an SSH Sensor to wait for the submitted job to be done
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
        dag=dag)

    get_qstat_output >> t1