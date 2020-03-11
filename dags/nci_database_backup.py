from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator

from datetime import datetime, timedelta

default_args = {
    'owner': 'Damien Ayers',
    'depends_on_past': False,  # Very important, will cause a single failure to propagate forever
    'start_date': datetime(2020, 3, 11),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'timeout': 3600,  # For running SSH Commands
    'params': {
        'project': 'v10',
        'queue': 'normal',
        'module': 'dea/unstable',
        'year': '2019'
    }
}

dag = DAG(
    'nci_database_backup',
    default_args=default_args,
    catchup=False,
    schedule_interval=None,
)

with dag:
    run_backup = SSHOperator(
        task_id='execute_daily_backup',
        ssh_conn_id='lpgs_gadi',
        command="""
        cd /g/data/v10/agdc/backup;
        ./trigger-daily-db-backup.sh &>> "/data/logs/nc-db-backup_$(date -d${1:-today} +%Y%m%d_%s).log
        """
    )
