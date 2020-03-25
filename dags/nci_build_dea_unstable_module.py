from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Damien Ayers',
    'start_date': datetime(2020, 3, 12),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'timeout': 1200,  # For running SSH Commands
}

dag = DAG(
    'nci_build_dea_unstable_module',
    default_args=default_args,
    schedule_interval=None,
    tags=['nci'],
)

with dag:
    build_env_task = SSHOperator(
        task_id=f'build_dea_unstable_module',
        ssh_conn_id='lpgs_gadi',
        command="""
        cd ~/dea-orchestration/raijin_scripts/deploy
        git reset --hard
        git pull
        git status
        module load python3/3.7.4
        pip3 install --user pyyaml jinja2
        
        rm -rf /g/data/v10/public/modules/dea/unstable/
        ./build_environment_module.py dea_unstable/modulespec.yaml
        """,
    )
