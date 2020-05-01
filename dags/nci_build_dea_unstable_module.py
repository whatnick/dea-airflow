"""
# Rebuild `dea/unstable` module on the NCI

"""
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import datetime, timedelta

from airflow.operators.email_operator import EmailOperator
from airflow.operators.slack_operator import SlackAPIOperator, SlackAPIPostOperator

import pendulum

local_tz = pendulum.timezone("Australia/Canberra")

default_args = {
    'owner': 'dayers',
    'start_date': datetime(2020, 3, 12, tzinfo=local_tz),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'timeout': 1200,  # For running SSH Commands
}

dag = DAG(
    'nci_build_dea_unstable_module',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
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

    post_to_slack = SlackAPIPostOperator(
        task_id='post_to_slack',
        slack_conn_id='',
        channel='#dea-beginners',
        username='airflow-bot',
        text='Successfully built new dea/unstable module on the NCI',
        icon_url='',

    )

    send_email = EmailOperator(
        task_id='send_email',
        to='damien@omad.net',
        subject='New dea/unstable Module',
        html_content='Successfully built new dea/unstable module on the NCI',
    )

    build_env_task >> [post_to_slack, send_email]
