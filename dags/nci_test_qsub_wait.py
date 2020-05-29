"""
# Test DAG for submitting PBS Jobs and awaiting their completion
"""
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator

from sensors.pbs_job_complete_sensor import PBSJobSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'Damien Ayers',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2020, 3, 4),  # probably ignored since schedule_interval is None
    'timeout': 90,  # For running SSH Commands
}

with DAG('nci_test_qsub_wait',
         default_args=default_args,
         catchup=False,
         schedule_interval=None,
         template_searchpath='templates/',
         doc_md=__doc__,
         ) as dag:
    submit_pbs_job = SSHOperator(
        task_id=f'submit_foo_pbs_job',
        ssh_conn_id='lpgs_gadi',
        command="""
          {% set work_dir = '~/airflow_testing/' -%}
          mkdir -p {{ work_dir }};
          cd {{ work_dir }};
          qsub \
          -q express \
          -W umask=33 \
          -l wd,walltime=0:10:00,mem=3GB -m abe \
          -l storage=gdata/v10+gdata/fk4+gdata/rs0+gdata/if87 \
          -P {{ params.project }} -o {{ work_dir }} -e {{ work_dir }} \
          -- /bin/bash -l -c \
              "source $HOME/.bashrc; \
              module use /g/data/v10/public/modules/modulefiles/; \
              module load {{ params.module }}; \
              dea-coherence --check-locationless time in [2019-12-01, 2019-12-31] > coherence-out.txt"
        """,
        params={
            'project': 'v10',
            'queue': 'normal',
            'module': 'dea/unstable',
            'year': '2019'
        },
        do_xcom_push=True,
    )
    wait_for_completion = PBSJobSensor(
        task_id='wait_for_completion',
        ssh_conn_id='lpgs_gadi',
        pbs_job_id="{{ ti.xcom_pull(task_ids='submit_pbs_job') }}"
    )

    submit_pbs_job >> wait_for_completion
