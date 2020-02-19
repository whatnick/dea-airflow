from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Damien Ayers',
    'depends_on_past': True,
    'start_date': datetime(2020, 2, 17),
    'email': ['damien.ayers@ga.gov.au'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'timeout': 90,  # For running SSH Commands
    'params': {
        'project': 'v10',
        'queue': 'normal',
        'module': 'dea/unstable',
        'year': '2019'
    }
}

dag = DAG(
    'execute_wofs',
    default_args=default_args,
    catchup=False,
    schedule_interval="@daily",
    template_searchpath='templates/'
)

with dag:
    start = DummyOperator(task_id='start')

    # Now we're into task_app territory, and have less control from here over output dirs
    WOFS_COMMAND = """
          module use /g/data/v10/public/modules/modulefiles;
          module load {{ params.module }};
          APP_CONFIG=/g/data/v10/public/modules/{{params.module}}/wofs/config/wofs_albers.yaml
          datacube-wofs submit -v -v --project {{ params.project }} --queue {{ params.queue }} --year {{ params.year }} \
          --app-config ${APP_CONFIG} --tag ls_wofs
        
    """
    completed = DummyOperator(task_id='submitted_to_pbs')
    wofs_task = SSHOperator(
        task_id=f'submit_wofs',
        ssh_conn_id='lpgs_gadi',
        command=WOFS_COMMAND,
        params={'product': 'wofs_albers'},
        do_xcom_push=True,
    )

    start >> wofs_task >> completed
