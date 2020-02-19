from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

ingest_products = {
    'ls8_nbar_scene': 'ls8_nbar_albers',
    'ls8_nbart_scene': 'ls8_nbart_albers',
    'ls8_pq_scene': 'ls8_pq_albers'
}
default_args = {
    'owner': 'Damien Ayers',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 1),
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
ingest_dag = DAG(
    'schedule_dataset_ingest',
    default_args=default_args,
    catchup=False,
    schedule_interval="@daily",
    template_searchpath='templates/'
)

with ingest_dag:
    start = DummyOperator(task_id='start')

    # Now we're into task_app territory, and have less control from here over output dirs
    INGEST_COMMAND = """
          module use /g/data/v10/public/modules/modulefiles;
          module load {{ params.module }};
          yes Y | dea-submit-ingest qsub --project {{ params.project }} --queue {{ params.queue }} -n 1 -t 15 --allow-product-changes \
    --name ing_{{params.ing_product}}_{{params.year}} {{params.ing_product}} {{params.year}}

    """
    completed = DummyOperator(task_id='submitted_to_pbs')
    for ing_product in ingest_products.values():
        ingest_task = SSHOperator(
            task_id=f'submit_ingest_{ing_product}',
            ssh_conn_id='lpgs_gadi',
            command=INGEST_COMMAND,
            params={'ing_product': ing_product},
            do_xcom_push=True,
        )

        start >> ingest_task >> completed
