from airflow import DAG
from datetime import datetime
from operators.nci_operators import TemplateToSFTPOperator

default_args = {
    'owner': 'Damien Ayers',

    'depends_on_past': True,
    'start_date': datetime(2020, 2, 1),
    'email': ['damien.ayers@ga.gov.au'],
}

dag = DAG(
    'testdag',
    default_args=default_args,
    catchup=False,
    schedule_interval="@daily",
    template_searchpath='/home/omad/airflow/dags/templates/'
)

with dag:
    upload_template_var = TemplateToSFTPOperator(
        task_id='upload_template_var',
        ssh_conn_id='omad_localhost',
        remote_filepath='/home/omad/{{ ds }}test.txt',
        file_contents="testtemplate.jinja2",
        create_intermediate_dirs=False,
        file_mode=0o644,
    )
