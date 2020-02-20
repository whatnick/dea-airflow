import re
from airflow import DAG, AirflowException
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Damien Ayers',
    'depends_on_past': False,  # Very important, will cause a single failure to propagate forever
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
fc_products = ['ls8_fc_albers']

dag = DAG(
    'execute_fractional_cover',
    default_args=default_args,
    catchup=False,
    schedule_interval="@daily",
    template_searchpath='templates/'
)


def extract_workdir_from_log(upstream_task_id, ti, **kwargs):
    example_log = """
    2020-02-20 14:12:51,233 1315 digitalearthau.runners.util INFO Created work directory /g/data/v10/work/ls8_fc_albers/fc/2020-02/20-031251
    2020-02-20 14:12:51,237 1315 /g/data/v10/public/modules/dea/unstable/lib/python3.6/site-packages/fc/fc_app.py INFO Created task description: /g/data/v10/work/ls8_fc_albers/fc/2020-02/20-031251/task-description.json

    """
    log_output = ti.xcom_pull(key=None, task_ids='')
    match = re.search(r'.*Created work dir (/.*)$', log_output, re.MULTILINE)
    if match:
        return match.group(1)
    else:
        raise AirflowException('Unable to extract workdir from log for task: %s', upstream_task_id)


with dag:
    start = DummyOperator(task_id='start')

    # Now we're into task_app territory, and have less control from here over output dirs
    create_workdir_cmd = """
        module use /g/data/v10/public/modules/modulefiles;
        module load {{ params.module }};
        APP_CONFIG="$(datacube-fc list | grep "{{ params.product }}")";
        
        # Initialises the 'task_app' configuration and directory structure
        # But doesn't try to use the internal qsub machinery
        datacube-fc submit -v -v --project {{ params.project }} --queue {{ params.queue }} --year {{ params.year }} \
          --app-config "${APP_CONFIG}" --config "${DATACUBE_CONFIG_PATH}" --no-qsub --tag ls_fc
          
    """

    submit_fc_generate_cmd = """
      {% set work_dir = task_instance.xcom_pull(task_ids=discover_workdir_ + params.product) -%}
      qsub -N fc_{{ params.product}}_{{ params.year }} \
      -q {{ params.queue }} \
      -W umask=33 \
      -l wd,walltime=1:00:00,mem=4GB,ncpus=1 -m abe \
      -l storage=gdata/v10+gdata/fk4+gdata/rs0+gdata/if87 \
      -M nci.monitor@dea.ga.gov.au \
      -P {{ params.project }} -o {{ work_dir }} -e {{ work_dir }} \
      -- /bin/bash -l -c \
          "source $HOME/.bashrc; \
          module use /g/data/v10/public/modules/modulefiles/; \
          module load {{ params.module }}; \
        datacube-fc generate -v -v --project {{ params.project }} --queue {{ params.queue }} --year {{ params.year }} \
          --app-config ${APP_CONFIG} --config ${DATACUBE_CONFIG_PATH} --no-qsub --tag ls_fc"
    """
    completed = DummyOperator(task_id='submitted_to_pbs')

    for product in fc_products:
        initialise_workdir = SSHOperator(
            task_id=f'initialise_fc_{product}',
            ssh_conn_id='lpgs_gadi',
            command=create_workdir_cmd,
            params={'product': product},
            do_xcom_push=True
        )

        discover_workdir = PythonOperator(
            task_id=f'extract_workdir_{product}',
            python_callable=extract_workdir_from_log,
            op_kwargs={'upstream_task_id': f'initialise_fc_{product}'}
        )

        submit_generate_job = SSHOperator(
            task_id=f'submit_generate_job_{product}',
            ssh_conn_id='lpgs_gadi',
            command=submit_fc_generate_cmd,
            params={'product': product},
            do_xcom_push=True

        )

        wait_for_generate_job = DummyOperator(
            task_id=f'wait_for_generate_{product}'
        )

        start >> initialise_workdir >> discover_workdir >> submit_generate_job
        submit_generate_job >> completed
