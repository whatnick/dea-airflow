import re
from airflow import DAG, AirflowException
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from sensors.pbs_job_complete_sensor import PBSJobSensor

default_args = {
    'owner': 'Damien Ayers',
    'depends_on_past': False,  # Very important, will cause a single failure to propagate forever
    'start_date': datetime(2020, 2, 17),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'timeout': 1800,  # For running SSH Commands
    'params': {
        'project': 'v10',
        'queue': 'normal',
        'module': 'dea/unstable',
        'year': '2019'
    }
}

fc_products = [
    'ls7_fc_albers',
    'ls8_fc_albers',
]

dag = DAG(
    'execute_fractional_cover',
    default_args=default_args,
    catchup=False,
    schedule_interval=None,
    template_searchpath='templates/'
)


def extract_workdir_from_log(upstream_task_id, ti, **kwargs):
    example_log = """
    2020-02-20 14:12:51,233 1315 digitalearthau.runners.util INFO Created work directory /g/data/v10/work/ls8_fc_albers/fc/2020-02/20-031251
    2020-02-20 14:12:51,237 1315 /g/data/v10/public/modules/dea/unstable/lib/python3.6/site-packages/fc/fc_app.py INFO Created task description: /g/data/v10/work/ls8_fc_albers/fc/2020-02/20-031251/task-description.json

    """
    log_output = ti.xcom_pull(key=None, task_ids=upstream_task_id)
    match = re.search(r'.*Created work dir (/.*)$', log_output, re.MULTILINE)
    if match:
        return match.group(1)
    else:
        raise AirflowException('Unable to extract workdir from log for task: %s', upstream_task_id)


with dag:
    start = DummyOperator(task_id='start')
    completed = DummyOperator(task_id='completed')

    COMMON = """
        module use /g/data/v10/public/modules/modulefiles;
        module load {{ params.module }};
        APP_CONFIG="$(datacube-fc list | grep "{{ params.product }}")";
        
    """
    # Now we're into task_app territory, and have less control from here over output dirs
    create_workdir_cmd = COMMON + """
        # Initialises the 'task_app' configuration and directory structure
        # But doesn't try to use the internal qsub machinery
        datacube-fc submit -v -v --project {{ params.project }} --queue {{ params.queue }} --year {{ params.year }} \
          --app-config "${APP_CONFIG}" --config "${DATACUBE_CONFIG_PATH}" --no-qsub --tag ls_fc
          
    """

    COMMON += """
      {% set work_dir = task_instance.xcom_pull(task_ids=discover_workdir_ + params.product) -%}
      
        cd {{work_dir}};
    """
    fc_generate_cmd = COMMON + """
      
        datacube-fc generate -v -v --project {{ params.project }} --queue {{ params.queue }} --year {{ params.year }} \
          --app-config ${APP_CONFIG} --no-qsub --tag ls_fc"
    """
    fc_test_cmd = COMMON + """
      
        datacube-fc generate -v -v --project {{ params.project }} --queue {{ params.queue }} --year {{ params.year }} \
          --app-config ${APP_CONFIG} --no-qsub --tag ls_fc"
    """
    submit_fc_cmd = """
      {% set work_dir = task_instance.xcom_pull(task_ids=discover_workdir_ + params.product) -%}
      
        cd {{work_dir}};
      
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
          datacube-fc run -vv --project {{ params.project }} --queue {{ params.queue }} --year {{ params.year }} \
          --app-config ${APP_CONFIG} --tag ls_fc"
    """

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
        generate_tasks = SSHOperator(
            ssh_conn_id='lpgs_gadi',
            command=fc_generate_cmd,
            params={'product': product},
            task_id=f'generate_fc_tasks_{product}',
        )
        test_tasks = SSHOperator(
            ssh_conn_id='lpgs_gadi',
            command=fc_test_cmd,
            params={'product': product},
            task_id=f'test_fc_tasks_{product}',
        )

        submit_fc_job = SSHOperator(
            task_id=f'submit_fc_{product}',
            ssh_conn_id='lpgs_gadi',
            command=submit_fc_cmd,
            params={'product': product},
            do_xcom_push=True
        )

        wait_for_completion = PBSJobSensor(
            task_id=f'wait_for_{product}_ingest',
            ssh_conn_id='lpgs_gadi',
            pbs_job_id="{{ ti.xcom_pull(task_ids='submit_fc_%s') }}" % product,
        )

        start >> initialise_workdir >> discover_workdir >> generate_tasks
        generate_tasks >> test_tasks >> submit_fc_job >> wait_for_completion
        wait_for_completion >> completed
