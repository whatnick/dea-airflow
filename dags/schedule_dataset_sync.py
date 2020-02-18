from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'Damien Ayers',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 1),
    'email': ['damien.ayers@ga.gov.au'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    'params': {'project': 'v10',
               'queue': 'normal',
               'module': 'dea/unstable',
               'year': '2019'
               }
}

synced_products = 'ls8_nbar_scene,ls8_nbart_scene,ls8_pq_scene,ls8_pq_legacy_scene'.split(
    ',')
unsynced = 'ls7_nbar_scene,ls7_nbart_scene,ls7_pq_scene,ls7_pq_legacy_scene'

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

ingest_products = 'ls8_nbar_albers,ls8_nbart_albers,ls8_pq_albers'
ls7_ingest = 'ls7_nbar_albers,ls7_nbart_albers,ls7_pq_albers'

dag = DAG('schedule_sync_jobs',
          default_args=default_args,
          catchup=False,
          schedule_interval=timedelta(days=1))

SYNC_CACHE = "${WORKDIR}" / cache_$(date '+%N')
mkdir - p
sync_command = """
  {% set work_dir = '/g/data/v10/work/sync/' + params.product + '/' + ds %}
  {% set sync_cache_dir = work_dir + '/cache' %}
  {% set sync_path = '/g/data/rs0/scenes/LSKJ/ls8/' + year + '/' %}
  mkdir -p 
  qsub -N sync_{{ params.product}}_{{ params.year }} -q {{ params.queue }} -W umask=33 -l wd,walltime=20:00:00,mem=3GB -m abe \
  -l storage=gdata/v10+gdata/fk4+gdata/rs0+gdata/if87 -M nci.monitor@dea.ga.gov.au -P {{ params.project }} -o {{work_dir}} -e {{work_dir}} \
  -- /bin/bash -l -c "source $HOME/.bashrc; module use /g/data/v10/public/modules/modulefiles/; \
  module load {{ params.module }}; dea-sync -vvv --cache-folder {{sync_cache_dir}} -j 1 \
  --update-locations --index-missing ${syncpath}"
"""

def make_sync_task(product, ):

    submit_sync = SSHOperator(
        task_id='submit_sync',
        ssh_conn_id='lpgs_gadi',
        command=SYNC_CMD,
        dag=dag,
        params={'product': product,
                'year': '2019',
                'base_path': '/g/data/rs0/scenes/nbar-scenes-tmp/ls8/2019/12/output/nbar',
                'base_path': '/g/data/rs0/scenes/nbar-scenes-tmp/ls8/2019/12/output/nbart',
                'types': ['nbar-scenes-tmp/  pq-legacy-scenes-tmp/  pq-scenes-tmp/  pq-wofs_scenes-tmp/']
                }
    )
    return submit_sync

for product in synced_products:
    # TODO setup workdir
    """
BASE_PATH=/g/data/rs0/
WORKDIR=/g/data/v10/work/sync/"${PRODUCT}"/$(date '+%FT%H%M')
START_YEAR=$(echo "$YEAR" | cut -f 1 -d '-')
END_YEAR=$(echo "$YEAR" | cut -f 2 -d '-')
JOB_NAME=sync_"${PRODUCT}_${START_YEAR}-${END_YEAR}"
SUBMISSION_LOG="${WORKDIR}"/sync-${PRODUCT}-$(date '+%F-%T').log
PATHS_TO_PROCESS+=("$BASE_PATH$year$SUFFIX_PATH")
    """

    submit_sync = make_sync_task(product)

    get_qstat_output = SSHOperator(
        task_id='get_qstat_output',
        command='qstat -xf -F json',
        do_xcom_push=True,
        dag=dag
    )

    # TODO Implement an SSH Sensor to wait for the submitted job to be done
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
        dag=dag)

    submit_sync >> get_qstat_output >> t1
