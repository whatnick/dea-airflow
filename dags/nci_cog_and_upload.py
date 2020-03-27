from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator

from sensors.pbs_job_complete_sensor import PBSJobSensor

COG_S3PREFIX_PATH = {
    'wofs_albers': 's3://dea-public-data/WOfS/WOFLs/v2.1.5/combined',
    'ls8_fc_albers': 's3://dea-public-data/fractional-cover/fc/v2.2.1/ls8',
    'ls7_fc_albers': 's3://dea-public-data/fractional-cover/fc/v2.2.1/ls7'
}

default_args = {
    'owner': 'Damien Ayers',
    'start_date': datetime(2020, 3, 12),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'ssh_conn_id': 'lpgs_gadi',
    'params': {
        'project': 'v10',
        'queue': 'normal',
        'module': 'dea/unstable',
        'year': '2020'
    }
}

dag = DAG(
    'nci_cog_and_upload',
    default_args=default_args,
    catchup=False,
    schedule_interval=None,
    template_searchpath='templates/',
    tags=['nci'],
)

with dag:
    start = DummyOperator(task_id='start')

    COMMON = """
          {% set work_dir = '/g/data/v10/work/wofs_albers/' + ts_nodash %}
          module use /g/data/v10/public/modules/modulefiles;
          module load {{ params.module }};
          
          APP_CONFIG=/g/data/v10/public/modules/{{params.module}}/wofs/config/wofs_albers.yaml
    """
    download_s3_inventory = SSHOperator(
        task_id='download_s3_inventory',
        command='''
            source "$HOME"/.bashrc
            module use /g/data/v10/public/modules/modulefiles
            module load dea

            dea_cogger save-s3-inventory -p "${PRODUCT}" -o "${OUT_DIR}" -c aws_products_config.yaml
        '''
    )
    generate_work_list = SSHOperator(
        task_id='generate_wofs_tasks',
        command=COMMON + """
            module use /g/data/v10/public/modules/modulefiles/
            module load dea
            module load openmpi/3.1.2

            dea_cogger generate-work-list --config "${ROOT_DIR}"/aws_products_config.yaml \
            --product-name "$PRODUCT_NAME" --time-range "$TIME_RANGE" --output-dir "$OUTPUT_DIR" --pickle-file "$PICKLE_FILE"
        
        """,
        timeout=60 * 60 * 2,
    )
    bulk_cog_convert = SSHOperator(
        task_id='submit_cog_convert_job',
        command="""
            module use /g/data/v10/public/modules/modulefiles/
            module load dea
            module load openmpi/3.1.2

            mpirun --tag-output python3 "${ROOT_DIR}"/cog_conv_app.py mpi-convert -p "${PRODUCT}" -o "${OUTPUT_DIR}" "${FILE_LIST}"
        """,
        do_xcom_push=True,
        timeout=60 * 20,
    )

    validate_cogs = SSHOperator(
        task_id='validate_cogs',
        command="""
        """
    )

    upload_to_s3 = SSHOperator(
        task_id='upload_to_s3',
        command="""
        aws s3 sync "$OUT_DIR" "$S3_BUCKET" --exclude "*.pickle" --exclude "*.txt" --exclude "*file_list*" \
        --exclude "*.log"

        # Remove cog converted files after aws s3 sync
        rm -r "$OUT_DIR"
        """
    )

    submit_task_id = 'submit_wofs_albers'
    submit_wofs_job = SSHOperator(
        task_id=submit_task_id,
        command=COMMON + """
          # TODO Should probably use an intermediate task here to calculate job size
          # based on number of tasks.
          # Although, if we run regularaly, it should be pretty consistent.
          # Last time I checked, FC took about 30s per tile (task).

          cd {{work_dir}}

          qsub -N wofs_albers \
          -q {{ params.queue }} \
          -W umask=33 \
          -l wd,walltime=5:00:00,mem=190GB,ncpus=48 -m abe \
          -l storage=gdata/v10+gdata/fk4+gdata/rs0+gdata/if87 \
          -M nci.monitor@dea.ga.gov.au \
          -P {{ params.project }} -o {{ work_dir }} -e {{ work_dir }} \
          -- /bin/bash -l -c \
              "module use /g/data/v10/public/modules/modulefiles/; \
              module load {{ params.module }}; \
              datacube-wofs run -v --input-filename {{work_dir}}/tasks.pickle --celery pbs-launch"
        """,
    )
    wait_for_completion = PBSJobSensor(
        task_id=f'wait_for_wofs_albers',
        pbs_job_id="{{ ti.xcom_pull(task_ids='%s') }}" % submit_task_id,
        timeout=60 * 60 * 24 * 7,
    )
    completed = DummyOperator(task_id='submitted_to_pbs')


