"""
# Batch Convert NetCDFs to COGs and Upload to AWS S3

This DAG runs tasks on Gadi at the NCI. It:

 * downloads an S3 inventory listing for each ODC Product,
 * compares what is in the Database with what is on S3,
 * runs a batch convert of NetCDF to COG (Inside a scheduled PBS job)
 * Uploads the COGs to S3

"""
import os
from textwrap import dedent

from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import datetime, timedelta

from sensors.pbs_job_complete_sensor import PBSJobSensor

DEST = os.environ.get(
    "COG_OUTPUT_DESTINATION",
    "s3://dea-public-data/"
)

NCI_MODULE = os.environ.get(
    "NCI_MODULE",
    'dea/unstable'
)

COG_S3PREFIX_PATH = {
    'wofs_albers': f'{DEST}/WOfS/WOFLs/v2.1.5/combined',
    'ls8_fc_albers': f'{DEST}/dea-public-data/fractional-cover/fc/v2.2.1/ls8',
    'ls7_fc_albers': f'{DEST}/dea-public-data/fractional-cover/fc/v2.2.1/ls7'
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
        'module': NCI_MODULE,
        'year': '2020'
    }
}

dag = DAG(
    'nci_cog_and_upload',
    doc_md=__doc__,
    default_args=default_args,
    catchup=False,
    schedule_interval=None,
    template_searchpath='templates/',
    tags=['nci'],
)

with dag:
    for product, upload_path in COG_S3PREFIX_PATH.items():
        COMMON = """
                {% set work_dir = '/g/data/v10/work/cog/' + params.product + '/' + ts_nodash %}
                module load {{params.module}}
              
        """
        download_s3_inventory = SSHOperator(
            task_id=f'download_s3_inventory_{product}',
            command=dedent(COMMON + '''
                mkdir -p {{work_dir}}

                dea-cogger save-s3-inventory --product-name "{{ params.product }}" --output-dir "{{work_dir}}"
            '''),
            params={'product': product},
        )
        generate_work_list = SSHOperator(
            task_id=f'generate_work_list_{product}',
            command=dedent(COMMON + """
                cd {{work_dir}}

                dea-cogger generate-work-list --product-name "{{params.product}}" \
                 --output-dir "{{work_dir}}" --s3-list  "{{params.product}}_s3_inv_list.txt" \
                 --time-range "time in [2018-01-01, 2020-12-31]"
            """),
            # --time-range "time in [{{prev_ds}}, {{ds}}]"
            timeout=60 * 60 * 2,
            params={'product': product},
        )
        submit_task_id = f'submit_cog_convert_job_{product}'
        bulk_cog_convert = SSHOperator(
            task_id=submit_task_id,
            command=dedent(COMMON + """
                cd {{work_dir}}
                
                qsub <<EOF
                #!/bin/bash
                #PBS -l wd,walltime=5:00:00,mem=190GB,ncpus=48,jobfs=1GB
                #PBS -P {{params.project}}
                #PBS -q {{params.queue}}
                #PBS -l storage=gdata/v10+gdata/fk4+gdata/rs0+gdata/if87
                #PBS -W umask=33
                #PBS -N cog_{{params.product}} 
                
        
                module load {{params.module}}
                module load openmpi/3.1.4

                mpirun --tag-output dea-cogger mpi-convert --product-name "{{params.product}}" 
                 --output-dir "{{work_dir}}" {{params.product}}_file_list.txt
                
                EOF
            """),
            do_xcom_push=True,
            timeout=60 * 5,
            params={'product': product},
        )

        wait_for_cog_convert = PBSJobSensor(
            task_id=f'wait_for_cog_convert_{product}',
            pbs_job_id="{{ ti.xcom_pull(task_ids='%s') }}" % submit_task_id,
            timeout=60 * 60 * 24 * 7,
        )

        validate_task_id = f'submit_validate_cog_job_{product}'
        validate_cogs = SSHOperator(
            task_id=validate_task_id,
            command=dedent(COMMON + """
                cd {{work_dir}}
                
                qsub <<EOF
                #!/bin/bash
                #PBS -l wd,walltime=5:00:00,mem=190GB,ncpus=48,jobfs=1GB
                #PBS -P {{params.project}}
                #PBS -q {{params.queue}}
                #PBS -l storage=gdata/v10+gdata/fk4+gdata/rs0+gdata/if87
                #PBS -W umask=33
                #PBS -N cog_{{params.product}} 
                
        
                module load {{params.module}}
                module load openmpi/3.1.4

                mpirun --tag-output dea-cogger verify --rm-broken "{{work_dir}}"
                
                EOF
            """),
            params={'product': product},
        )
        wait_for_validate_job = PBSJobSensor(
            task_id=f'wait_for_cog_validate_{product}',
            pbs_job_id="{{ ti.xcom_pull(task_ids='%s') }}" % validate_task_id,
            timeout=60 * 60 * 24 * 7,

        )

        upload_to_s3 = SSHOperator(
            task_id=f'upload_to_s3_{product}',
            command=dedent(COMMON + """
            aws s3 sync "{{work_dir}}" "{{params.upload_path}}" \
            --exclude "*.pickle" --exclude "*.txt" --exclude "*file_list*" --exclude "*.log"
            """),
            params={'product': product,
                    'upload_path': upload_path},
        )

        delete_nci_cogs = SSHOperator(
            task_id=f'delete_nci_cogs_{product}',
            # Remove cog converted files after aws s3 sync
            command=dedent(COMMON + """
            rm -rf "{{work_dir}}"
            """),
            params={'product': product},
        )

        download_s3_inventory >> generate_work_list >> bulk_cog_convert >> wait_for_cog_convert
        wait_for_cog_convert >> validate_cogs >> upload_to_s3 >> delete_nci_cogs
