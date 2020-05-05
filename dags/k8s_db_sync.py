"""
# NCI to RDS Datacube DB migration

DAG to periodically sync NCI datacube to RDS mainly for the purpose of
running [Explorer](https://github.com/opendatacube/datacube-explorer)
and [Resto](https://github.com/jjrom/resto).

[Waits for S3Key](https://gist.github.com/nehiljain/6dace5faccb680653f7ea4d5d5273946)
for a day's backup to be available via 
[S3KeySensor](https://airflow.apache.org/docs/stable/_api/airflow/sensors/s3_key_sensor/index.html)
and excutes downstream task including verifying backup
integrity using md5sum
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret
from airflow.operators.dummy_operator import DummyOperator

DEFAULT_ARGS = {
    "owner": "Tisham Dhar",
    "depends_on_past": False,
    "start_date": datetime(2020, 2, 1),
    "email": ["tisham.dhar@ga.gov.au"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    "k8s_db_sync",
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    catchup=False,
    concurrency=1,
    tags=["k8s"],
    schedule_interval=timedelta(minutes=5),
)


def backup_announce(**kwargs):
    print("A new db dump is here!!")

file_prefix="105-{{ ds_nodash }}"

with dag:
    START = DummyOperator(task_id="nci_rds_sync")
    # Wait for S3 Key
    S3_BACKUP_SENSE = S3KeySensor(
        task_id='s3_backup_sense',
        poke_interval=60*30,
        bucket_key=f"s3://nci-db-dump/prod/{file_prefix}-datacube.pgdump",
        aws_conn_id="aws_nci_db_backup"
    )
    ANNOUNCE_BACKUP_ARRIVAL = PythonOperator(
        task_id="announce_backup_arrival",
        provide_context=True,
        python_callable=backup_announce,
    )
    # Download PostgreSQL backup from S3 to within K8S storage
    RESTORE_RDS_S3 = DummyOperator(task_id="restore_rds_s3")
    # Restore to a local db and link it to explorer codebase and run summary
    SUMMARIZE_DATACUBE = DummyOperator(task_id="summarize_datacube")
    # Get API responses from Explorer and ensure product count summaries match
    AUDIT_EXPLORER = DummyOperator(task_id="audit_explorer")
    # Transfer Data via Explorer STAC API to Resto PostgreSQL DB
    ETL_RESTO = DummyOperator(task_id="etl_resto")
    # Ensure RESTO gives expected results
    AUDIT_RESTO = DummyOperator(task_id="audit_resto")
    COMPLETE = DummyOperator(task_id="all_done")

    START >> S3_BACKUP_SENSE
    S3_BACKUP_SENSE >> RESTORE_RDS_S3
    S3_BACKUP_SENSE >> ANNOUNCE_BACKUP_ARRIVAL
    RESTORE_RDS_S3 >> SUMMARIZE_DATACUBE
    SUMMARIZE_DATACUBE >> AUDIT_EXPLORER
    RESTORE_RDS_S3 >> ETL_RESTO
    ETL_RESTO >> AUDIT_RESTO
    AUDIT_EXPLORER >> COMPLETE
    AUDIT_RESTO >> COMPLETE
