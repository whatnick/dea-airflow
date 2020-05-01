"""
# NCI to RDS Datacube DB migration

DAG to periodically sync NCI datacube to RDS mainly for the purpose of
running [Explorer](https://github.com/opendatacube/datacube-explorer)
and [Resto](https://github.com/jjrom/resto).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret
from airflow.operators.dummy_operator import DummyOperator

DEFAULT_ARGS = {
    "owner": "Tisham Dhar",
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 1),
    'email': ["tisham.dhar@ga.gov.au"],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('ssh_k8s_db_sync',
          doc_md=__doc__,
          default_args=DEFAULT_ARGS,
          catchup=False,
          concurrency=1,
          schedule_interval=None)

with dag:
  START = DummyOperator(task_id="nci_rds_sync")
  RESTORE_RDS_S3 = DummyOperator(task_id="restore_rds_s3")
  SUMMARIZE_DATACUBE = DummyOperator(task_id="summarize_datacube")
  AUDIT_EXPLORER = DummyOperator(task_id="audit_explorer")
  ETL_RESTO = DummyOperator(task_id="etl_resto")
  AUDIT_RESTO = DummyOperator(task_id="audit_resto")
  COMPLETE = DummyOperator(task_id="all_done")

  START >> RESTORE_RDS_S3
  RESTORE_RDS_S3 >> SUMMARIZE_DATACUBE
  SUMMARIZE_DATACUBE >> AUDIT_EXPLORER
  RESTORE_RDS_S3 >> ETL_RESTO
  ETL_RESTO >> AUDIT_RESTO
  AUDIT_EXPLORER >> COMPLETE
  AUDIT_RESTO >> COMPLETE