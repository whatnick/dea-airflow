"""DAG to periodically update explorer and ows schemas in RDS
after daily NRT has been indexed.
- Run Explorer summaries
- Run ows update ranges for NRT products
- Run ows update ranges for NRT multi-products

This DAG uses k8s executors and pre-existing pods in cluster with relevant tooling
and configuration installed

set start_date to something real, not datetime.utcnow() because that'll break things

for setting upstream and downstream, use
start >> passing
start >> failing

You can use `with dag: ` as a context manager, and not have to manually pass it as an argument
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret
from airflow.operators.dummy_operator import DummyOperator


DEFAULT_ARGS = {
    "owner": "Tisham Dhar",
    "depends_on_past": False,
    "start_date": datetime(2020, 3, 4),
    "email": ["tisham.dhar@ga.gov.au"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "secrets": [Secret('env', 'DB_HOSTNAME', "ows-db")]
}

dag = DAG("k8s_s2_nrt_orchestrate",
          default_args=DEFAULT_ARGS,
          schedule_interval=timedelta(hours=12),
          catchup=False
          )


with dag:
    START = DummyOperator(task_id="s3_index_publish")
    INDEXING = KubernetesPodOperator(
        namespace="processing",
        image="opendatacube/datacube-index:0.1.62",
        cmds=["s3-to-dc"],
        env_vars={"AWS_DEFAULT_REGION": "ap-southeast-2"},
        # TODO: Collect form JSON used to trigger DAG
        arguments=[
            "s3://dea-public-data/cemp_insar/insar/displacement/alos//**/*.yaml",
            "cemp_insar_alos_displacement",
            # "{{ dag_run.conf.s3_glob }}", # TODO: Jinja templates for arguments
            # "{{ dag_run.conf.product }}"
        ],
        # TODO: Use secrets to send DB Creds
        # TODO: Lift secrets into environment variables for datacube
        labels={"step": "s3-to-rds"},
        name="datacube-index",
        task_id="indexing-task",
        get_logs=True,
    )

    UPDATE_RANGES = KubernetesPodOperator(
        namespace="processing",
        image="opendatacube/ows:0.13.1",
        cmds=["python", "-c"],
        arguments=["print('hello world')"],
        labels={"step": "ows"},
        name="ows-update-ranges",
        task_id="update-ranges-task",
        get_logs=True,
    )

    SUMMARY = KubernetesPodOperator(
        namespace="processing",
        image="opendatacube/dashboard:2.1.6",
        cmds=["python", "-c"],
        arguments=["print('hello world')"],
        labels={"step": "explorer"},
        name="explorer-summary",
        task_id="explorer-summary-task",
        get_logs=True,
    )

    COMPLETE = DummyOperator(task_id="all_done")

    START >> INDEXING
    INDEXING >> UPDATE_RANGES
    INDEXING >> SUMMARY
    UPDATE_RANGES >> COMPLETE
    SUMMARY >> COMPLETE
