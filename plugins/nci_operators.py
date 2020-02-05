from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


# This isn't necessary, can call qsub directly, and set do_xcom_push=True the job id
class SubmitPBSJob(SSHOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(SubmitPBSJob).__init__(*args, **kwargs)

    def execute(self, context):
        pass


class PBSJobSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, ssh_hook, pbs_job_id, xcom_task_id_key, *args, **kwargs):
        super(PBSJobSensor).__init__(*args, **kwargs)
        self.hook = ssh_hook

        if pbs_job_id is not None:
            self.job_id = pbs_job_id
        else:
            task_instance = context['task_instance']
            self.job_id = task_instance.xcom_pull(xcom_task_id_key)

    def poke(self, context):
        pass


class NCIPlugin(AirflowPlugin):
    name = "nci_plugin"
    operators = [PBSJobSensor]
