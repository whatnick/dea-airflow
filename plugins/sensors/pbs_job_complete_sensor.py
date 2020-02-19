from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

class PBSJobSensor(BaseSensorOperator):

    template_fields = ('pbs_job_id',)

    @apply_defaults
    def __init__(self,
                 ssh_conn_id=None,
            ssh_hook=None,
            pbs_job_id=None,
            xcom_task_id_key=None,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hook = ssh_hook

        if pbs_job_id is not None:
            self.job_id = pbs_job_id
        else:
            task_instance = context['task_instance']
            self.job_id = task_instance.xcom_pull(xcom_task_id_key)

    def poke(self, context):
        try:
            if self.ssh_conn_id:
                if self.ssh_hook and isinstance(self.ssh_hook, SSHHook):
                    self.log.info("ssh_conn_id is ignored when ssh_hook is provided.")
                else:
                    self.log.info("ssh_hook is not provided or invalid. " +
                                  "Trying ssh_conn_id to create SSHHook.")
                    self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)

            if not self.ssh_hook:
                raise AirflowException("Cannot operate without ssh_hook or ssh_conn_id.")

            with self.ssh_hook.get_conn() as ssh_client:
        pass


class PBSJobCompleteSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

    def poke(self, context):
        return True
