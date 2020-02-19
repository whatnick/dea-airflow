import os.path
from io import StringIO

from airflow import AirflowException
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.sftp_operator import _make_intermediate_dirs
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


class TemplateToSFTPOperator(BaseOperator):
    """
    TemplateToSFTPOperator is for uploading a file to a remote server
    based on a template. It takes many of the same params as SFTPOperator.

    :param str ssh_conn_id: connection id from airflow Connections.
        `ssh_conn_id` will be ignored if `ssh_hook` is provided.
    :param bool create_intermediate_dirs: create missing intermediate directories when
        copying from remote to local and vice-versa. Default is False.
    :param int file_mode: permissions to set on the remote file. eg 0o644 or 0o755
    :param file_contents: contents to upload into the file (templated)
    :param remote_filepath: remote file path to get or put. (templated)
    """
    template_fields = ('file_contents', 'remote_filepath')
    template_ext = ('jinja2',)

    @apply_defaults
    def __init__(self,
                 ssh_conn_id=None,
                 ssh_hook=None,
                 file_mode=None,
                 file_contents='',
                 remote_filepath=None,
                 create_intermediate_dirs=True,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.ssh_hook = ssh_hook
        self.ssh_conn_id = ssh_conn_id
        self.file_mode = file_mode
        self.file_contents = file_contents
        self.remote_filepath = remote_filepath
        self.create_intermediate_dirs = create_intermediate_dirs

    def execute(self, context):
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
                sftp_client = ssh_client.open_sftp()

                remote_folder = os.path.dirname(self.remote_filepath)
                if self.create_intermediate_dirs:
                    _make_intermediate_dirs(
                        sftp_client=sftp_client,
                        remote_directory=remote_folder,
                    )
                self.log.info("Starting to transfer file to %s", self.remote_filepath)

                file_contents_fo = StringIO(self.file_contents)
                sftp_client.putfo(file_contents_fo, self.remote_filepath)

                if self.file_mode is not None:
                    sftp_client.chmod(self.remote_filepath, self.file_mode)
        except Exception as e:
            raise AirflowException("Error while uploading to {0}, error: {1}"
                                   .format(self.remote_filepath, str(e)))

        return self.remote_filepath


