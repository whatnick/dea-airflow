import json
from base64 import b64decode
from select import select

from airflow import AirflowException
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class SSHRunMixin:
    @apply_defaults
    def __init__(self,
                 ssh_conn_id=None,
                 ssh_hook=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.log.info('Inside SSHRunMixin Init Function')
        self.ssh_hook = ssh_hook
        self.ssh_conn_id = ssh_conn_id

    def _run_ssh_command_and_return_output(self, command) -> (int, str):
        # Copied from ssh_operator.py . It's not reusable from there.
        try:
            if self.ssh_conn_id:
                if self.ssh_hook and isinstance(self.ssh_hook, SSHHook):
                    self.log.info("ssh_conn_id is ignored when ssh_hook is provided.")
                else:
                    self.log.info("ssh_hook is not provided or invalid. " +
                                  "Trying ssh_conn_id to create SSHHook.")
                    self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id,
                                            timeout=self.timeout)

            if not self.ssh_hook:
                raise AirflowException("Cannot operate without ssh_hook or ssh_conn_id.")

            if not command:
                raise AirflowException("SSH command not specified. Aborting.")

            with self.ssh_hook.get_conn() as ssh_client:
                self.log.info("Running command: %s", command)

                # set timeout taken as params
                stdin, stdout, stderr = ssh_client.exec_command(command=command,
                                                                get_pty=False,
                                                                timeout=self.timeout,
                                                                )
                # get channels
                channel = stdout.channel

                # closing stdin
                stdin.close()
                channel.shutdown_write()

                agg_stdout = b''
                agg_stderr = b''

                # capture any initial output in case channel is closed already
                stdout_buffer_length = len(stdout.channel.in_buffer)

                if stdout_buffer_length > 0:
                    agg_stdout += stdout.channel.recv(stdout_buffer_length)

                # read from both stdout and stderr
                while not channel.closed or \
                        channel.recv_ready() or \
                        channel.recv_stderr_ready():
                    readq, _, _ = select([channel], [], [], self.timeout)
                    for c in readq:
                        if c.recv_ready():
                            line = stdout.channel.recv(len(c.in_buffer))
                            line = line
                            agg_stdout += line
                            self.log.info(line.decode('utf-8').strip('\n'))
                        if c.recv_stderr_ready():
                            line = stderr.channel.recv_stderr(len(c.in_stderr_buffer))
                            line = line
                            agg_stderr += line
                            self.log.warning(line.decode('utf-8').strip('\n'))
                    if stdout.channel.exit_status_ready() \
                            and not stderr.channel.recv_stderr_ready() \
                            and not stdout.channel.recv_ready():
                        stdout.channel.shutdown_read()
                        stdout.channel.close()
                        break

                stdout.close()
                stderr.close()

                exit_status = stdout.channel.recv_exit_status()

                return exit_status, agg_stdout.decode('utf-8')

        except Exception as e:
            raise AirflowException("PBS Job Completion sensor error: {0}".format(str(e)))


def maybe_decode_base64(data):
    # The SSHOperator will base64 encode output stored in XCOM, if pickling of xcom vals is disabled
    # For now, lets sniff the value and base64 decode it if necessary
    if data.endswith('='):
        return b64decode(data).decode('utf8').strip()
    else:
        return data


# Putting the SSHMixin first, so that it hopefully consumes it's __init__ arguments
class PBSJobSensor(SSHRunMixin, BaseSensorOperator):
    template_fields = ('pbs_job_id',)

    @apply_defaults
    def __init__(self,
                 pbs_job_id: str = None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log.info('Inside PBSJobSensor Init Function')

        self.pbs_job_id = maybe_decode_base64(pbs_job_id)
        self.log.info('Using pbs_job_id: %s', self.pbs_job_id)

    def poke(self, context):
        # qstat json output incorrectly attempts to escape single quotes
        # This can be fixed with sed, or within python, sed  "s/\\\'/'/g"
        pbs_job_id = self.pbs_job_id
        if pbs_job_id.endswith('='):
            self.pbs_job_id = b64decode(pbs_job_id).decode('utf8').strip()
            self.log.info('Decoding pbs_job_id to: %s', self.pbs_job_id)
        else:
            # Lets trust the value given
            self.pbs_job_id = pbs_job_id
            self.log.info('Trusting given pbs_job_id: %s', self.pbs_job_id)

        ret_val, output = self._run_ssh_command_and_return_output(f'qstat -fx -F json {self.pbs_job_id}')
        output = output.replace("\'", "'")
        result = json.loads(output)

        job_state = result['Jobs'][self.pbs_job_id]['job_state']
        return job_state == 'F'
