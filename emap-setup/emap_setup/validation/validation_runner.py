import os
from os.path import join
from subprocess import Popen
from datetime import date
from time import time, sleep

from emap_setup.utils import EnvironmentFile


class ValidationRunner:

    def __init__(self,
                 docker_runner: 'DockerRunner',
                 time_window:   'TimeWindow'):
        """Validation runner that will be run over a time window"""

        self.docker = docker_runner
        self.time_window = time_window

    def run(self) -> None:
        """Run a validation run"""

        with TemporaryEnvironmentSate(self.env_dir_path):
            self._set_time_window_in_envs()
            self._run_emap()
            self._wait_for_queue_to_empty()
            self._save_logs_and_stop()

        return None

    @property
    def env_dir_path(self) -> str:
        return os.path.join(self.docker.main_dir, 'config')

    @property
    def log_file_prefix(self) -> str:
        return f'rebuild_log_{date.today()}'

    def _set_time_window_in_envs(self) -> None:
        """Set the time window in all the required files"""

        for name in os.listdir(self.env_dir_path):
            if name.startswith('.') or not name.endswith('config-envs'):
                continue

            self._set_time_window_in_env_file(EnvironmentFile(name))

        return None

    def _set_time_window_in_env_file(self, file: EnvironmentFile) -> None:
        """Set the correct time stamps in the environment file"""

        file.replace_value_of(
            'IDS_CFG_DEFAULT_START_DATETIME', self.time_window.start_stamp
        )
        file.replace_value_of(
            'IDS_CFG_END_DATETIME', self.time_window.end_stamp
        )
        file.replace_value_of(
            'HOOVER_DATE_FROM', self.time_window.start_stamp
        )
        file.replace_value_of(
            'HOOVER_DATE_UNTIL', self.time_window.end_stamp
        )

        file.write(directory=self.env_dir_path)

        return None

    def _run_emap(self) -> None:

        self.docker.run('down')
        self.docker.setup_glowroot_password()
        self.docker.run('up -d glowroot-central')
        self.docker.run('ps')

        """
        If this is run after the data sources, it would deadlock if the 
        hl7source generates more messages than can fit in the queue, but
        currently emapstar doesn't like being started up unless the queues 
        exist, or start existing very quickly. So, start it up just a little
        after the datasources!
        """
        _ = Popen(['sleep', '180;']
                  + self.docker.base_docker_command.split()
                  + ['up', '-d', 'emapstar'])

        self.docker.run('up --exit-code-from hl7source hl7source',
                        output_filename=f'{self.log_file_prefix}_hl7source.txt')
        self.docker.run('up --exit-code-from hoover hoover',
                        output_filename=f'{self.log_file_prefix}_hoover.txt')

        self.docker.run('ps')

    def _wait_for_queue_to_empty(self, timeout_secs=36000) -> None:
        """
        Wait for the rabbitmq queue to be empty
        If it's still going after 10 hours something's gone very wrong and we
        should give up
        """
        start_time = time()

        while time() - start_time < timeout_secs:

            raise NotImplementedError

        self._save_logs_and_stop()
        exit('Waiting for queue timed out')

    def _save_logs_and_stop(self) -> None:
        """Save the logs of the required docker containers"""

        self.docker.run('ps')
        for name in ('emapstar', 'rabbitmq'):
            self.docker.run(f'logs {name}',
                            output_filename=f'{self.log_file_prefix}_{name}.txt')

        self.docker.run('down')
        self.docker.run('ps')

        return None


class TemporaryEnvironmentSate:

    def __init__(self, dir_path):
        """
        Context manager for a temporary environment state for which all env
        files are initially cached then re-written

        :param: dir_path: Path to the directory containing XXX-config-envs file
        """

        self.files = {join(dir_path, f): open(join(dir_path, f), 'r').readlines()
                      for f in os.listdir(dir_path)}

    def __enter__(self):
        return self.files

    def __exit__(self, *args, **kwargs):

        for file_path, file_lines in self.files.items():
            with open(file_path, 'w') as file:
                file.write(''.join(file_lines))
