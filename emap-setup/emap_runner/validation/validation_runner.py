from subprocess import Popen
from datetime import date, timedelta
import time
from typing import Union, List
from pathlib import Path

from emap_runner.files import EnvironmentFile
from emap_runner.utils import EMAPRunnerException


class ValidationRunnerException(EMAPRunnerException):
    """Exception for when a validation issue occurs"""


class ValidationRunner:
    wait_secs = 120
    final_wait_secs = 600

    def __init__(
        self,
        docker_runner: "DockerRunner",
        time_window: "TimeWindow",
        timeout: timedelta,
        should_build: bool = True,
        use_hl7_reader: bool = True,
        use_hoover: bool = True,
        use_waveform: bool = False,
    ):
        """Validation runner that will be run over a time window"""

        self.should_build = should_build
        self.use_hl7_reader = use_hl7_reader
        self.use_hoover = use_hoover
        self.use_waveform = use_waveform

        self.docker = docker_runner
        self.time_window = time_window
        self.timeout = timeout

    def run(self) -> None:
        """Run a validation run pipeline by constructing services and waiting
        until all message queues are empty"""

        with TemporaryEnvironmentState(self.env_dir_path):
            self._set_time_window_in_envs()
            self._run_emap()
            self._wait_for_queue_to_empty()
            self._save_logs_and_stop()

        return None

    @property
    def env_dir_path(self) -> Path:
        return Path(self.docker.project_dir, "config")

    @property
    def log_file_prefix(self) -> Path:
        """Common prefix used for all log files"""
        return Path(self.logs_directory, f"rebuild_log_{date.today()}")

    @property
    def logs_directory(self) -> Path:
        return Path(self.docker.project_dir, "validation_logs")

    def _create_logs_directory(self) -> None:
        """Create a directory just for logging"""

        if not self.logs_directory.exists():
            self.logs_directory.mkdir()

        return None

    def _set_time_window_in_envs(self) -> None:
        """Set the time window in all the required files"""

        for item in self.env_dir_path.iterdir():
            if str(item).startswith(".") or not str(item).endswith("-envs"):
                continue

            self._set_time_window_in_env_file(
                EnvironmentFile(Path(self.env_dir_path, item))
            )

        return None

    def _should_set_date_at(self, string: str) -> bool:
        """Should the date be set at either the start or the end?"""

        try:
            return (
                not getattr(self.time_window, string).is_default
                or self.docker.config.get("dates", string) is None
            )

        except KeyError:
            return True

    def _set_time_window_in_env_file(self, file: EnvironmentFile) -> None:
        """Set the correct time stamps in the environment file"""

        if self._should_set_date_at("start"):
            file.replace_value_of(
                "IDS_CFG_DEFAULT_START_DATETIME", self.time_window.start_stamp
            )
            file.replace_value_of("HOOVER_DATE_FROM", self.time_window.start_stamp)

        if self._should_set_date_at("end"):
            file.replace_value_of("IDS_CFG_END_DATETIME", self.time_window.end_stamp)
            file.replace_value_of("HOOVER_DATE_UNTIL", self.time_window.end_stamp)

        file.write(directory=self.env_dir_path)

        return None

    def _run_emap(self) -> None:
        """Run the services that constitute EMAP"""

        _ = input(
            f"About to run a validation run with:\n"
            f"{'Schema:':20s}{self.docker.config['uds']['UDS_SCHEMA']}\n"
            f"{'Time window':20s}{self.time_window.start_stamp} -> {self.time_window.end_stamp}\n"
            f"{'On domain':20s}{self.docker.config['glowroot']['DOMAIN']}\n"
            f"Press any key to continue"
        )

        self._create_logs_directory()
        self.docker.run("down")

        if self.should_build:
            self.docker.run("build")

        self.docker.run("up -d cassandra rabbitmq")
        self.docker.setup_glowroot_password()
        self.docker.run("up -d glowroot-central")
        self.docker.run("ps")

        """
        Running core before data sources requires RabbitMQ to be up and running. A time delay was added to overcome
        the time delay required for RabbitMQ to be there. Should this prove not to be sufficient, then the running order
        may need to change, i.e. the data sources to be started first (as background services though!).
        """
        _ = Popen(
            "sleep 180 && "
            + self.docker.base_docker_compose_command
            + "up -d core",
            shell=True,
        )

        if self.use_hl7_reader:
            self.docker.run(
                "up --exit-code-from hl7-reader hl7-reader",
                output_filename=f"{self.log_file_prefix}_hl7-reader.txt",
            )

        if self.use_hoover:
            self.docker.run(
                "up --exit-code-from hoover hoover",
                output_filename=f"{self.log_file_prefix}_hoover.txt",
            )

        # Assume for the time being that this validation option is only to control whether
        # the waveform *reader* is brought up, which will be reading from an HL7 dump file
        # and therefore doesn't require the waveform-generator.
        if self.use_waveform:
            self.docker.run(
                "up --exit-code-from waveform-reader waveform-reader",
                output_filename=f"{self.log_file_prefix}_waveform-reader.txt",
            )


        self.docker.run("ps")

    def _wait_for_queue_to_empty(self) -> None:
        """
        Wait for the rabbitmq queue to be empty
        If it's still going after 10 hours something's gone very wrong and we
        should give up
        """
        start_time_monotonic = time.monotonic()

        while self._has_populated_queues:
            time.sleep(ValidationRunner.wait_secs)
            elapsed_time = timedelta(seconds=time.monotonic() - start_time_monotonic)
            if elapsed_time > self.timeout:
                self._save_logs_and_stop()
                raise ValidationRunnerException(
                    f"Waiting for queue timed out. Elapsed time "
                    f"({elapsed_time}) > timeout ({self.timeout})"
                )

        # exits too keenly from databaseExtracts queue, adding in a wait period
        time.sleep(ValidationRunner.final_wait_secs)

        return None

    @property
    def _has_populated_queues(self) -> bool:
        """Are there queues that are still populated?

        Check the number of messages remaining in the rabbitmmq queue and parse
        an output like:

            name    messages
            hl7Queue        0
            databaseExtracts        0
        """
        output_lines = []

        self.docker.run(
            "exec rabbitmq rabbitmqctl -q list_queues", output_lines=output_lines
        )

        return not self._stdout_rabbitmq_queues_all_zero_length(output_lines)

    @staticmethod
    def _stdout_rabbitmq_queues_all_zero_length(lines: List[str]) -> bool:
        """
        Do a set of output lines generated from querying the rabbitmq
        queues indicate that all queues are empty?
        """

        for line in lines:

            line_items = line.split()
            if len(line_items) == 0:
                continue

            if line_items[-1].isnumeric():
                number = int(line_items[-1])

                if number > 0:
                    return False

        return True

    def _save_logs_and_stop(self) -> None:
        """Save the logs of the required docker containers"""

        self.docker.run("ps")
        for name in ("core", "rabbitmq"):
            self.docker.run(
                f"logs {name}", output_filename=f"{self.log_file_prefix}_{name}.txt"
            )

        self.docker.run("down")
        self.docker.run("ps")

        return None


class TemporaryEnvironmentState:
    def __init__(self, dir_path: Union[Path, str]):
        """
        Context manager for a temporary environment state for which all env
        files are initially cached then re-written

        :param: dir_path: Path to the directory containing XXX-config-envs file
        """

        self._files = {}

        for filename in Path(dir_path).iterdir():
            file_path = Path(dir_path, filename)
            self._files[file_path] = open(file_path, "r").readlines()

    def __enter__(self):
        return self._files

    def __exit__(self, *args, **kwargs):

        for file_path, file_lines in self._files.items():
            with open(file_path, "w") as file:
                file.write("".join(file_lines))
