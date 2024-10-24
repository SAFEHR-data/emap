import os
from pathlib import Path
from subprocess import CalledProcessError, PIPE, Popen
from typing import IO, List, Optional

from emap_runner.files import EnvironmentFile, File
from emap_runner.log import logger
from emap_runner.utils import EMAPRunnerException


class DockerRunnerException(EMAPRunnerException):
    """Exception for something breaking within docker"""


def first_not_none(*args):
    for a in args:
        if a is not None:
            return a
    return None

class DockerRunner:
    """Orchestration for multiple services using docker"""

    def __init__(self,
                 project_dir: Path,
                 config: "GlobalConfiguration",
                 enable_waveform=None,
                 use_fake_waveform=None,
                 use_fake_uds=None,
                 ):
        """Initialise a docker runner with docker-compose.yml files relative
        to the main directory given a specific configuration"""

        self.project_dir = project_dir
        self.emap_dir = project_dir / "emap"
        self.config = config
        self.enable_waveform = first_not_none(enable_waveform, self.config.get("features", "waveform"))
        self.use_fake_waveform = first_not_none(use_fake_waveform, self.config.get("features", "waveform_generator"))
        self.use_fake_uds = first_not_none(use_fake_uds, self.config.get("features", "fake_uds"))

    def run(
        self,
        *docker_compose_args: str,
        output_filename: Optional[str] = None,
        output_lines: Optional[list] = None,
    ) -> None:
        """
        Run docker compose

        :param docker_compose_args: Arguments to pass to the full docker
                                    compose call e.g. ps
        :param output_filename: Name of the file to write the stdout to
        :param output_lines: List to append the stdout to
        """

        self._check_paths_exist()

        cmd = self.base_docker_compose_command.split()
        for arg in docker_compose_args:
            cmd += [x.strip('"') for x in arg.split()]

        logger.info(f'Running:\n {" ".join(cmd)}\n')

        with Popen(
            cmd,
            stdout=PIPE if (output_filename or output_lines is not None) else None,
            bufsize=1,
            universal_newlines=True,
            env=self._all_global_environment_variables(),
        ) as p:

            if output_filename is not None and p.stdout is not None:
                _write_to_file(p.stdout, output_filename)

            elif output_lines is not None and p.stdout is not None:
                for line in p.stdout:
                    output_lines.append(line)

        if p.returncode not in (0, None):
            raise DockerRunnerException(
                f"Process failed with error code: {p.returncode}"
            )

        return None

    @property
    def base_docker_compose_command(self) -> str:
        return (
            "docker compose -f "
            + " -f ".join(str(p) for p in self.docker_compose_paths)
            + f' -p {self.config["EMAP_PROJECT_NAME"]} '
        )

    @property
    def docker_compose_paths(self) -> List[Path]:
        """Paths of all the required docker-compose yamls"""

        paths = [
            self.core_docker_compose_path,
            Path(self.emap_dir, "hl7-reader", "docker-compose.yml"),
        ]
        # Fakes are for testing only. Waveform is a real feature that is currently off
        # by default, except for the waveform generator which is for testing waveform
        # data only.
        if self.use_fake_uds:
            paths.append(Path(self.emap_dir, "core", "docker-compose.fakeuds.yml"))
        if self.enable_waveform:
            paths.append(Path(self.emap_dir, "waveform-reader", "docker-compose.yml"))
            if self.use_fake_waveform:
                paths.append(Path(self.emap_dir, "waveform-generator", "docker-compose.yml"))

        # allow for hoover and to be optional compose path
        if "hoover" in self.config["repositories"]:
            paths.append(Path(self.project_dir, "hoover", "docker-compose.yml"))

        return paths

    @property
    def core_docker_compose_path(self) -> Path:
        return Path(self.emap_dir, "core", "docker-compose.yml")

    def setup_glowroot_password(self) -> None:
        """Run the required command to password protect glowroot"""

        username = self.config["glowroot"]["GLOWROOT_USERNAME"]
        password = self.config["glowroot"]["GLOWROOT_PASSWORD"]

        try:
            self.run(
                'run glowroot-central java -jar "glowroot-central.jar" '
                f"setup-admin-user {username} {password}"
            )

        except CalledProcessError as e:
            raise DockerRunnerException(
                f"{e}\n\n"
                f"Failed to password protect glowroot. Check that the docker "
                f"containers have enough available RAM"
            ) from e

        return None

    def _check_paths_exist(self) -> None:
        """Ensure all the docker compose files exist"""

        paths = self.docker_compose_paths

        if not all(path.exists() for path in paths):
            _paths_str = "\n".join(str(p) for p in paths)
            raise DockerRunnerException(
                f"Cannot run docker-compose. "
                f"At least one path did not exist:\n {_paths_str} "
            )

        return None

    @staticmethod
    def _all_global_environment_variables() -> dict:
        """Dictionary of all global variables present in
        config/global-config-envs added to the currently set env vars"""

        config_dir_path = Path(Path.cwd(), "config")

        if not config_dir_path.exists():
            raise DockerRunnerException(
                "Failed to locate all the env vars ./config dir did not exist"
            )

        env_vars = os.environ.copy()

        for item in config_dir_path.iterdir():
            # only necessary to read the global config variables; rest will be
            # pulled through containers directly
            if item.is_file() and item.stem == "global-config-envs":
                env_vars.update(EnvironmentFile(item).environment_variables)

        return env_vars

    @property
    def glowroot_is_up(self) -> bool:
        """Is glowroot up?"""

        output_lines = []
        self.run("ps", output_lines=output_lines)
        # filter in expression to allow for no services to be running
        glowroot_running = any(
            " running " in line for line in output_lines if "glowroot-central" in line
        )

        logger.info(f"Is glowroot-central running: {glowroot_running}")
        return glowroot_running


def _write_to_file(stdout: IO, filename: str) -> None:
    """Write standard output to a file"""

    with open(filename, "w") as file:
        for line in stdout:
            print(line, end="", file=file)

    return None
