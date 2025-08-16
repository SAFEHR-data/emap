import argparse

from pathlib import Path

from emap_runner.parser import Parser
from emap_runner.utils import TimeWindow
from emap_runner.global_config import GlobalConfiguration
from emap_runner.docker.docker_runner import DockerRunner
from emap_runner.validation.validation_runner import ValidationRunner
from emap_runner.utils import EMAPRunnerException


def create_parser() -> Parser:
    """Create a custom argument parser"""

    parser = Parser(description="Setup, update and run an instance of EMAP")

    parser.add_argument(
        "-f",
        "--filename",
        help="Path to the .yaml file containing the global configuration.\n"
        "Default: global-configuration.yaml",
        default="global-configuration.yaml",
    )

    subparsers = parser.add_subparsers(help="sub-command help", dest="subcommand")

    setup_parser = subparsers.add_parser(
        "setup", help="Initialise/update repository directories"
    )

    setup_type_group = setup_parser.add_mutually_exclusive_group()
    setup_type_group.add_argument(
        "-i",
        "--init",
        help="Clone repositories and create config dir",
        default=False,
        action="store_true",
    )
    setup_type_group.add_argument(
        "-u",
        "--update",
        help="Update repositories and config files",
        default=False,
        action="store_true",
    )
    setup_type_group.add_argument(
        "-c",
        "--clean",
        help="Clean the repositories and config files",
        default=False,
        action="store_true",
    )
    setup_type_group.add_argument(
        "-g",
        "--only_update_config_from_global",
        help="Only update the configuration",
        default=False,
        action="store_true",
    )

    setup_parser.add_argument(
        "-b",
        "--branch",
        help="Name of the branch to clone/update to. Overrides those defined "
        "in the global configuration. Falls back to develop if the branch "
        "does not exist",
        default=None,
        type=str,
    )

    docker_parser = subparsers.add_parser("docker", help="Run the docker instance")
    docker_parser.add_argument(
        "docker_compose_args",
        help="Subcommands to pass to docker compose. E.g, up, ps, down",
        nargs="+",
    )

    validation_parser = subparsers.add_parser(
        "validation", help="Run validation of the full pipeline"
    )
    validation_parser.add_argument(
        "-s",
        "--start_date",
        type=str,
        help="Date at which to start parsing messages. Default: 7 days ago",
        default="7 days ago_default",
    )
    validation_parser.add_argument(
        "-e",
        "--end_date",
        type=str,
        help="Date at which to start parsing messages. Default: today",
        default="today_default",
    )
    validation_parser.add_argument(
        "--skip-build",
        help="Skip the building of the docker containers",
        default=False,
        action="store_true",
    )

    validation_source_group = validation_parser.add_mutually_exclusive_group()
    validation_source_group.add_argument(
        "--only-hl7",
        dest="use_only_hl7_reader",
        help="Use only the hl7-reader service (no hoover)",
        default=False,
        action="store_true",
    )
    validation_source_group.add_argument(
        "--only-hoover",
        dest="use_only_hoover",
        help="Use only the hoover service (no hl7-reader)",
        default=False,
        action="store_true",
    )

    config_parser = subparsers.add_parser("config", help="Configuration operations")
    config_parser.add_argument(
        "-r",
        "--print-rabbitmq",
        help="Print the RabbitMQ configuration for quick login",
        default=False,
        action="store_true",
    )
    config_parser.add_argument(
        "-g",
        "--print-glowroot",
        help="Print the glowroot configuration for quick login",
        default=False,
        action="store_true",
    )

    return parser


class EMAPRunner:
    def __init__(self, args: argparse.Namespace, config: GlobalConfiguration):

        self.args = args
        self.global_config = config

    def setup(self) -> None:
        """Run the setup"""

        repos = self.global_config.extract_repositories(branch_name=self.args.branch)

        if self.args.init:
            repos.clean(print_warnings=False)
            repos.clone()

        elif self.args.update:
            repos.update()

        elif self.args.clean:
            return repos.clean()

        elif self.args.only_update_config_from_global:
            pass

        else:
            exit("Please run --help for options")

        self.global_config.create_or_update_config_dir_from(repos)
        return None

    def config(self) -> None:
        """Operations on the configuration only"""

        if self.args.print_rabbitmq:
            print(self.global_config.rabbitmq_config_string)

        if self.args.print_glowroot:
            print(self.global_config.glowroot_config_string)

        return None

    def docker(self) -> None:
        """Run a docker instance"""

        runner = DockerRunner(project_dir=Path.cwd(), config=self.global_config)

        if "up" in self.args.docker_compose_args and not runner.glowroot_is_up:
            runner.setup_glowroot_password()

        runner.run(*self.args.docker_compose_args)

        return None

    def validation(self) -> None:
        """Run a validation run of EMAP"""
        # allow for hoover not to be defined in global config
        use_hoover = ("hoover" in self.global_config["repositories"]) and (not self.args.use_only_hl7_reader)

        runner = ValidationRunner(
            docker_runner=DockerRunner(project_dir=Path.cwd(), config=self.global_config),
            time_window=TimeWindow(
                start_date=self.args.start_date, end_date=self.args.end_date
            ),
            should_build=not self.args.skip_build,
            use_hl7_reader=not self.args.use_only_hoover,
            use_hoover=use_hoover,
        )

        runner.run()

        return None

    def run(self, method_name: str) -> None:
        """Call a method of this runner instance defined by its name"""

        if hasattr(self, method_name):
            return getattr(self, method_name)()

        raise EMAPRunnerException(
            f"Failed to run {method_name} as it did not "
            f"exist as a method. Maybe run --help?"
        )


def main():
    parser = create_parser()
    args = parser.parse_args()

    if not Path(args.filename).exists():
        exit(f"Configuration file {args.filename} not found. Exiting")

    runner = EMAPRunner(args=args, config=GlobalConfiguration(args.filename))
    runner.run(args.subcommand)

    return None


if __name__ == "__main__":
    """Invoke with e.g.

        python runner.py setup -i

    to initialise EMAP by cloning all the required repositories and setting
    files with specific environment variables from the global configuration yaml
    """
    main()
