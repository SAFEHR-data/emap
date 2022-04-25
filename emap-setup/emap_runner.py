import os
import argparse


from emap_setup.read_config import ConfigFile
from emap_setup.parser import Parser
from emap_setup.setup.config_dir_setup import create_or_update_config_dir
from emap_setup.setup.repo_setup import RepoSetup
from emap_setup.docker.docker_runner import DockerRunner
from emap_setup.utils import TimeWindow
from emap_setup.validation.validation_runner import ValidationRunner


def create_parser() -> Parser:
    """Create a custom argument parser"""

    parser = Parser(description='Setup, update and run an instance of EMAP')

    parser.add_argument(
        '-f', '--filename',
        help='Path to the .yaml file containing the global configuration.\n'
             'Default: global-configuration.yaml',
        default='global-configuration.yaml'
    )

    subparsers = parser.add_subparsers(help='sub-command help',
                                       dest='subcommand')

    setup_parser = subparsers.add_parser(
        'setup',
        help='Initialise/update repository directories'
    )

    setup_type_group = setup_parser.add_mutually_exclusive_group()
    setup_type_group.add_argument(
        '-i', '--init',
        help='Clone repositories and create config dir',
        default=False,
        action='store_true'
    )
    setup_type_group.add_argument(
        '-u', '--update',
        help='Update repositories and config files',
        default=False,
        action='store_true'
    )
    setup_type_group.add_argument(
        '-c', '--clean',
        help='Clean the repositories and config files',
        default=False,
        action='store_true'
    )

    docker_parser = subparsers.add_parser(
        'docker',
        help='Run the docker instance'
    )
    docker_parser.add_argument(
        'docker_compose_args',
        help='Subcommands to pass to docker-compose. E,g, up, ps, down',
        nargs='+'
    )
    docker_parser.add_argument(
        '-fake', '--fake-epic',
        help='Include services for fake clarity and caboodle servers',
        default=False,
        action='store_true'
    )

    validation_parser = subparsers.add_parser(
        'validation',
        help='Run validation of the full pipeline'
    )

    validation_type = validation_parser.add_mutually_exclusive_group()
    validation_type.add_argument(
        '-r', '--run',
        help='Run the validation',
        default=False,
        action='store_true'
    )

    validation_parser.add_argument(
        'start_date',
        type=str,
        help='Date at which to start parsing messages. e.g. 7 days ago',
        default='7 days ago'
    )
    validation_parser.add_argument(
        'end_date',
        type=str,
        help='Date at which to start parsing messages. e.g. today',
        default='today'
    )

    return parser


class EMAPRunner:

    def __init__(self,
                 args:        argparse.Namespace,
                 config_file: ConfigFile):

        self.args = args
        self.config_file = config_file
        self.main_dir = os.path.dirname(os.path.abspath(__file__))

    def setup(self) -> None:
        """Run the setup"""

        repo_setup = RepoSetup(main_dir=self.main_dir,
                               git_dir=self.config_file.git_dir,
                               repos=self.config_file.repo_info)

        if self.args.init:
            repo_setup.clone()

        elif self.args.update:
            repo_setup.update()

        elif self.args.clean:
            repo_setup.clean()

        else:
            exit('Please run --help for options')

        create_or_update_config_dir(main_dir=self.main_dir,
                                    config_file=self.config_file)
        return None

    def docker(self) -> None:
        """Run a docker instance"""

        runner = DockerRunner(main_dir=self.main_dir,
                              use_fake_epic=self.args.fake_epic,
                              config=self.config_file.config)

        if 'up' in self.args.docker_compose_args:
            runner.setup_glowroot_password()

        runner.inject_ports()
        runner.run(*self.args.docker_compose_args)

        return None

    def validation(self) -> None:
        """Run a validation run of EMAP"""

        runner = ValidationRunner(
            docker_runner=DockerRunner(main_dir=self.main_dir,
                                       config=self.config_file.config),
            time_window=TimeWindow(start_date=self.args.start_date,
                                   end_date=self.args.end_date)
        )

        if self.args.run:
            runner.run()

        else:
            exit('Please run --help for options')

        return None

    def run(self, function_name) -> None:
        """Call a method of this runner instance defined by its name"""
        return getattr(self, function_name)()


def main():
    parser = create_parser()
    args = parser.parse_args()

    if not os.path.exists(args.filename):
        exit(f'Configuration file *{args.filename}* not found. Exiting')

    runner = EMAPRunner(args=args,
                        config_file=ConfigFile(args.filename))

    try:
        runner.run(args.subcommand)

    except AttributeError:
        exit('No recognised command found. Run --help for options')

    return print("All done")


if __name__ == '__main__':
    main()
