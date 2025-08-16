import pytest

from os import mkdir
from os.path import dirname, abspath, exists
from pathlib import Path
from datetime import date

from emap_runner.runner import create_parser, EMAPRunner
from emap_runner.validation.validation_runner import (
    ValidationRunner,
    TemporaryEnvironmentState,
)
from emap_runner.docker.docker_runner import DockerRunner
from emap_runner.global_config import GlobalConfiguration
from emap_runner.utils import TimeWindow
from .utils import work_in_tmp_directory

config_path = Path(
    dirname(abspath(__file__)), "data", "test-global-configuration-only-docs.yaml"
)


@work_in_tmp_directory(to_copy=None)
def test_clone_then_clean_repos():

    parser = create_parser()
    config = GlobalConfiguration(config_path)

    runner = EMAPRunner(args=parser.parse_args(["setup", "-i"]), config=config)
    runner.run("setup")

    # Ensure that the cloned directory exists
    assert exists("emap_documentation")
    assert exists("config")

    # and can be cleaned
    runner = EMAPRunner(args=parser.parse_args(["setup", "-c"]), config=config)
    runner.run("setup")

    assert not exists("emap_documentation")


def test_default_time_window():

    parser = create_parser()
    args = parser.parse_args(["validation"])

    window = TimeWindow(start_date=args.start_date, end_date=args.end_date)
    assert window.end == date.today()
    assert window.start.toordinal() == date.today().toordinal() - 7


@work_in_tmp_directory(to_copy=[config_path])
def test_time_window_is_set():

    config = GlobalConfiguration(Path("test-global-configuration-only-docs.yaml"))

    runner = ValidationRunner(
        docker_runner=DockerRunner(project_dir=Path.cwd(), config=config),
        time_window=TimeWindow(start_date="today", end_date="7 days ago"),
    )

    mkdir("config")
    with open("config/hoover-envs", "w") as file:
        print("HOOVER_DATE_FROM=X\n" "HOOVER_DATE_UNTIL=Y", file=file)

    with TemporaryEnvironmentState(runner.env_dir_path):
        runner._set_time_window_in_envs()

        with open("config/hoover-envs", "r") as file:
            for line in file:
                if line.startswith("HOOVER_DATE_"):
                    assert line.strip().endswith("T00:00:00.00Z")


def test_validation_source_arguments_cannot_be_both_only_hl7_and_hoover():
    """Test that both hl7 and hoover can't be used as only-X arguments"""

    parser = create_parser()

    with pytest.raises(SystemExit):
        _ = parser.parse_args(["validation", "--only-hl7", "--only-hoover"])


def test_validation_source_arguments_set_correct_runner_attributes():
    """Test source parser arguments translate to correct runner attributes"""

    parser = create_parser()

    args = parser.parse_args(["validation", "--only-hl7"])
    global_config = GlobalConfiguration(config_path)

    runner = ValidationRunner(
        docker_runner=DockerRunner(project_dir=Path.cwd(), config=global_config),
        time_window=TimeWindow(args.start_date, args.end_date),
        use_hl7_reader=not args.use_only_hoover,
        use_hoover=not args.use_only_hl7_reader,
    )

    assert runner.use_hl7_reader and not runner.use_hoover


@pytest.mark.parametrize("queue_length,expected", [(0, True), (1, False)])
def test_zero_length_queues(queue_length, expected):
    """Test that stdout from a rabbitmq list queues call is parsed correctly"""

    lines = [
        "\n",
        "name    messages\n",
        f"databaseExtracts        {queue_length}\n",
        "hl7Queue        0\n",
    ]

    method = ValidationRunner._stdout_rabbitmq_lines_have_zero_length_queues
    assert method(lines) == expected
