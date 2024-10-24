from unittest.mock import patch, MagicMock, PropertyMock

import pytest

from os import mkdir
from os.path import dirname, abspath, exists
from pathlib import Path
from datetime import date, timedelta

from emap_runner.runner import create_parser, EMAPRunner
from emap_runner.validation.validation_runner import (
    ValidationRunner,
    TemporaryEnvironmentState,
    ValidationRunnerException,
)
from emap_runner.docker.docker_runner import DockerRunner
from emap_runner.global_config import GlobalConfiguration
from emap_runner.utils import TimeWindow, EMAPRunnerException
from .utils import work_in_tmp_directory

config_path_only_docs = Path(
    dirname(abspath(__file__)), "data", "test-global-configuration-only-docs.yaml"
)
config_path_all = Path(
    dirname(abspath(__file__)), "data", "test-global-configuration.yaml"
)


@work_in_tmp_directory(to_copy=None)
def test_clone_then_clean_repos():

    parser = create_parser()
    config = GlobalConfiguration(config_path_only_docs)

    runner = EMAPRunner(args=parser.parse_args(["setup", "-i"]), config=config)
    runner.run()

    # Ensure that the cloned directory exists
    assert exists("emap_documentation")
    assert exists("config")

    # and can be cleaned
    runner = EMAPRunner(args=parser.parse_args(["setup", "-c"]), config=config)
    runner.run()

    assert not exists("emap_documentation")


@work_in_tmp_directory(to_copy=None)
def test_double_clone():
    """
    Reproduce issue #25
    """
    parser = create_parser()
    config = GlobalConfiguration(config_path_only_docs)

    args = ["setup", "-i"]
    runner = EMAPRunner(args=parser.parse_args(args), config=config)
    runner.run()
    # Make some un-pushed changes to newly cloned repo
    file_to_keep = Path('emap_documentation/my_favourite_file.txt')
    with open(file_to_keep, 'w') as fh:
        fh.write("cheese")

    assert file_to_keep.exists()

    # try to clone again, should get error
    with pytest.raises(EMAPRunnerException, match='already existed'):
        runner = EMAPRunner(args=parser.parse_args(args), config=config)
        runner.run()

    # my local changes are still there
    assert file_to_keep.exists()



def test_default_time_window():

    parser = create_parser()
    args = parser.parse_args(["validation"])

    window = TimeWindow(start_date=args.start_date, end_date=args.end_date)
    assert window.end == date.today()
    assert window.start.toordinal() == date.today().toordinal() - 7


@work_in_tmp_directory(to_copy=[config_path_only_docs])
def test_time_window_is_set():

    config = GlobalConfiguration(Path("test-global-configuration-only-docs.yaml"))

    runner = ValidationRunner(
        docker_runner=DockerRunner(project_dir=Path.cwd(), config=config),
        time_window=TimeWindow(start_date="today", end_date="7 days ago"),
        timeout=timedelta(minutes=3),
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


@pytest.mark.parametrize(
    "args_list,hoover_in_config,exp_use_hl7,exp_use_hoover,exp_use_waveform,exp_raises",
    [
        ([], True, True, True, False, False),
        ([], False, True, True, False, True), # hoover requested but not present in repos
        (["--no-use-hoover"], True, True, False, False, False),
        (["--no-use-hoover"], False, True, False, False, False),
        (["--no-use-hl7"], True, False, True, False, False),
        (["--use-waveform"], True, True, True, True, False),

    ])
def test_validation_source_arguments_set_correct_runner_attributes(args_list,
                                                                   hoover_in_config,
                                                                   exp_use_hl7, exp_use_hoover, exp_use_waveform, exp_raises):
    """Test source parser arguments translate to correct runner attributes"""
    parser = create_parser()

    args = parser.parse_args(["validation", *args_list])
    global_config = GlobalConfiguration(config_path_all if hoover_in_config else config_path_only_docs)

    with patch.object(ValidationRunner, 'run', new_callable=MagicMock) as mock_obj:
        emap_runner = EMAPRunner(args=args, config=global_config)
        if exp_raises:
            with pytest.raises(ValueError, match="hoover requested but is missing"):
                validation_runner = emap_runner.run()
        else:
            validation_runner = emap_runner.run()
            mock_obj.assert_called_once()
            assert validation_runner.use_hl7_reader == exp_use_hl7
            assert validation_runner.use_hoover == exp_use_hoover
            assert validation_runner.use_waveform == exp_use_waveform


@pytest.mark.parametrize(
    "num_trues,timeout_seconds,expect_raises",
    [
        (0, 1, False),
        (1, 1, False),
        (2, 1, True),
    ])
def test_validation_timeout(num_trues, timeout_seconds, expect_raises):
    parser = create_parser()
    args = parser.parse_args(["validation", "--use-waveform", "--timeout", str(timeout_seconds / 3600)])
    global_config = GlobalConfiguration(config_path_all)

    # simulate having to go through the check and wait loop a variable number of times
    def mock_has_populated_queues(num_trues: int):
        call_count = 0

        def inner_mock_has_populated_queues():
            nonlocal call_count
            call_count += 1
            return call_count <= num_trues
        return inner_mock_has_populated_queues

    with patch.multiple(ValidationRunner, wait_secs=0.6, final_wait_secs=0):
        with patch.object(ValidationRunner, '_run_emap', new_callable=MagicMock) as run_emap:
            with patch.object(ValidationRunner, '_has_populated_queues', new_callable=PropertyMock) as populated_queues:
                with patch.object(ValidationRunner, '_save_logs_and_stop', new_callable=MagicMock) as stop:
                    populated_queues.side_effect = mock_has_populated_queues(num_trues)
                    # run the whole validation run, with some mocks
                    emap_runner = EMAPRunner(args=args, config=global_config)
                    if expect_raises:
                        with pytest.raises(ValidationRunnerException, match="Waiting for queue timed out"):
                            emap_runner.run()
                    else:
                        emap_runner.run()
                    run_emap.assert_called_once()
                    stop.assert_called_once()
                    populated_queues.assert_called()

@pytest.mark.parametrize("queue_length,expected", [(0, True), (1, False)])
def test_zero_length_queues(queue_length, expected):
    """Test that stdout from a rabbitmq list queues call is parsed correctly"""

    lines = [
        "\n",
        "name    messages\n",
        f"databaseExtracts        {queue_length}\n",
        "hl7Queue        0\n",
    ]

    method = ValidationRunner._stdout_rabbitmq_queues_all_zero_length
    assert method(lines) == expected
