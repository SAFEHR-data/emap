import os
import pytest

from pathlib import Path
from emap_runner.global_config import GlobalConfiguration

this_dir = os.path.dirname(os.path.abspath(__file__))
config_path = Path(this_dir, "data", "test-global-configuration.yaml")


@pytest.fixture(scope="module")
def config():
    return GlobalConfiguration(config_path)


def test_read_global_config(config: GlobalConfiguration):
    """Configuration should act as a dictionary, populated from a yaml"""

    assert config["git_org_url"] == "https://github.com/inform-health-informatics"


def test_set_global_config(config: GlobalConfiguration):
    """Ensure the global configuration is immutable"""

    with pytest.raises(Exception):
        config["a"] = "b"


def test_repositories_from_global_config(config: GlobalConfiguration):
    """Ensure that repository data is present in the global configuration"""

    repos = config.extract_repositories()

    assert len(repos) > 0
    repo = repos[0]
    assert hasattr(repo, "name")
    assert hasattr(repo, "branch")


def test_get_data_for(config: GlobalConfiguration):
    """Test data for an entry we know exists"""

    assert config.get_first("RABBITMQ_PORT", "global") is not None
    assert config.get_first("RABBITMQ_PORT", "core") is not None
