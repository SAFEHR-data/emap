import os
import yaml

from emap_runner.log import logger
from emap_runner.setup.repos import Repository, Repositories


class GlobalConfiguration(dict):
    """A configuration constructed from an existing .yaml file"""

    # Non-exhaustive list of expected sections in the global configuration
    possible_sections = (
        "rabbitmq",
        "ids",
        "uds",
        "informdb",
        "omop",
        "dates",
        "global",
        "glowroot",
        "common",
    )

    def __init__(self, filename: str):
        super().__init__()

        with open(filename, "r") as f:
            self.update(yaml.load(f, Loader=yaml.FullLoader))

        self.filename = filename
        self._update_dates()

    def __setitem__(self, key, value):
        raise ValueError(f"Cannot set {key}. Global configuration is immutable")

    def extract_repositories(self, default_branch_name: str = "master") -> Repositories:
        """Extract repository instances for all those present in the config"""

        repos = Repositories()

        for name, data in self["repositories"].items():

            if data is None:
                data = {"repo_name": name}

            repo = Repository(
                name=data.get("repo_name", name),
                main_git_url=self["main_git_dir"],
                branch=data.get("branch", default_branch_name),
            )

            repos.append(repo)

        return repos

    def get(self, *keys: str) -> str:
        """
        Get a value from this configuration based on a set of descending
        keys. e.g. repositories -> Emap-Core -> branch
        """

        if len(keys) == 0:
            raise ValueError("Must have at least one key")

        elif len(keys) == 1:
            return self[keys[0]]

        elif len(keys) == 2:
            return self[keys[0]][keys[1]]

        elif len(keys) == 3:
            return self[keys[0]][keys[1]][keys[2]]

        raise ValueError(f"Expecting at most 3 keys. Had: {keys}")

    def create_or_update_config_dir_from(self, repositories: Repositories) -> None:
        """
        Update the config/ directory with the data present in this global
        configuration
        """

        if not os.path.exists(repositories.config_dir_path):
            logger.info("Creating config/")
            os.mkdir(repositories.config_dir_path)
        else:
            logger.info("Updating config directory")

        for env_file in repositories.environment_files:

            self._substitute_vars(env_file)

            env_file.write(directory=repositories.config_dir_path)

            for line in env_file.unchanged_lines:
                logger.warn(
                    f"{line.strip()[:29]:30s} in {env_file.basename[:10]:11s} "
                    f"was not updated from {self.filename}"
                )

        return None

    def _substitute_vars(self, env_file: "EnvironmentFile") -> None:
        """
        For all the standard types of configuration that may be present an
        environment file update them with those from this global configuration
        """

        for i, line in enumerate(env_file.lines):

            if line.startswith("#"):
                env_file.set_comment_line_at(line, idx=i)
                continue

            key, value = line.split("=")  # e.g. IDS_SCHEMA=schemaname

            try:
                value = self.get_first(key, env_file.basename)
                env_file.set_new_line_at(f"{key}={value}\n", idx=i)

            except KeyError:
                continue

        return None

    def get_first(self, key: str, section: str) -> str:
        """Get the first value of a key within a section of this global
        configuration. If it cannot be found then use the top-level"""

        if section in self and key in self[section]:
            """
            e.g.
            global:
                RABBITMQ_PORT: 5678
            """
            return self.get(section, key)

        for section in self.possible_sections:

            if section in self:
                if key in self[section]:
                    return self.get(section, key)

        if key in self:
            return self.get(key)

        raise KeyError(f"Failed to find {key} in any part of {self.filename}")

    def _update_dates(self) -> None:
        """Update the dates based on the global date"""

        def _date_or_empty_string(x):
            """Date time formatted for Java or an empty string"""

            date = self["dates"][x]
            if date is None:
                return " "

            # Format as e.g. 2020-06-04T00:00:00.00Z
            d, t = date.date(), date.time()
            return (
                f"{d.year}-{d.month:02d}-{d.day:02d}T"
                f"{t.hour:02d}:{t.minute:02d}:{t.second:02d}.00Z"
            )

        self["ids"]["IDS_CFG_DEFAULT_START_DATETIME"] = _date_or_empty_string("start")
        self["ids"]["IDS_CFG_END_DATETIME"] = _date_or_empty_string("end")

        return None
