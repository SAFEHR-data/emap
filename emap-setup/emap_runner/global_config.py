import re

import yaml

from typing import Optional
from pathlib import Path
from emap_runner.log import logger
from emap_runner.setup.repos import Repository, Repositories


class GlobalConfiguration(dict):
    """A configuration constructed from an existing .yaml file"""

    # Non-exhaustive list of expected sections in the global configuration
    possible_sections = (
        "rabbitmq",
        "ids",
        "uds",
        "omop",
        "dates",
        "global",
        "glowroot",
        "common",
        "fake_uds",
        "fake_mssql",
        "waveform",
        "monitoring",
    )

    def __init__(self, filepath: Path):
        """Construct a global configuration dictionary from a file"""
        super().__init__()

        with open(filepath, "r") as f:
            self.update(yaml.load(f, Loader=yaml.FullLoader))

        self.filename = filepath
        self._update_dates()

    def __setitem__(self, key, _):
        raise ValueError(f"Cannot set {key}. Global configuration is immutable")

    def extract_repositories(
        self, branch_name: Optional[str] = None, default_branch_name: str = "develop"
    ) -> Repositories:
        """Extract repository instances for all those present in the config

        :param branch_name: Name of the branch for all repositories, if None
                            then use the branch defined in the global
                            configuration file

        :param default_branch_name: Name of the default branch on each
                                    repository. Used as a default fallback
        """

        repos = Repositories()

        for name, data in self["repositories"].items():

            if data is None:
                data = {"repo_name": name}

            if branch_name is not None:
                data["branch"] = branch_name

            repo = Repository(
                name=data.get("repo_name", name),
                main_git_url=self["git_org_url"],
                branch=data.get("branch", default_branch_name),
                fallback_branch=default_branch_name,
            )

            repos.append(repo)

        return repos

    def get(self, *keys: str) -> str:
        """
        Get a value from this configuration based on a set of descending
        keys. e.g. repositories -> core -> branch
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

        if not repositories.config_dir_path.exists():
            logger.info("Creating config/")
            Path.mkdir(repositories.config_dir_path)
        else:
            logger.info("Updating config directory")

        for env_file in repositories.environment_files:

            self._substitute_vars(env_file)

            env_file.write(directory=repositories.config_dir_path)

            for line in env_file.unchanged_lines:
                logger.warning(
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

            if re.match(r"\s*#", line):
                env_file.set_comment_line_at(line, idx=i)
                continue

            if re.match(r"\s*$", line):
                continue

            key, value = line.split("=")  # e.g. IDS_SCHEMA=schemaname

            try:
                value = self.get_first(key, env_file.basename)
                if value is None:
                    # Don't stringify None, Spring won't understand.
                    # Empty string is the closest alternative.
                    value = ""
                env_file.set_new_line_at(f"{key}={value}\n", idx=i)

            except KeyError:
                continue

        return None

    def get_first(self, key: str, section: str) -> str:
        """
        Search the config for the given key in the following order:
         - In the section given by arg `section`
         - In any of the sections in possible_sections
         - At the top level
         """

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

            if "dates" not in self:
                logger.warning(f"Failed to find any dates in {self}")
                return " "

            date = self["dates"][x]
            if date is None:
                return " "

            # Format as e.g. 2020-06-04T00:00:00.00Z
            d, t = date.date(), date.time()
            return (
                f"{d.year}-{d.month:02d}-{d.day:02d}T"
                f"{t.hour:02d}:{t.minute:02d}:{t.second:02d}.00Z"
            )

        if "ids" not in self:
            logger.warning(f"Failed to find ids in {self}")
            return

        self["ids"]["IDS_CFG_DEFAULT_START_DATETIME"] = self["global"][
            "HOOVER_DATE_FROM"
        ] = _date_or_empty_string("start")

        self["ids"]["IDS_CFG_END_DATETIME"] = self["global"][
            "HOOVER_DATE_UNTIL"
        ] = _date_or_empty_string("end")

        return None

    @property
    def rabbitmq_config_string(self) -> str:
        """String outlining the rabbitmq admin configuration"""

        try:
            # Note: the domain is the same as the glowroot one
            return _domain_port_username_and_password_to_string(
                self.get("glowroot", "DOMAIN"),
                self.get("global", "RABBITMQ_ADMIN_PORT"),
                self.get("rabbitmq", "RABBITMQ_DEFAULT_USER"),
                self.get("rabbitmq", "RABBITMQ_DEFAULT_PASS"),
            )

        except KeyError:
            return "Unknown"

    @property
    def glowroot_config_string(self) -> str:
        """String outlining the glowroot admin configuration"""

        try:
            return _domain_port_username_and_password_to_string(
                self.get("glowroot", "DOMAIN"),
                self.get("glowroot", "GLOWROOT_ADMIN_PORT"),
                self.get("glowroot", "GLOWROOT_USERNAME"),
                self.get("glowroot", "GLOWROOT_PASSWORD"),
            )

        except KeyError:
            return "Unknown"


def _domain_port_username_and_password_to_string(*args: str) -> str:
    return (
        f"Domain:    http://{args[0]}:{args[1]}\n"
        f"Username:  {args[2]}\n"
        f"Password:  {args[3]}"
    )
