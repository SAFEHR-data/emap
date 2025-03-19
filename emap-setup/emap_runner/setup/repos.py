import git
import shutil

from pathlib import Path
from tqdm import tqdm
from typing import Optional, List

from emap_runner.log import logger
from emap_runner.files import EnvironmentFile
from emap_runner.utils import EMAPRunnerException


class RepoOperationException(EMAPRunnerException):
    """Exception for when a repo cannot be e.g. cloned or updated"""


class _CloneProgressBar(git.RemoteProgress):
    """Progress bar suitable for monitoring the progress of a git operation"""

    def __init__(self):
        super().__init__()
        self.pbar = tqdm()

    def update(self, op_code, cur_count, max_count=None, message=""):
        self.pbar.total = max_count
        self.pbar.n = cur_count
        self.pbar.refresh()


class Repository:
    def __init__(
        self,
        name: str,
        main_git_url: str,
        branch: Optional[str] = None,
        fallback_branch: Optional[str] = None,
    ):
        """
        Git repository

        :param name: Name of the repository relative to the main git organisation
        :param main_git_url: Main git url for the whole project
        :param branch: Specific git branch e.g. master
        :param fallback_branch: git branch to use if branch does not exist
        """

        self.name = name
        self._base_git_url = self._base_url_from_https(main_git_url)
        self.branch = self._branch_or_fallback_branch(branch, fallback_branch)

    def clone(self, ssh: bool = False) -> None:
        """
        Clone this repo

        :param ssh: Clone the repo using ssh rather than https
        """

        if self.local_version_exists:
            raise EMAPRunnerException(f"Cannot clone {self.name} as it already existed")

        logger.info(f"Cloning {self.name:20s} on branch {self.branch:15s}")
        try:
            git.Repo.clone_from(
                url=self.ssh_git_url if ssh else self.https_git_url,
                to_path=self.path,
                branch=self.branch,
                progress=_CloneProgressBar(),
            )

        except git.GitCommandError as e:
            raise RepoOperationException("Repos could not be cloned") from e

        return None

    def update(self) -> None:
        """Update a repo on a specific branch"""
        logger.info(f"Checking out {self}")

        try:

            repo = git.Repo(self.path)
            for remote in repo.remotes:
                remote.fetch()

            repo.git.checkout(self.branch)
            repo.remotes[0].pull()

        except git.GitCommandError as e:
            raise RepoOperationException("Cannot checkout branch") from e

        return None

    def clean(self, print_warnings: bool = True) -> None:
        """
        Clean this repository from the directory by removing it

        :param print_warnings: Should warnings be printed if a folder cannot
                               be removed
        """

        if self.local_version_exists:
            shutil.rmtree(self.path)

        elif print_warnings:
            logger.warning(f"Failed to remove {self.name:30s} as it did not exist")

        return None

    @property
    def local_version_exists(self) -> bool:
        return self.path.exists()

    @property
    def path(self) -> Path:
        """Path to this repository"""
        return Path(Path.cwd(), self.name)

    @property
    def https_git_url(self) -> str:
        """Generate a https URL. e.g. https://github.com/../emap-setup.git"""
        return f"https://{self._base_git_url}/{self.name}"

    @property
    def ssh_git_url(self):
        """Generate a valid ssh URL. e.g. git@github.com:.../emap-setup.git"""

        split_url = self._base_git_url.split("/")
        base_url, directory = split_url[0], split_url[1:]

        return f"git@{base_url}:{directory}"

    @property
    def environment_files(self) -> List[EnvironmentFile]:
        """Create an environment file from the example in this repository"""

        if not self.path.exists():
            raise RepoOperationException(
                "Cannot obtain a set of environment "
                f"files as {self.name} did not exist"
            )

        files = []

        for item in self.path.iterdir():

            path = Path(self.path, item)

            self._add_env_to_files(path, files)
            # in emap monorepo, need to also look in submodules
            if path.is_dir():
                for subdir_path in path.iterdir():
                    self._add_env_to_files(subdir_path, files)

        return files

    @staticmethod
    def _add_env_to_files(path: Path, files: List[EnvironmentFile]):
        if path.is_file() and str(path).endswith(("-envs.EXAMPLE", "-build-args.EXAMPLE")):
            files.append(EnvironmentFile.from_example_file(path))

    @staticmethod
    def _base_url_from_https(string) -> str:
        """
        Convert either a https link into a base url i.e. strip the https prefix
        """
        https = "https://"

        if https in string:
            return string.split(https)[1]

        else:
            raise ValueError(
                f"Failed to parse {string} as a git URL. "
                f"Expecting it to start with {https}"
            )

    def _branch_or_fallback_branch(self, branch, fallback_branch):
        """Branch that exists on remote, either that specified or a fallback"""

        try:
            data = git.Git().execute(
                [f"git ls-remote -h {self.https_git_url}"], shell=True
            )

        except git.GitCommandError as e:
            logger.error(f"{e}\nFailed to check remotes. Assuming branch exists")
            return branch

        remote_branches = [x.split(r"/")[-1] for x in data.split("\n")]

        if any(b == branch for b in remote_branches):
            return branch

        elif any(b == fallback_branch for b in remote_branches):
            logger.warning(f"Falling back to {fallback_branch} for {self.name}")
            return fallback_branch

        raise RepoOperationException(
            f"Failed to find either {branch} or " f"{fallback_branch} on remote"
        )

    def __str__(self):
        return f"Repository({self.name}, branch={self.branch})"


class Repositories(list):
    @property
    def environment_files(self) -> List[EnvironmentFile]:
        """List of all the environment files associated with all the repos"""
        return sum([repo.environment_files for repo in self], [])

    def update(self) -> None:
        return self._run_for_all("update")

    def clone(self) -> None:
        return self._run_for_all("clone")

    def clean(self, print_warnings: bool = True) -> None:
        return self._run_for_all("clean", print_warnings=print_warnings)

    def _run_for_all(self, method_name: str, **kwargs) -> None:

        for repo in self:
            getattr(repo, method_name)(**kwargs)

        return None

    @property
    def config_dir_path(self) -> Path:
        """Path of the configuration directory adjacent to all these repos"""
        return Path(Path.cwd(), "config")
