import os

from shutil import rmtree, copy
import setuptools
from pathlib import Path
from tempfile import mkdtemp
from functools import wraps
from typing import Optional


def work_in_tmp_directory(to_copy: Optional[list]):
    """Execute a function in a temporary directory"""

    if to_copy is None:
        to_copy = []

    def func_decorator(func):
        @wraps(func)
        def wrapped_function(*args, **kwargs):
            cwd_path = os.getcwd()
            tmpdir_path = mkdtemp()

            for item in to_copy:
                if Path(item).is_dir():
                    setuptools.distutils.dir_util.copy_tree(item, tmpdir_path)
                else:
                    copy(item, tmpdir_path)

            os.chdir(tmpdir_path)
            result = func(*args, **kwargs)
            os.chdir(cwd_path)

            rmtree(tmpdir_path)

            return result

        return wrapped_function

    return func_decorator
