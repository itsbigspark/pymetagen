"""
Version Utils
==============
Do not use this module directly for the version number.
Instead, use pymetagen.__version__ to get the
version number.
"""
from __future__ import annotations

import regex as re
import subprocess

from importlib.metadata import version
from pathlib import Path


def _get_version(
    package_name: str = "pymetagen",
    pyproject_path: Path | str = Path("pyproject.toml"),
    tag_prefix: str = "pymetagen-",
    diff_suffix: str = "*",
    no_diff_suffix: str = "",
):
    """
    Checks the package version against the version in the pyproject.toml
    and the git tag.

    If there is a difference between the package version and the pyproject.toml
    version, then the pyproject.toml version is used (this can occur when
    the version number has been incremented but 'poetry install'
    has not been run).

    If there is difference in the diff between local repo and the tag,
    then diff_suffix is appended to the version number.
    Otherwise, no_diff_suffix is appended.

    Args:
        package_name: name of the package, defaults to 'pymetagen'
        pyproject_path: path to the pyproject.toml file.
                        Defaults to 'pyproject.toml'
        tag_prefix: prefix of the git tag, defaults to 'pymetagen-'
        diff_suffix: suffix to append to the version number if there
                     is a difference between the local repo and the tag.
                     Defaults to '*'
        no_diff_suffix: suffix to append to the version number if there is
                        no difference between the local repo and the tag.
                        Defaults to ''
    Returns:
        version number with suffix appended if there is a difference between
        the local repo and the tag. Otherwise, the version number with no
        suffix appended.
    """

    base_version = version(package_name)
    if not isinstance(pyproject_path, Path):
        pyproject_path = Path(pyproject_path)

    if pyproject_path.exists():
        _pyproject_version = re.search(
            r"version = \"(?P<version>\d+\.\d+\.\d+[a-z]*\d*)\"",
            pyproject_path.read_text(),
            re.MULTILINE,
        ).group("version")

        if base_version != _pyproject_version:
            base_version = _pyproject_version

    try:
        diff = subprocess.Popen(
            f"git diff {tag_prefix}{base_version}",
            stdin=None,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
        )

        (d, _) = diff.communicate()
        if diff.returncode != 0:
            # either tag does not exists, not in git repo etc.
            raise Exception(f"git diff returned {diff.returncode}")
        suffix = diff_suffix if d else no_diff_suffix

    except Exception:
        suffix = no_diff_suffix

    return base_version + suffix
