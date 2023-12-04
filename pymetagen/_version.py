"""
Versioning utilities for pymetagen.

Do not use this module directly for the version number. 
Instead, use pymetagen.__version__ to get the
version number.
"""
from __future__ import annotations

import subprocess
from importlib.metadata import version
from pathlib import Path

import regex as re


def _get_version(
    package_name: str = "pymetagen",
    pyproject_path: Path | str = Path("pyproject.toml"),
    tag_prefix: str = "pymetagen-",
    diff_suffix: str = "*",
    no_diff_suffix: str = "",
):
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
