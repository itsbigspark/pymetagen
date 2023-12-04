from pathlib import Path

import polars as pl
import pytest


@pytest.fixture
def tmp_dir_path(tmpdir) -> Path:
    return Path(tmpdir)


@pytest.fixture
def input_csv_path() -> Path:
    return Path("tests/data/input.csv")


@pytest.fixture
def data(input_csv_path: Path) -> pl.DataFrame:
    return pl.read_csv(input_csv_path)
