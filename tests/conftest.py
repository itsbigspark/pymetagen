from pathlib import Path
from typing import Any

import polars as pl
import pytest


@pytest.fixture
def tmp_dir_path(tmpdir) -> Path:
    return Path(tmpdir)


@pytest.fixture
def input_csv_path() -> Path:
    return Path("tests/data/input.csv")


@pytest.fixture
def eager_data(input_csv_path: Path) -> pl.DataFrame:
    return pl.read_csv(input_csv_path)


@pytest.fixture
def lazy_data(input_csv_path: Path) -> pl.LazyFrame:
    return pl.scan_csv(input_csv_path, low_memory=False)


@pytest.fixture
def input_parquet_path(eager_data: pl.DataFrame) -> Path:
    """
    Uses the CSV data fixture to create a parquet file.
    """
    path = Path("tests/data/input.parquet")
    eager_data.write_parquet(path)
    return path


@pytest.fixture
def input_xlsx_path(eager_data: pl.DataFrame) -> Path:
    """
    Uses the CSV data fixture to create a xlsx file.
    """
    path = Path("tests/data/input.xlsx")
    eager_data.write_excel(path)
    return path


@pytest.fixture
def descriptions_csv_path() -> Path:
    return Path("tests/data/descriptions.csv")


@pytest.fixture
def descriptions_json_path() -> Path:
    return Path("tests/data/descriptions.json")


@pytest.fixture
def columns_with_nulls() -> dict[str, list[Any]]:
    return {
        "all_nulls": [None, None, None, None, None],
        "no_nulls": [1, 2, 3, 4, 5],
        "mixed": [1, 2, 3, None, None],
    }
