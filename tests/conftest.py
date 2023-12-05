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
def eager_data(input_csv_path: Path) -> pl.DataFrame:
    return pl.read_csv(input_csv_path)


@pytest.fixture
def lazy_data(input_csv_path: Path) -> pl.LazyFrame:
    return pl.scan_csv(input_csv_path, low_memory=False)


@pytest.fixture
def input_parquet_path(data: pl.DataFrame) -> Path:
    """
    Uses the CSV data fixture to create a parquet file.
    """
    path = Path("tests/data/input.parquet")
    data.write_parquet(path)
    return path


@pytest.fixture
def input_xlsx_path(data: pl.DataFrame) -> Path:
    """
    Uses the CSV data fixture to create a xlsx file.
    """
    path = Path("tests/data/input.xlsx")
    data.write_excel(path)
    return path
