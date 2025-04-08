import shutil
import tempfile
from pathlib import Path
from typing import Any

import polars as pl
import pytest


@pytest.fixture(scope="session")
def test_data_dir():
    """Copy the test data to a temporary directory to facilitate parallel testing: separate test sessions will not attempt to access the same files at the same time."""
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp_test_dir = Path(tmpdirname)
        shutil.copytree(
            Path("tests/data"),
            tmp_test_dir,
            dirs_exist_ok=True,
        )
        yield tmp_test_dir


@pytest.fixture
def tmp_dir_path(tmpdir) -> Path:
    return Path(tmpdir)


@pytest.fixture
def input_csv_path(test_data_dir: Path) -> Path:
    return test_data_dir / "input.csv"


@pytest.fixture
def eager_data(input_csv_path: Path) -> pl.DataFrame:
    return pl.read_csv(input_csv_path)


@pytest.fixture
def lazy_data(input_csv_path: Path) -> pl.LazyFrame:
    return pl.scan_csv(input_csv_path, low_memory=False)


@pytest.fixture
def input_parquet_path(eager_data: pl.DataFrame, test_data_dir: Path) -> Path:
    """
    Uses the CSV data fixture to create a parquet file.
    """
    path = test_data_dir / "input.parquet"
    eager_data.write_parquet(path)
    return path


@pytest.fixture
def input_xlsx_path(eager_data: pl.DataFrame, test_data_dir: Path) -> Path:
    """
    Uses the CSV data fixture to create a xlsx file.
    """
    path = test_data_dir / "input.xlsx"
    eager_data.write_excel(path)
    return path


@pytest.fixture
def descriptions_csv_path(test_data_dir: Path) -> Path:
    return test_data_dir / "descriptions.csv"


@pytest.fixture
def descriptions_json_path(test_data_dir: Path) -> Path:
    return test_data_dir / "descriptions.json"


@pytest.fixture
def columns_with_nulls() -> dict[str, list[Any]]:
    return {
        "all_nulls": [None, None, None, None, None],
        "no_nulls": [1, 2, 3, 4, 5],
        "mixed": [1, 2, 3, None, None],
    }


@pytest.fixture
def df_eager() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "a": [1, 2, 3, 4, 5],
            "b": [1, 2, 3, 4, 5],
            "c": [1, 2, 3, 4, 5],
        }
    )
