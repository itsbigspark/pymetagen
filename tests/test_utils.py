from __future__ import annotations

import pytest

from pymetagen.utils import get_nested_parquet_path

input_paths = ["input_csv_path", "input_parquet_path", "input_xlsx_path"]


class TestMetaGenUtils:
    @pytest.mark.parametrize(
        ["base_path", "expected_result"],
        [
            ("tests/data/input.parquet", "tests/data/input.parquet"),
            (
                "tests/data/input_a_partition.parquet",
                "tests/data/input_a_partition.parquet/*/*.parquet",
            ),
            (
                "tests/data/input_ab_partition.parquet",
                "tests/data/input_ab_partition.parquet/*/*/*.parquet",
            ),
            (
                "tests/data/input_abc_partition.parquet",
                "tests/data/input_abc_partition.parquet/*/*/*/*.parquet",
            ),
        ],
    )
    def test_get_nested_parquet_path(
        self, base_path: str, expected_result: str
    ):
        nested_path = get_nested_parquet_path(base_path)
        assert nested_path == expected_result
