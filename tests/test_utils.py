from __future__ import annotations

from collections.abc import Sequence

import pytest

from pymetagen.utils import (
    InspectionMode,
    get_nested_path,
    map_inspection_modes,
    map_string_to_list_inspection_modes,
)

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
        nested_path = get_nested_path(base_path)
        assert nested_path == expected_result


def test_inspection_modes_enums():
    assert InspectionMode.list() == ["head", "tail", "sample"]
    assert InspectionMode.head == "head"
    assert InspectionMode.head.value == "head"
    assert InspectionMode.list() == list(
        map(InspectionMode, ["head", "tail", "sample"])
    )


@pytest.mark.parametrize(
    ["string_to_map", "expected_result"],
    [
        ("head", [InspectionMode.head]),
        ("tail", [InspectionMode.tail]),
        ("sample", [InspectionMode.sample]),
        ("head,tail", [InspectionMode.head, InspectionMode.tail]),
        ("head,sample", [InspectionMode.head, InspectionMode.sample]),
        ("tail,sample", [InspectionMode.tail, InspectionMode.sample]),
        (
            "head,tail,sample",
            [InspectionMode.head, InspectionMode.tail, InspectionMode.sample],
        ),
        (
            None,
            [InspectionMode.head, InspectionMode.tail, InspectionMode.sample],
        ),
    ],
)
def test_map_string_to_list_inspection_modes(
    string_to_map: str | None, expected_result: Sequence[InspectionMode]
):
    assert (
        map_string_to_list_inspection_modes(string_to_map) == expected_result
    )


@pytest.mark.parametrize(
    ["inspection_modes", "expected_result"],
    [
        (["head"], [InspectionMode.head]),
        (["tail"], [InspectionMode.tail]),
        (["sample"], [InspectionMode.sample]),
        (["head", "tail"], [InspectionMode.head, InspectionMode.tail]),
        (["head", "sample"], [InspectionMode.head, InspectionMode.sample]),
        (["tail", "sample"], [InspectionMode.tail, InspectionMode.sample]),
        (
            ["head", "tail", "sample"],
            [InspectionMode.head, InspectionMode.tail, InspectionMode.sample],
        ),
    ],
)
def test_map_inspection_modes(
    inspection_modes: Sequence[str], expected_result: Sequence[InspectionMode]
):
    assert map_inspection_modes(inspection_modes) == expected_result


@pytest.mark.parametrize(
    "inspection_modes",
    [
        ["head", "tail", "sample", "invalid"],
        ["invalid"],
        ["head", "invalid"],
        [""],
    ],
)
def test_map_inspection_modes_raises_error(inspection_modes: Sequence[str]):
    with pytest.raises(ValueError) as exc_info:
        map_inspection_modes(inspection_modes=inspection_modes)

    assert (
        f"inspection_modes must be one of {InspectionMode.list()}"
        == exc_info.value.args[0]
    )
