from __future__ import annotations

import datetime
import json
from collections.abc import Sequence
from pathlib import Path

import pytest

from pymetagen.utils import (
    CustomDecoder,
    CustomEncoder,
    InspectionMode,
    get_nested_path,
    map_inspection_modes,
    map_string_to_list_inspection_modes,
    selectively_update_dict,
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
    assert InspectionMode.values() == ["head", "tail", "sample"]
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


class TestMetaGenUtilsCustomJSONDecoder:
    def test_custom_json_decoder(self):
        path = Path("tests/data/sample.json")
        with path.open("r") as f:
            data = json.load(f, cls=CustomDecoder)

        assert data == {
            "name": "John Doe",
            "dob": datetime.date(1990, 1, 1),
            "date": datetime.date(2021, 1, 1),
            "timestamp": datetime.datetime(2021, 1, 1, 0, 0),
            "age": 30,
            "address": {
                "street": "123 Main St",
                "city": "Anytown",
                "state": "AS",
                "zip": "12345",
            },
            "phones": [
                {"type": "home", "number": "123-456-7890"},
                {"type": "work", "number": "123-456-7890"},
            ],
        }


class TestMetaGenUtilsCustomJSONEncoder:
    def test_custom_json_encoder(self, tmp_dir_path: Path):
        data = {
            "name": "John Doe",
            "dob": datetime.date(1990, 1, 1),
            "date": datetime.date(2021, 1, 1),
            "timestamp": datetime.datetime(2021, 1, 1, 0, 0),
            "age": 30,
            "address": {
                "street": "123 Main St",
                "city": "Anytown",
                "state": "AS",
                "zip": "12345",
            },
            "phones": [
                {"type": "home", "number": "123-456-7890"},
                {"type": "work", "number": "123-456-7890"},
            ],
        }
        path = tmp_dir_path / "sample.json"
        with path.open("w") as f:
            json.dump(data, f, cls=CustomEncoder, indent=4)

        assert path.exists()
        with path.open("r") as f:
            loaded_data = json.load(f)

        assert loaded_data == {
            "name": "John Doe",
            "dob": "1990-01-01",
            "date": "2021-01-01",
            "timestamp": "2021-01-01T00:00:00",
            "age": 30,
            "address": {
                "street": "123 Main St",
                "city": "Anytown",
                "state": "AS",
                "zip": "12345",
            },
            "phones": [
                {"type": "home", "number": "123-456-7890"},
                {"type": "work", "number": "123-456-7890"},
            ],
        }


class TestMetaGenUtilsFunctions:
    def test_selectively_update_dict(self):
        dict1 = {
            "a": 1,
            "c": {"d": 2, "e": "e"},
            "f": [1, 2, 3],
            "dt": datetime.datetime(2021, 1, 1),
            "foo": "bar",
            "g": {"h": 1, "i": [1, 2, 3]},
        }
        dict2 = {
            "a": 2,
            "b": 2,
            "c": {"d": 3},
            "f": [4, 5, 6],
            "dt": datetime.datetime(2021, 1, 2),
            "g": {"h": 2, "i": [4, 5, 6]},
        }
        expected_dict = {
            "a": 2,
            "b": 2,
            "c": {"d": 3, "e": "e"},
            "f": [4, 5, 6],
            "dt": datetime.datetime(2021, 1, 2),
            "foo": "bar",
            "g": {"h": 2, "i": [4, 5, 6]},
        }
        updated_dict = selectively_update_dict(dict1, dict2)
        assert updated_dict == expected_dict
