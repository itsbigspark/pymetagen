from collections.abc import Callable
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from pymetagen import MetaGen, json_metadata_to_pandas
from pymetagen.datatypes import MetaGenSupportedLoadingModes
from pymetagen.exceptions import (
    FileTypeUnsupportedError,
    LoadingModeUnsupportedError,
)

input_paths = ["input_csv_path", "input_parquet_path", "input_xlsx_path"]


@pytest.mark.parametrize(
    "data",
    [
        "eager_data",
        "lazy_data",
    ],
)
class TestMetaGen:
    """
    Test the MetaGen class on both eager and lazy data.

    Each test is parametrized with the data fixture name and should run:

    ```
    data = request.getfixturevalue(data)
    ```

    to get the data.
    """

    def test_init(self, data: str, request: pytest.FixtureRequest):
        data = request.getfixturevalue(data)
        MetaGen(
            data=data,
        )

    def test_compute_metadata(self, data: str, request: pytest.FixtureRequest):
        data = request.getfixturevalue(data)
        metadata = MetaGen(
            data=data,
        ).compute_metadata()

        assert set(metadata.index) == set(data.columns)

    @pytest.mark.parametrize(
        "extension, read_metadata",
        [
            ["csv", lambda x: pd.read_csv(x).set_index("Name")],
            [
                "xlsx",
                lambda x: pd.read_excel(x, engine="openpyxl").set_index(
                    "Name"
                ),
            ],
            ["json", json_metadata_to_pandas],
            ["parquet", pd.read_parquet],
        ],
    )
    def test_write(
        self,
        extension: str,
        data: str,
        tmp_dir_path: Path,
        read_metadata: Callable,
        request: pytest.FixtureRequest,
    ):
        data = request.getfixturevalue(data)
        outpath = tmp_dir_path / f"out.{extension}"

        metagen = MetaGen(data=data)
        metagen.write_metadata(outpath=outpath)

        assert outpath.exists()
        assert outpath.is_file()

        outdata = read_metadata(outpath)

        assert len(outdata) == len(data.columns)
        assert list(outdata.columns) == [
            "Long Name",
            "Type",
            "Description",
            "Min",
            "Max",
            "Min Length",
            "Max Length",
            "# nulls",
            "# empty/zero",
            "# positive",
            "# negative",
            "# unique",
            "Values",
        ]

    @pytest.mark.parametrize(
        "column_name, expected_value",
        [
            ["# nulls", 5],
            ["# empty/zero", 5],
            ["# unique", 1],
            ["Values", [None]],
        ],
    )
    def test_metadata_data_frame_with_null_column(
        self,
        capsys,
        data: str,
        request: pytest.FixtureRequest,
        column_name: str,
        expected_value: int | None,
    ):
        null_data = {
            "data_values": [None, None, None, None, None],
        }
        null_data = pl.DataFrame(null_data, schema={"data_values": pl.Null})
        metagen = MetaGen(data=null_data)
        metadata = metagen.compute_metadata()
        metadata_dict = metadata.to_dict(orient="records").pop()

        assert metadata_dict[column_name] == expected_value


@pytest.mark.parametrize(
    "mode",
    MetaGenSupportedLoadingModes.list(),
)
class TestMetaGenFromPath:
    @pytest.mark.parametrize(
        "path",
        input_paths,
    )
    def test_from_path(
        self,
        path: str,
        mode: MetaGenSupportedLoadingModes,
        request: pytest.FixtureRequest,
    ):
        path: Path = request.getfixturevalue(path)
        MetaGen.from_path(
            path=path,
            mode=mode,
        )

    def test_unsupported_path(
        self,
        tmp_dir_path: Path,
        mode: MetaGenSupportedLoadingModes,
    ):
        with pytest.raises(FileTypeUnsupportedError):
            MetaGen.from_path(
                path=tmp_dir_path / "test.unsupported",
                mode=mode,
            )

    @pytest.mark.parametrize(
        "descriptions_path",
        [
            "descriptions_csv_path",
            "descriptions_json_path",
        ],
    )
    def test_with_descriptions(
        self,
        descriptions_path: str,
        request: pytest.FixtureRequest,
        input_csv_path: Path,
        mode: MetaGenSupportedLoadingModes,
    ):
        descriptions_path: Path = request.getfixturevalue(descriptions_path)

        metagen = MetaGen.from_path(
            path=input_csv_path, descriptions_path=descriptions_path, mode=mode
        )
        metadata = metagen.compute_metadata()

        for field in ["Description", "Long Name"]:
            assert field in metadata.columns
            assert metadata[field].notnull().all()
            assert metadata[field].notna().all()


def test_from_path_unsupported_mode(
    tmp_dir_path: Path,
):
    with pytest.raises(LoadingModeUnsupportedError):
        MetaGen.from_path(
            path=tmp_dir_path / "test.csv",
            mode="unsupported_mode",
        )


def test_write_metadata_unsupported_extension(
    tmp_dir_path: Path, eager_data: pl.DataFrame
):
    metagen = MetaGen(data=pd.DataFrame)
    with pytest.raises(FileTypeUnsupportedError):
        metagen.write_metadata(tmp_dir_path / "test.unsupported")
