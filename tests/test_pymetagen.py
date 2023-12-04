from pathlib import Path

import polars as pl
import pytest

from pymetagen import MetaGen
from pymetagen.datatypes import MetaGenSupportedLoadingModes

input_paths = ["input_csv_path", "input_parquet_path", "input_xlsx_path"]


class TestMetaGen:
    def test_init(self, data: pl.DataFrame, tmp_dir_path: Path):
        MetaGen(
            data=data,
            outpath=tmp_dir_path / "test.csv",
        )


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
        tmp_dir_path: Path,
        mode: MetaGenSupportedLoadingModes,
        request: pytest.FixtureRequest,
    ):
        path: Path = request.getfixturevalue(path)
        MetaGen.from_path(
            path=path,
            outpath=tmp_dir_path / "test.csv",
            mode=mode,
        )
