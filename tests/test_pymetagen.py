from pathlib import Path

import polars as pl

from pymetagen import MetaGen


class TestMetaGen:
    def test_init(self, data: pl.DataFrame, tmp_dir_path: Path):
        MetaGen(
            data=data,
            outpath=tmp_dir_path / "test.csv",
        )

    def test_from_csv(self, input_csv_path: Path, tmp_dir_path: Path):
        MetaGen.from_path(
            path=input_csv_path,
            outpath=tmp_dir_path / "test.csv",
        )
