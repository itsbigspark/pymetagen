"""
PyMetaGen
=========

Python Metadata Generator


- Command Line Interface

$ pymetagen -re --input (first argument) $(IN_FILE) \ --sheet-name 
 --output  (-o) $(OUT_METADATA) \
--regex Array(Field Names) --descriptions JSON_FILE | DATA_LIKE_FILE


- Python Implementation

Polars implementation
---------------------
import os

class MetaGen:
    def __init__(
        self,
        input_data: DataFrameT,
        output_path: str,
        create_regex: bool | None = None,
        descriptions: pl.LazyFrame | None  = None,
    )

    self.data = self.load_data(input_data)
    self.output_path = output_path
    self.create_regex = create_regex
    self.descriptions = descriptions

    def write_metadata(self) -> None:
        basename, ext_output = tuple(
            os.path.basename(self.output_file).split('.')
        )
        if 'csv' in ext_output:
            self.write_csv_metadata()
        elif 'xlsx' in ext_output:
            self.write_excel_metadata()
        elif 'json' in ext_output:
            self.write_json_metadata()
        elif ext_outpt == '.parquet':
            self.write_parquet_metadata()
    
    def infer_datatypes(self):
        pass
"""
import os
from pathlib import Path

import polars as pl

from pymetagen.dataloader import DataLoader, LazyDataLoader
from pymetagen.datatypes import MetaGenSupportedLoadingModes
from pymetagen.typing import DataFrameT


class MetaGen:
    def __init__(
        self,
        data: DataFrameT,
        outpath: Path,
        create_regex: bool | None = None,
        descriptions: pl.LazyFrame | None = None,
    ):
        self.data = data
        self.output_path = outpath
        self.create_regex = create_regex
        self.descriptions = descriptions

    def write_metadata(self) -> None:
        basename, ext_output = tuple(
            os.path.basename(self.output_path).split(".")
        )
        if "csv" in ext_output:
            self.write_csv_metadata()
        elif "xlsx" in ext_output:
            self.write_excel_metadata()
        elif "json" in ext_output:
            self.write_json_metadata()
        elif ext_output == ".parquet":
            self.write_parquet_metadata()

    @classmethod
    def from_path(
        cls,
        path: Path,
        outpath: Path,
        create_regex: bool | None = None,
        descriptions: pl.LazyFrame | None = None,
        mode: MetaGenSupportedLoadingModes = MetaGenSupportedLoadingModes.LAZY,
    ) -> DataFrameT:
        mode_mapping = {
            MetaGenSupportedLoadingModes.LAZY: LazyDataLoader,
            MetaGenSupportedLoadingModes.FULL: DataLoader,
        }
        loader = mode_mapping[mode](path)
        data = loader()

        return cls(
            data=data,
            outpath=outpath,
            create_regex=create_regex,
            descriptions=descriptions,
        )
