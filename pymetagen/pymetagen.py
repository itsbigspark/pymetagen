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
        input_data: pl.DataFrame | pl.LazyFrame,
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
import polars as pl

from pymetagen.pymetagen.data_loader import DataLoader
from pymetagen.pymetagen.datatypes import MetaGenSupportedLoadingModes


class MetaGen:
    def __init__(
        self,
        input_data: str | pl.DataFrame | pl.LazyFrame,
        output_path: str,
        mode: MetaGenSupportedLoadingModes = MetaGenSupportedLoadingModes.lazy,
        create_regex: bool | None = None,
        descriptions: pl.LazyFrame | None = None,
    ):
        self.data = (
            self.load_data(input_data, mode)
            if isinstance(input_data, str)
            else input_data
        )
        self.output_path = output_path
        self.create_regex = create_regex
        self.descriptions = descriptions

    def write_metadata(self) -> None:
        basename, ext_output = tuple(
            os.path.basename(self.output_path).split('.')
        )
        if 'csv' in ext_output:
            self.write_csv_metadata()
        elif 'xlsx' in ext_output:
            self.write_excel_metadata()
        elif 'json' in ext_output:
            self.write_json_metadata()
        elif ext_output == '.parquet':
            self.write_parquet_metadata()

    def load_data(
        self,
        input_file: str,
        mode: MetaGenSupportedLoadingModes = MetaGenSupportedLoadingModes.lazy,
    ) -> pl.DataFrame | pl.LazyFrame:
        data_loader = DataLoader(input_file, mode)
        return data_loader.data
