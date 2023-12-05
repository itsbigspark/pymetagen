"""
Data Loader
===========
polars_default_read_csv_options
{
    has_header: bool = True,
    columns: Sequence[int] | Sequence[str] | None = None,
    new_columns: Sequence[str] | None = None,
    separator: str = ",",
    comment_char: str | None = None,
    quote_char: str | None = r'"',
    skip_rows: int = 0,
    dtypes: Mapping[str, PolarsDataType] | Sequence[PolarsDataType] | None = None,
    null_values: str | Sequence[str] | dict[str, str] | None = None,
    missing_utf8_is_empty_string: bool = False,
    ignore_errors: bool = False,
    try_parse_dates: bool = False,
    n_threads: int | None = None,
    infer_schema_length: int | None = N_INFER_DEFAULT,
    batch_size: int = 8192,
    n_rows: int | None = None,
    encoding: CsvEncoding | str = "utf8",
    low_memory: bool = False,
    rechunk: bool = True,
    use_pyarrow: bool = False,
    storage_options: dict[str, Any] | None = None,
    skip_rows_after_header: int = 0,
    row_count_name: str | None = None,
    row_count_offset: int = 0,
    sample_size: int = 1024,
    eol_char: str = "\n",
    raise_if_empty: bool = True,
}
"""

import os
from typing import Any

import polars as pl
from polars.datatypes.constants import N_INFER_DEFAULT
from polars.type_aliases import ParallelStrategy

from pymetagen.pymetagen.datatypes import (
    MetaGenSupportedFileExtensions,
    MetaGenSupportedLoadingModes,
)
from pymetagen.pymetagen.utils import selectively_update_dict


class BaseLoadingOptions:
    def __init__(self) -> None:
        self.columns: list[int] | list[str] | None = None
        self.use_pyarrow: bool = False
        self.n_rows: int | None = (None,)
        self.row_count_name: str | None = None
        self.row_count_offset: int = 0
        self.low_memory: bool = False
        self.rechunk: bool = True

    def parquet_full_loading_options(
        self,
        memory_map: bool,
        storage_options: dict[str, Any] | None,
        parallel: ParallelStrategy,
        pyarrow_options: dict[str, Any] | None,
        use_statistics: bool,
    ) -> dict[str, Any]:
        self.memory_map: memory_map
        self.storage_options: storage_options
        self.parallel = parallel
        self.pyarrow_options = pyarrow_options
        self.use_statistics = use_statistics


POLARS_DEFAULT_READ_CSV_OPTIONS: dict[str, Any] = {
    "columns": None,
    "use_pyarrow": False,
    "n_rows": None,
    "row_count_name": None,
    "row_count_offset": 0,
    "low_memory": False,
    "rechunk": True,
    "has_header": True,
    "new_columns": None,
    "separator": ",",
    "comment_char": None,
    "quote_char": r'"',
    "skip_rows": 0,
    "dtypes": None,
    "null_values": None,
    "missing_utf8_is_empty_string": False,
    "ignore_errors": False,
    "try_parse_dates": False,
    "n_threads": None,
    "infer_schema_length": N_INFER_DEFAULT,
    "batch_size": 8192,
    "encoding": "utf8",
    "storage_options": None,
    "skip_rows_after_header": 0,
    "sample_size": 1024,
    "eol_char": "\n",
    "raise_if_empty": True,
}

LIST_OF_EXCEL_OPTIONS_FROM_CSV_OPTIONS = [
    "storage_options",
    "sample_size",
    "has_header",
    "new_columns",
    "separator",
    "comment_char",
    "quote_char",
    "skip_rows",
    "dtypes",
    "null_values",
    "missing_utf8_is_empty_string",
    "ignore_errors",
    "try_parse_dates",
    "infer_schema_length",
    "n_rows",
    "encoding",
    "low_memory",
    "rechunk",
    "skip_rows_after_header",
    "row_count_name",
    "row_count_offset",
    "eol_char",
    "raise_if_empty",
]


LIST_OF_EXCEL_OPTIONS_FROM_CSV_OPTIONS


class DataLoader:
    def __init__(
        self,
        input_file: str,
        mode: MetaGenSupportedLoadingModes = MetaGenSupportedLoadingModes.lazy,
        polars_read_csv_options: None | dict[str, Any] = None,
    ):
        self.input_file = input_file
        self.polars_read_csv_options = self.update_read_csv_polars_options(
            polars_read_csv_options
        )
        self.data = self.load(mode)

    def load(
        self,
        mode: MetaGenSupportedLoadingModes,
    ) -> pl.LazyFrame:
        _, ext_input = tuple(os.path.basename(self.input_file).split("."))
        if ext_input not in MetaGenSupportedFileExtensions.list():
            raise NotImplementedError(
                f"File {ext_input} not yet implemented. Only supported file"
                f" extensions: {MetaGenSupportedFileExtensions.list()}"
            )
        if "csv" in ext_input:
            return self.load_csv_data(mode)
        elif "xlsx" in ext_input:
            return self.load_excel_data(mode)
        elif ext_input == "parquet":
            return self.load_parquet_data(mode)
        elif "json" in ext_input:
            return self.load_json_data(mode)

    def get_polars_read_excel_options(self):
        return {
            key: value
            for key, value in POLARS_DEFAULT_READ_CSV_OPTIONS
            if key in LIST_OF_EXCEL_OPTIONS_FROM_CSV_OPTIONS
        }

    def update_polars_read_excel_options(
        self,
        sheet_name: str,
    ) -> dict[str, Any]:
        polars_read_excel_options = self.get_polars_read_excel_options()
        polars_read_excel_options["sheet_name"] = sheet_name
        return polars_read_excel_options

    def update_read_csv_polars_options(
        self, polars_read_csv_options: dict[str, Any] | None
    ) -> dict[str, Any]:
        d = POLARS_DEFAULT_READ_CSV_OPTIONS.copy()
        if polars_read_csv_options is None:
            return d
        selectively_update_dict(d, polars_read_csv_options)
        return d

    def load_csv_data(
        self, mode: MetaGenSupportedLoadingModes
    ) -> pl.LazyFrame | pl.DataFrame:
        if mode not in MetaGenSupportedLoadingModes.list():
            raise KeyError(
                f"Unknownn load mode: {mode}. Change to one of supported"
                f" loading modes: {MetaGenSupportedLoadingModes.list()}"
            )
        if mode == MetaGenSupportedLoadingModes.lazy:
            return pl.scan_csv(
                source=self.input_file, **self.polars_read_csv_options
            )
        elif mode == MetaGenSupportedLoadingModes.full:
            return pl.read_csv(
                source=self.input_file, **self.polars_read_csv_options
            ).lazy()

    def load_excel_data(
        self, mode: MetaGenSupportedLoadingModes
    ) -> pl.LazyFrame | pl.DataFrame:
        if mode not in MetaGenSupportedLoadingModes.list():
            raise KeyError(
                f"Unknown load mode: {mode}. Change to"
                f" {MetaGenSupportedLoadingModes.full} for Excel files"
                " loading."
            )
        if mode == MetaGenSupportedLoadingModes.lazy:
            raise NotImplementedError(
                "Lazy mode is not implemented for Excel files, use loading"
                f" mode: {MetaGenSupportedLoadingModes.full}."
            )
        elif mode == MetaGenSupportedLoadingModes.full:
            return pl.read_excel(
                source=self.input_file, **self.polars_read_excel_options
            ).lazy()

    def load_parquet_data(
        self, mode: MetaGenSupportedLoadingModes
    ) -> pl.LazyFrame | pl.DataFrame:
        if mode not in MetaGenSupportedLoadingModes.list():
            raise KeyError(
                f"Unknown read mode: {mode}. Change to"
                f" {MetaGenSupportedLoadingModes.full} for Excel files"
                " loading."
            )
        if mode == MetaGenSupportedLoadingModes.lazy:
            return pl.scan_parquet(source=self.input_file)
        elif mode == MetaGenSupportedLoadingModes.full:
            return pl.read_parquet(source=self.input_file).lazy()
