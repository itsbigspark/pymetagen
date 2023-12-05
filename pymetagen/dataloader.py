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

import warnings
from pathlib import Path
from typing import Any

import polars as pl
from polars.datatypes.constants import N_INFER_DEFAULT
from polars.type_aliases import ParallelStrategy

from pymetagen.datatypes import MetaGenSupportedFileExtensions
from pymetagen.exceptions import FileTypeUnsupportedError
from pymetagen.utils import selectively_update_dict


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

POLARS_DEFAULT_LAZY_READ_CSV_OPTIONS: dict[str, Any] = (
    POLARS_DEFAULT_READ_CSV_OPTIONS.copy()
)
lazy_csv_unsupported_options = [
    "columns",
    "use_pyarrow",
    "n_threads",
    "batch_size",
    "storage_options",
    "sample_size",
]
for option in lazy_csv_unsupported_options:
    del POLARS_DEFAULT_LAZY_READ_CSV_OPTIONS[option]

LIST_OF_EXCEL_OPTIONS_FROM_CSV_OPTIONS = [
    # "storage_options",
    # "sample_size",
    # "quote_char",
    # "skip_rows",
    # "dtypes",
    # "null_values",
    # "missing_utf8_is_empty_string",
    # "ignore_errors",
    # "try_parse_dates",
    # "infer_schema_length",
    # "encoding",
    # "skip_rows_after_header",
    # "eol_char",
    # "raise_if_empty",
]
POLARS_DEFAULT_READ_EXCEL_OPTIONS: dict[str, Any] = {
    key: value
    for key, value in POLARS_DEFAULT_READ_CSV_OPTIONS.items()
    if key in LIST_OF_EXCEL_OPTIONS_FROM_CSV_OPTIONS
}
POLARS_DEFAULT_READ_EXCEL_OPTIONS["engine"] = "openpyxl"


POLARS_DEFAULT_READ_PARUET_OPTIONS = {}


class DataLoader:
    def __init__(
        self,
        path: Path,
        polars_read_csv_options: None | dict[str, Any] = None,
        sheet_name: str | None = None,
        _default_read_csv_options: dict[
            str, Any
        ] = POLARS_DEFAULT_READ_CSV_OPTIONS,
        _default_read_excel_options: dict[
            str, Any
        ] = POLARS_DEFAULT_READ_EXCEL_OPTIONS,
        _default_read_parquet_options: dict[
            str, Any
        ] = POLARS_DEFAULT_READ_PARUET_OPTIONS,
    ):
        self.path = path
        self.polars_read_csv_options = _default_read_csv_options.copy()
        self._update_read_csv_polars_options(polars_read_csv_options)
        self.polars_read_excel_options = _default_read_excel_options.copy()
        self._update_polars_read_excel_options(
            sheet_name,
        )
        self.polars_read_parquet_options = _default_read_parquet_options.copy()

    def __call__(self):
        return self.load()

    def load(
        self,
    ) -> pl.DataFrame:
        extension_mapping = {
            MetaGenSupportedFileExtensions.CSV: self._load_csv_data,
            MetaGenSupportedFileExtensions.XLSX: self._load_excel_data,
            MetaGenSupportedFileExtensions.PARQUET: self._load_parquet_data,
            MetaGenSupportedFileExtensions.JSON: self._load_json_data,
        }
        file_extension = self.path.suffixes[-1]
        try:
            return extension_mapping[file_extension]()
        except KeyError:
            raise FileTypeUnsupportedError(
                f"File extension {file_extension} is not supported"
            )

    def _update_polars_read_excel_options(
        self,
        sheet_name: str,
    ) -> dict[str, Any]:
        self.polars_read_excel_options["sheet_name"] = sheet_name
        return self.polars_read_excel_options

    def _update_read_csv_polars_options(
        self, polars_read_csv_options: dict[str, Any] | None
    ) -> dict[str, Any]:
        if polars_read_csv_options is None:
            return
        selectively_update_dict(
            self.polars_read_csv_options, polars_read_csv_options
        )

    def _load_csv_data(self) -> pl.DataFrame:
        return pl.read_csv(source=self.path, **self.polars_read_csv_options)

    def _load_excel_data(self) -> pl.DataFrame:
        return pl.read_excel(
            source=self.path, **self.polars_read_excel_options
        )

    def _load_parquet_data(self) -> pl.DataFrame:
        return pl.read_parquet(
            source=self.path, **self.polars_read_parquet_options
        )

    def _load_json_data(self):
        raise NotImplementedError


class LazyDataLoader(DataLoader):
    def __init__(
        self,
        path: Path,
        polars_read_csv_options: None | dict[str, Any] = None,
        sheet_name: str | None = None,
    ):
        super().__init__(
            path=path,
            polars_read_csv_options=polars_read_csv_options,
            sheet_name=sheet_name,
            _default_read_csv_options=POLARS_DEFAULT_LAZY_READ_CSV_OPTIONS,
        )

    def load(self) -> pl.LazyFrame:
        return super().load()

    def _load_csv_data(self) -> pl.LazyFrame:
        return pl.scan_csv(source=self.path, **self.polars_read_csv_options)

    def _load_excel_data(self) -> pl.DataFrame:
        warnings.warn(
            "Excel files are not supported in lazy mode, switching to full"
            " mode"
        )
        return super()._load_excel_data()

    def _load_parquet_data(self) -> pl.LazyFrame:
        return pl.scan_parquet(
            source=self.path, **self.polars_read_parquet_options
        )
