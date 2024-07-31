"""
PyMetaGen
=========

Python Metadata Generator
"""

from __future__ import annotations

import json
import subprocess
from collections.abc import Sequence
from functools import cached_property
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd
import polars as pl

from pymetagen._typing import (
    Any,
    DataFrameT,
    Hashable,
    OptionalAnyValueDict,
    OptionalPandasDataFrame,
)
from pymetagen.dataloader import DataLoader, LazyDataLoader
from pymetagen.datatypes import (
    MetaGenDataType,
    MetaGenSupportedFileExtension,
    MetaGenSupportedLoadingMode,
    dtype_to_metagen_type,
)
from pymetagen.exceptions import (
    FileTypeUnsupportedError,
    LoadingModeUnsupportedError,
)
from pymetagen.utils import (
    CustomDecoder,
    CustomEncoder,
    InspectionMode,
    collect,
    extract_data,
)


class MetaGen:
    """
    Generate metadata for a Polars DataFrame.

    Args:
        data: Polars DataFrame to generate metadata for.
        descriptions: A dictionary of column names, each containing a
            dictionary with keys "description" and "long_name". e.g.
                "column_name": {
                    "description": "A description of the column",
                    "long_name": "A long name for the column",
                }
        compute_metadata: Flag for computing metadata on instantiation
    """

    def __init__(
        self,
        data: DataFrameT,
        descriptions: dict[Hashable, dict[str, Any]] | None = None,
        compute_metadata: bool = False,
    ):
        self.data = data
        self.descriptions = descriptions or {}
        if compute_metadata:
            self.pandas_metadata = self._metadata

    @classmethod
    def from_path(
        cls,
        path: Path,
        mode: MetaGenSupportedLoadingMode = MetaGenSupportedLoadingMode.LAZY,
        descriptions_path: Path | None = None,
        compute_metadata: bool = False,
    ) -> MetaGen:
        """
        Generate metadata from a file.

        Args:
            path: Path to the file to generate metadata from.
            descriptions_path: Path to a JSON or CSV file containing descriptions.

                In a JSON file the 'description' key will be read
                and should contain a key for each column in the data. Each column key should contain a dictionary
                with keys 'description' and 'long_name'. e.g.
                    {
                        "descriptions": {
                            "column_1": {
                                "description": "A description of the column",
                                "long_name": "A long name for the column",
                            },
                            "column_2": {
                                "description": "A description of the column",
                                "long_name": "A long name for the column",
                            },
                        }
                    }

                In a CSV file, their should be three columns: 'column_name', 'description', and 'long_name'. e.g.
                    column_name,description,long_name
                    column_1,A description of the column,A long name for the column
                    column_2,A description of the column,A long name for the column

            mode: Loading mode to use. See :class:`pymetagen.datatypes.MetaGenSupportedLoadingModes` for supported
                modes.
            compute_metadata: Flag for computing metadata on instantiation.
        """
        mode_mapping = {
            MetaGenSupportedLoadingMode.LAZY: LazyDataLoader,
            MetaGenSupportedLoadingMode.EAGER: DataLoader,
        }
        try:
            loader_class = mode_mapping[mode]
        except KeyError:
            raise LoadingModeUnsupportedError(
                f"Mode {mode} is not supported. Supported modes are: "
                f"{MetaGenSupportedLoadingMode.values()}"
            )
        data = loader_class(path)()

        if descriptions_path is not None:
            func_map: dict[
                str, Callable[[Path], dict[Hashable, dict[str, Any]]]
            ] = {
                MetaGenSupportedFileExtension.JSON.value: cls._load_descriptions_from_json,
                MetaGenSupportedFileExtension.CSV.value: cls._load_descriptions_from_csv,
            }

            descriptions = func_map[descriptions_path.suffix](
                descriptions_path
            )
        else:
            descriptions = None

        return cls(
            data=data,
            descriptions=descriptions,
            compute_metadata=compute_metadata,
        )

    @cached_property
    def _metadata(self):
        return self.compute_metadata().reset_index()

    @property
    def _polars_metadata(self):
        return pl.from_pandas(self._metadata)

    @staticmethod
    def _load_descriptions_from_json(
        path: Path,
    ) -> dict[Hashable, dict[str, Any]]:
        return json.loads(path.read_text(), cls=CustomDecoder)["descriptions"]

    @staticmethod
    def _load_descriptions_from_csv(
        path: Path,
    ) -> dict[Hashable, dict[str, Any]]:
        return (
            pd.read_csv(path).set_index("column_name").to_dict(orient="index")
        )

    def compute_metadata(self) -> pd.DataFrame:
        columns_to_drop = [
            "25%",
            "50%",
            "75%",
        ]
        pymetagen_columns = [
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

        assert_msg = (
            "Internal error: while calculating '{}' metadata."
            " Number of columns in metadata table does not match number of"
            " columns in data."
        )

        metadata: dict[Hashable, dict[Hashable, Any]] = {}
        schema = self.data.collect_schema()
        length_of_columns = schema.len()

        simple_metadata = self._get_simple_metadata(
            columns_to_drop=columns_to_drop
        )
        for column, data in simple_metadata.items():
            assert len(data) == length_of_columns, assert_msg.format(column)
        metadata.update(simple_metadata)

        number_of_null_and_zeros = self._number_of_null_and_zeros(
            metadata["Type"]
        )
        assert (
            len(number_of_null_and_zeros) == length_of_columns
        ), assert_msg.format("null and zeros")
        metadata["# empty/zero"] = number_of_null_and_zeros

        number_of_positive_values = self._number_of_positive_values(
            metadata["Type"]
        )
        assert (
            len(number_of_positive_values) == length_of_columns
        ), assert_msg.format("positive values")
        metadata["# positive"] = number_of_positive_values

        number_of_negative_values = self._number_of_negative_values(
            metadata["Type"]
        )
        assert (
            len(number_of_negative_values) == length_of_columns
        ), assert_msg.format("negative values")
        metadata["# negative"] = number_of_negative_values

        minimal_string_length = self._minimal_string_length(metadata["Type"])
        assert (
            len(minimal_string_length) == length_of_columns
        ), assert_msg.format("minimal string length")
        metadata["Min Length"] = minimal_string_length

        maximal_string_length = self._maximal_string_length(metadata["Type"])
        assert (
            len(maximal_string_length) == length_of_columns
        ), assert_msg.format("maximal string length")
        metadata["Max Length"] = maximal_string_length

        number_of_unique_counts = self._number_of_unique_counts()
        assert (
            len(number_of_unique_counts) == length_of_columns
        ), assert_msg.format("number of unique counts")
        metadata["# unique"] = number_of_unique_counts

        number_of_unique_values = self._number_of_unique_values()
        assert (
            len(number_of_unique_values) == length_of_columns
        ), assert_msg.format("number of unique values")
        metadata["Values"] = number_of_unique_values

        metadata["Description"] = {}
        metadata["Long Name"] = {}
        for column in schema.names():
            description_data: dict[str, Any] = self.descriptions.get(
                column, {}
            )
            metadata["Description"][column] = description_data.get(
                "description", ""
            )
            metadata["Long Name"][column] = description_data.get(
                "long_name", ""
            )

        full_metadata = pd.DataFrame(metadata).replace(np.nan, None)
        full_metadata.index.name = "Name"
        return full_metadata[pymetagen_columns]

    def metadata_by_output_format(
        self,
    ) -> dict[str, pd.DataFrame | dict[Hashable, Any]]:
        metadata = self.compute_metadata()
        return {
            MetaGenSupportedFileExtension.PARQUET.value: metadata,
            MetaGenSupportedFileExtension.CSV.value: metadata.reset_index(),
            MetaGenSupportedFileExtension.XLSX.value: metadata.reset_index(),
            MetaGenSupportedFileExtension.JSON.value: metadata.to_dict(
                orient="index"
            ),
        }

    def _get_simple_metadata(
        self, columns_to_drop: list[str] | None = None
    ) -> dict[Hashable, Any]:
        columns_to_drop = columns_to_drop or []
        table = (
            self.data.with_columns(pl.col(pl.Categorical).cast(pl.Utf8))
            .pipe(collect)
            .describe()
            .to_pandas()
            .convert_dtypes()
        )
        description_col = table.columns[0]
        metadata_table = (
            table.rename(columns={description_col: "Name"})
            .set_index("Name")
            .T.drop(columns=columns_to_drop)
            .rename(
                columns={
                    "null_count": "# nulls",
                    "min": "Min",
                    "max": "Max",
                    "mean": "Mean",
                    "std": "Std",
                }
            )
            .astype({"# nulls": int, "Min": str, "Max": str})
            .to_dict()
        )

        types_: dict[Hashable, str] = {}
        for col, type_ in zip(
            self.data.collect_schema().names(), self.data.dtypes
        ):
            types_[col] = dtype_to_metagen_type(type_)
        metadata_table["Type"] = types_

        return metadata_table

    def _number_of_null_and_zeros(
        self, types: dict[Hashable, MetaGenDataType]
    ) -> dict[Hashable, int]:
        nulls: dict[Hashable, int] = {}
        for col in self.data.collect_schema().names():
            data = self.data.pipe(collect).select(col)
            null_count = data.null_count().row(0)[0]
            zero_count = (
                data.filter(pl.col(col) == 0).shape[0]
                if types[col] in MetaGenDataType.numeric_data_types()
                else 0
            )
            nulls[col] = zero_count + null_count
        return nulls

    def _number_of_positive_values(
        self, types: dict[Hashable, MetaGenDataType]
    ) -> dict[Hashable, int | None]:
        pos: dict[Hashable, int | None] = {}
        for col in self.data.collect_schema().names():
            pos_count = (
                self.data.filter(pl.col(col) > 0).pipe(collect).shape[0]
                if types[col] in MetaGenDataType.numeric_data_types()
                else None
            )
            pos[col] = pos_count
        return pos

    def _number_of_negative_values(
        self, types: dict[Hashable, MetaGenDataType]
    ) -> dict[Hashable, int | None]:
        neg: dict[Hashable, int | None] = {}
        for col in self.data.collect_schema().names():
            neg_count = (
                self.data.filter(pl.col(col) < 0).pipe(collect).shape[0]
                if types[col] in MetaGenDataType.numeric_data_types()
                else None
            )
            neg[col] = neg_count
        return neg

    def _minimal_string_length(
        self, types: dict[Hashable, MetaGenDataType]
    ) -> dict[Hashable, int | None]:
        min_str_length: dict[Hashable, int | None] = {}
        for col in self.data.collect_schema().names():
            if types[col] in MetaGenDataType.categorical_data_types():
                min_str_length[col] = (
                    self.data.with_columns(
                        pl.col(col)
                        .cast(pl.Utf8)
                        .str.len_bytes()
                        .alias(f"{col}_len")
                    )
                    .select(f"{col}_len")
                    .min()
                    .pipe(collect)
                    .row(0)[0]
                )
            else:
                min_str_length[col] = None
        return min_str_length

    def _maximal_string_length(
        self, types: dict[Hashable, MetaGenDataType]
    ) -> dict[Hashable, int | None]:
        max_str_length: dict[Hashable, int | None] = {}
        for col in self.data.collect_schema().names():
            if types[col] in MetaGenDataType.categorical_data_types():
                max_str_length[col] = (
                    self.data.with_columns(
                        pl.col(col)
                        .cast(pl.Utf8)
                        .str.len_bytes()
                        .alias(f"{col}_len")
                    )
                    .select(f"{col}_len")
                    .max()
                    .pipe(collect)
                    .row(0)[0]
                )
            else:
                max_str_length[col] = None
        return max_str_length

    def _is_column_all_null(self, col: str) -> bool:
        """
        Returns True if all values in the column are null.
        """
        df = self.data.select(col).pipe(collect)
        return df.null_count().row(0)[0] == len(df)

    def _number_of_unique_counts(self) -> dict[Hashable, int]:
        unique_counts: dict[Hashable, int] = {}
        for col in self.data.collect_schema().names():
            if not self._is_column_all_null(col):
                unique_counts[col] = (
                    self.data.select(col).pipe(collect).n_unique()
                )
            else:
                unique_counts[col] = 1

        return unique_counts

    def _number_of_unique_values(
        self, max_number_of_unique_to_show: int = 10
    ) -> dict[Hashable, list[Any] | list[None] | None]:
        unique_values: dict[Hashable, list[Any] | list[None] | None] = {}
        for col in self.data.collect_schema().names():
            if not self._is_column_all_null(col):
                values = (
                    self.data.select(col).pipe(collect).unique()[col].to_list()
                )
                try:
                    values.sort(
                        key=lambda e: (e is None, e)
                    )  # allow None to be sorted
                except Exception:
                    # if we couldn't sort, just return the values as is
                    pass
                unique_values[col] = values
            else:
                unique_values[col] = [None]

        unique_values = {
            col: (
                _list
                if _list is not None
                and len(_list) < max_number_of_unique_to_show
                else None
            )
            for col, _list in unique_values.items()
        }
        return unique_values

    def write_metadata(
        self,
        outpath: str | Path,
        metadata: OptionalAnyValueDict | OptionalPandasDataFrame = None,
    ) -> None:
        """
        Write metadata to a file.

        Args:
            outpath: Path to write metadata to. File extension determines
                output format. Supported file extensions can be found in
                :class:`pymetagen.datatypes.MetaGenSupportedFileExtensions`.
            metadata: Metadata to write. If a DataFrame is provided, it will be
                written as is. If a dictionary is provided, it will be written
                as a JSON file.
        """
        outpath = Path(outpath)

        output_type_mapping: dict[
            str,
            Callable[[Path, OptionalAnyValueDict], None]
            | Callable[[Path, OptionalPandasDataFrame], None],
        ] = {
            ".csv": self._write_csv_metadata,
            ".xlsx": self._write_excel_metadata,
            ".json": self._write_json_metadata,
            ".parquet": self._write_parquet_metadata,
        }

        try:
            write_metadata = output_type_mapping[outpath.suffix]
        except KeyError:
            raise FileTypeUnsupportedError(
                f"File type {outpath.suffix} not yet implemented. Only"
                " supported file extensions:"
                f" {MetaGenSupportedFileExtension.values()}"
            )

        write_metadata(outpath, metadata)  # type: ignore[arg-type]

    def _write_excel_metadata(
        self, output_path: Path, metadata: OptionalPandasDataFrame
    ) -> None:
        metadata = metadata if metadata is not None else self._metadata
        metadata.to_excel(output_path, sheet_name="Fields", index=False)

    def _write_csv_metadata(
        self, output_path: Path, metadata: OptionalPandasDataFrame
    ) -> None:
        metadata = metadata if metadata is not None else self._metadata
        metadata.to_csv(output_path, index=False)

    def _write_json_metadata(
        self, output_path: Path, metadata: OptionalAnyValueDict
    ) -> None:
        if metadata is not None:
            metadata_dict = metadata
        else:
            _metadata = self.compute_metadata()
            metadata_dict = _metadata.to_dict(orient="index")

        json_to_dump: dict[str, dict[Hashable, Any]] = {
            "fields": metadata_dict
        }
        with open(output_path, "w") as f:
            json.dump(
                json_to_dump,
                f,
                indent=4,
                ensure_ascii=False,
                cls=CustomEncoder,
            )

    def write_extracts(
        self,
        output_path: Path,
        random_seed: int | None = None,
        number_rows: int = 10,
        with_replacement: bool = False,
        inspection_modes: Sequence[InspectionMode] | None = None,
        formats_to_write: set[MetaGenSupportedFileExtension] | None = None,
        mode: MetaGenSupportedLoadingMode = MetaGenSupportedLoadingMode.LAZY,
    ) -> None:

        inspection_modes = inspection_modes or InspectionMode.list()
        formats_to_write = formats_to_write or {
            MetaGenSupportedFileExtension(output_path.suffix)
        }
        for inspection_mode in inspection_modes:
            for output_format in formats_to_write:
                path = output_path.with_suffix(output_format)
                path = path.with_name(
                    f"{path.stem}-{inspection_mode.value}{path.suffix}"
                )
                self.write_extract_by_inspection_mode(
                    output_path=path,
                    mode=mode,
                    inspection_mode=inspection_mode,
                    random_seed=random_seed,
                    number_rows=number_rows,
                    with_replacement=with_replacement,
                )

    def write_extract_by_inspection_mode(
        self,
        output_path: Path,
        mode: MetaGenSupportedLoadingMode,
        inspection_mode: InspectionMode,
        random_seed: int | None,
        number_rows: int,
        with_replacement: bool,
    ) -> None:
        data = self.extract_data(
            mode=mode,
            tbl_rows=number_rows,
            inspection_mode=inspection_mode,
            random_seed=random_seed,
            with_replacement=with_replacement,
        )
        self.write_data(outpath=output_path, data=data)

    def _write_parquet_metadata(
        self, output_path: Path, metadata: OptionalPandasDataFrame
    ) -> None:
        metadata = metadata if metadata is not None else self._metadata
        metadata.to_parquet(output_path)

    def inspect_data(
        self,
        data: DataFrameT | None = None,
        tbl_rows: int = 10,
        tbl_cols: int | None = None,
        fmt_str_lengths: int = 50,
    ) -> None:
        """
        Inspect the data.
        """
        data_to_look = self.data if data is None else data
        tbl_cols = tbl_cols or self.data.collect_schema().len()
        with pl.Config(
            fmt_str_lengths=fmt_str_lengths,
            tbl_cols=tbl_cols,
            tbl_rows=tbl_rows,
        ):
            return data_to_look.pipe(print)

    def extract_data(
        self,
        mode: MetaGenSupportedLoadingMode,
        inspection_mode: InspectionMode,
        tbl_rows: int = 10,
        random_seed: int | None = None,
        with_replacement: bool = False,
        inplace: bool = False,
    ) -> pl.DataFrame:
        """
        Extract data from a file.
        """
        data = extract_data(
            df=self.data,
            mode=mode,
            tbl_rows=tbl_rows,
            inspection_mode=inspection_mode,
            random_seed=random_seed,
            with_replacement=with_replacement,
        )
        if inplace:
            self.data = data
        return data

    def quick_look_preview(
        self,
        outpath: Path,
    ) -> None:
        """
        Preview a data.
        """
        (
            subprocess.run(
                ["qlmanage", "-p", outpath],
                stdout=subprocess.PIPE,
            )
        )

    def _filter_by_sql_query(
        self, sql_query: str, eager: bool = True, table_name: str = "data"
    ) -> DataFrameT:
        """
        Filter data by a SQL query.

        Args:
            sql_query: SQL query to filter data by.
            eager: If True, the data will be loaded into memory before
                filtering. If False, the data will be filtered lazily.
        """
        sql = pl.SQLContext()
        sql.register(table_name, self.data)
        return sql.execute(query=sql_query, eager=eager)  # type: ignore

    def filter_data(
        self, table_name: str, sql_query: Path | str, eager: bool = True
    ):
        """
        Filter the data attribute by a SQL query.

        Args:
            table_name: Name of the table to filter.
            sql_query: SQL query to filter data by. If a Path is provided, the
                       file will be read and the contents will be used as the SQL
            eager: If True, the data will be loaded into memory before
                filtering. If False, the data will be filtered lazily.
        """
        sql_query = Path(sql_query)
        if sql_query.is_file():
            sql_query = sql_query.read_text()
        else:
            sql_query = str(sql_query)
        self.data = self._filter_by_sql_query(
            sql_query, eager=eager, table_name=table_name
        )

    def write_data(
        self, outpath: str | Path, data: DataFrameT | None = None
    ) -> None:
        outpath = Path(outpath)

        output_type_mapping = {
            ".csv": self._write_csv_data,
            ".xlsx": self._write_excel_data,
            ".json": self._write_json_data,
            ".parquet": self._write_parquet_data,
        }

        try:
            write_data = output_type_mapping[outpath.suffix]
        except KeyError:
            raise FileTypeUnsupportedError(
                f"File type {outpath.suffix} not yet implemented. Only"
                " supported file extensions:"
                f" {MetaGenSupportedFileExtension.values()}"
            )
        write_data(outpath, data)

    def _write_csv_data(
        self, output_path: Path | str, data: DataFrameT | None
    ) -> None:
        data = data if data is not None else self.data
        data.pipe(collect).write_csv(output_path)

    def _write_excel_data(
        self, output_path: Path | str, data: DataFrameT | None
    ) -> None:
        data = data if data is not None else self.data
        data.pipe(collect).write_excel(output_path)

    def _write_json_data(
        self, output_path: Path | str, data: DataFrameT | None
    ) -> None:
        data = data if data is not None else self.data
        data.pipe(collect).to_pandas().to_json(
            output_path,
            orient="records",
            indent=4,
            force_ascii=False,
            date_format="iso",
            date_unit="s",
        )

    def _write_parquet_data(
        self, output_path: Path | str, data: DataFrameT | None
    ) -> None:
        data = data if data is not None else self.data
        data.pipe(collect).write_parquet(output_path)


def json_metadata_to_pandas(path: Path | str) -> pd.DataFrame:
    with open(path) as f:
        metadata = json.load(f)
    metadata = metadata["fields"]
    return pd.DataFrame.from_dict(metadata, orient="index")
