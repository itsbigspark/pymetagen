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

import json
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl

from pymetagen.dataloader import DataLoader, LazyDataLoader
from pymetagen.datatypes import (
    MetaGenDataType,
    MetaGenSupportedFileExtensions,
    MetaGenSupportedLoadingModes,
)
from pymetagen.exceptions import (
    FileTypeUnsupportedError,
    LoadingModeUnsupportedError,
)
from pymetagen.typing import DataFrameT


class MetaGen:
    def __init__(
        self,
        data: DataFrameT,
        create_regex: bool | None = None,
        descriptions: pl.LazyFrame | None = None,
    ):
        self.data = data
        self.create_regex = create_regex
        self.descriptions = descriptions

    @classmethod
    def from_path(
        cls,
        path: Path,
        create_regex: bool | None = None,
        descriptions: pl.LazyFrame | None = None,
        mode: MetaGenSupportedLoadingModes = MetaGenSupportedLoadingModes.EAGER,
    ) -> DataFrameT:
        mode_mapping = {
            MetaGenSupportedLoadingModes.LAZY: LazyDataLoader,
            MetaGenSupportedLoadingModes.EAGER: DataLoader,
        }
        try:
            LoaderClass = mode_mapping[mode]
        except KeyError:
            raise LoadingModeUnsupportedError(
                f"Mode {mode} is not supported. Supported modes are: "
                f"{MetaGenSupportedLoadingModes.list()}"
            )
        data = LoaderClass(path)()

        return cls(
            data=data,
            create_regex=create_regex,
            descriptions=descriptions,
        )

    def compute_metadata(self) -> pd.DataFrame:
        columns_to_drop = [
            "25%",
            "50%",
            "75%",
        ]
        pymetagen_columns = [
            "Type",
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

        metadata: dict[str, dict[str, Any]] = {}

        simple_metadata = self._get_simple_metadata(
            columns_to_drop=columns_to_drop
        )
        for column, data in simple_metadata.items():
            assert len(data) == len(self.data.columns), assert_msg.format(
                column
            )
        metadata.update(simple_metadata)

        number_of_null_and_zeros = self._number_of_null_and_zeros()
        assert len(number_of_null_and_zeros) == len(
            self.data.columns
        ), assert_msg.format("null and zeros")
        metadata["# empty/zero"] = number_of_null_and_zeros

        number_of_positive_values = self._number_of_positive_values()
        assert len(number_of_positive_values) == len(
            self.data.columns
        ), assert_msg.format("positive values")
        metadata["# positive"] = number_of_positive_values

        number_of_negative_values = self._number_of_negative_values()
        assert len(number_of_negative_values) == len(
            self.data.columns
        ), assert_msg.format("negative values")
        metadata["# negative"] = number_of_negative_values

        minimal_string_length = self._minimal_string_length()
        assert len(minimal_string_length) == len(
            self.data.columns
        ), assert_msg.format("minimal string length")
        metadata["Min Length"] = minimal_string_length

        maximal_string_length = self._maximal_string_length()
        assert len(maximal_string_length) == len(
            self.data.columns
        ), assert_msg.format("maximal string length")
        metadata["Max Length"] = maximal_string_length

        number_of_unique_counts = self._number_of_unique_counts()
        assert len(number_of_unique_counts) == len(
            self.data.columns
        ), assert_msg.format("number of unique counts")
        metadata["# unique"] = number_of_unique_counts

        number_of_unique_values = self._number_of_unique_values()
        assert len(number_of_unique_values) == len(
            self.data.columns
        ), assert_msg.format("number of unique values")
        metadata["Values"] = number_of_unique_values

        metadata: pd.DataFrame = pd.DataFrame(metadata)
        metadata.index.name = "Name"

        return metadata[pymetagen_columns]

    def _get_simple_metadata(
        self, columns_to_drop: list[str] = None
    ) -> dict[str, dict[str, Any]]:
        metadata_table = (
            self.data.describe()
            .to_pandas()
            .rename(columns={"describe": "Name"})
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
            .to_dict()
        )

        metadata_table["Type"] = dict(
            zip(
                self.data.columns,
                [
                    MetaGenDataType.polars_to_metagen_type(_type).value
                    for _type in self.data.dtypes
                ],
            )
        )
        return metadata_table

    def _number_of_null_and_zeros(self) -> dict[str, int]:
        nulls = {}
        for col in self.data.columns:
            column_dtype = self.data.select(col).dtypes.pop().__name__
            data = self.data.select(col)
            null_count = data.null_count().row(0)[0]
            zero_count = (
                data.filter(pl.col(col) == 0).shape[0]
                if column_dtype in MetaGenDataType.numeric_data_types
                else 0
            )
            nulls[col] = zero_count + null_count
        return nulls

    def _number_of_positive_values(self) -> dict[str, int]:
        pos = {}
        for col in self.data.columns:
            column_dtype = self.data.select(col).dtypes.pop().__name__
            pos_count = (
                self.data.filter(pl.col(col) > 0).shape[0]
                if column_dtype in MetaGenDataType.numeric_data_types
                else None
            )
            pos[col] = pos_count
        return pos

    def _number_of_negative_values(self) -> dict[str, int]:
        neg = {}
        for col in self.data.columns:
            column_dtype = self.data.select(col).dtypes.pop().__name__
            neg_count = (
                self.data.filter(pl.col(col) < 0).shape[0]
                if column_dtype in MetaGenDataType.numeric_data_types
                else None
            )
            neg[col] = neg_count
        return neg

    def _minimal_string_length(self) -> dict[str, int]:
        min_str_length = {}
        for col in self.data.columns:
            column_dtype = self.data.select(col).dtypes.pop().__name__
            if column_dtype in MetaGenDataType.categorical_data_types:
                min_str_length[col] = (
                    self.data.with_columns(
                        pl.col(col).str.lengths().alias(f"{col}_len")
                    )
                    .select(f"{col}_len")
                    .min()
                    .row(0)[0]
                )
            else:
                min_str_length[col] = None
        return min_str_length

    def _maximal_string_length(self) -> dict[str, int]:
        max_str_length = {}
        for col in self.data.columns:
            column_dtype = self.data.select(col).dtypes.pop().__name__
            if column_dtype in MetaGenDataType.categorical_data_types:
                max_str_length[col] = (
                    self.data.with_columns(
                        pl.col(col).str.lengths().alias(f"{col}_len")
                    )
                    .select(f"{col}_len")
                    .max()
                    .row(0)[0]
                )
            else:
                max_str_length[col] = None
        return max_str_length

    def _number_of_unique_counts(self) -> dict[str, int]:
        unique_counts = {}
        for col in self.data.columns:
            unique_counts[col] = self.data.select(col).n_unique()
        return unique_counts

    def _number_of_unique_values(
        self, max_number_of_unique_to_show: int = 7
    ) -> dict[str, int]:
        unique_values = {}
        for col in self.data.columns:
            unique_values[col] = self.data.select(col).unique()[col].to_list()

        unique_values = {
            col: _list if len(_list) < max_number_of_unique_to_show else None
            for col, _list in unique_values.items()
        }
        return unique_values

    def write_metadata(self, outpath: str | Path) -> None:
        outpath = Path(outpath)

        output_type_mapping = {
            ".csv": self._write_csv_metadata,
            ".xlsx": self._write_excel_metadata,
            ".json": self._write_json_metadata,
            ".parquet": self.write_parquet_metadata,
        }

        try:
            write_metadata = output_type_mapping[outpath.suffix]
        except KeyError:
            raise FileTypeUnsupportedError(
                f"File type {outpath.suffix} not yet implemented. Only"
                " supported file extensions:"
                f" {MetaGenSupportedFileExtensions.list()}"
            )
        write_metadata(outpath)

    def _write_excel_metadata(self, output_path: str) -> None:
        metadata = self.compute_metadata()
        metadata.to_excel(output_path)

    def _write_csv_metadata(self, output_path: str) -> None:
        metadata = self.compute_metadata()
        metadata.to_csv(output_path)

    def _write_json_metadata(self, output_path: str) -> None:
        metadata = self.compute_metadata().to_dict()
        json_to_dump = {"fields": metadata}
        with open(output_path, "w") as f:
            json.dump(json_to_dump, f, indent=4, ensure_ascii=False)

    def write_parquet_metadata(self, output_path: str) -> None:
        # NOTE: @vdiaz having problems due to type mixing in Min Max columns
        metadata = self.compute_metadata()
        metadata.to_parquet(output_path)


def json_metadata_to_pandas(path: Path) -> pd.DataFrame:
    with path.open() as f:
        metadata = json.load(f)
    metadata = metadata["fields"]
    return pd.DataFrame(metadata)
