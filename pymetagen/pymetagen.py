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

import pandas as pd
import polars as pl

from pymetagen.dataloader import DataLoader, LazyDataLoader
from pymetagen.datatypes import (
    MetaGenDataType,
    MetaGenSupportedFileExtensions,
    MetaGenSupportedLoadingModes,
)
from pymetagen.exceptions import LoadingModeUnsupportedError
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
        mode: MetaGenSupportedLoadingModes = MetaGenSupportedLoadingModes.LAZY,
    ) -> DataFrameT:
        mode_mapping = {
            MetaGenSupportedLoadingModes.LAZY: LazyDataLoader,
            MetaGenSupportedLoadingModes.FULL: DataLoader,
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
        metadata_table = self.get_simple_metadata(
            columns_to_drop=columns_to_drop
        )
        assert len(metadata_table) == len(self.data.columns)
        number_of_null_and_zeros = self.number_of_null_and_zeros()
        assert len(number_of_null_and_zeros) == len(self.data.columns)
        number_of_positive_values = self.number_of_positive_values()
        assert len(number_of_positive_values) == len(self.data.columns)
        number_of_negative_values = self.number_of_negative_values()
        assert len(number_of_negative_values) == len(self.data.columns)
        minimal_string_length = self.minimal_string_length()
        assert len(minimal_string_length) == len(self.data.columns)
        maximal_string_length = self.maximal_string_length()
        assert len(maximal_string_length) == len(self.data.columns)
        number_of_unique_counts = self.number_of_unique_counts()
        assert len(number_of_unique_counts) == len(self.data.columns)
        number_of_unique_values = self.number_of_unique_values()
        assert len(number_of_unique_values) == len(self.data.columns)

        metadata = (
            metadata_table.merge(
                number_of_null_and_zeros,
                on="Name",
                how="left",
            )
            .merge(
                number_of_positive_values,
                on="Name",
                how="left",
            )
            .merge(
                number_of_negative_values,
                on="Name",
                how="left",
            )
            .merge(
                minimal_string_length,
                on="Name",
                how="left",
            )
            .merge(
                maximal_string_length,
                on="Name",
                how="left",
            )
            .merge(
                number_of_unique_counts,
                on="Name",
                how="left",
            )
            .merge(
                number_of_unique_values,
                on="Name",
                how="left",
            )
        )
        metadata = metadata.set_index("Name")
        return metadata[pymetagen_columns]

    def get_simple_metadata(
        self, columns_to_drop: list[str] = None
    ) -> pd.DataFrame:
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
        )

        types = pd.DataFrame(
            list(
                zip(
                    self.data.columns,
                    [
                        MetaGenDataType.polars_to_metagen_type(_type).value
                        for _type in self.data.dtypes
                    ],
                )
            ),
            columns=["Name", "Type"],
        )
        metadata_table = (
            metadata_table.reset_index()
            .rename(columns={"index": "Name"})
            .merge(types, on="Name")
        )
        return metadata_table

    def number_of_null_and_zeros(self) -> pd.DataFrame:
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
        return (
            pd.DataFrame([nulls])
            .T.reset_index()
            .rename(columns={"index": "Name", 0: "# empty/zero"})
            .astype({"# empty/zero": "Int64"})
        )

    def number_of_positive_values(self) -> pd.DataFrame:
        pos = {}
        for col in self.data.columns:
            column_dtype = self.data.select(col).dtypes.pop().__name__
            pos_count = (
                self.data.filter(pl.col(col) > 0).shape[0]
                if column_dtype in MetaGenDataType.numeric_data_types
                else None
            )
            pos[col] = pos_count
        return (
            pd.DataFrame([pos])
            .T.reset_index()
            .rename(columns={"index": "Name", 0: "# positive"})
            .astype({"# positive": "Int64"})
        )

    def number_of_negative_values(self) -> pd.DataFrame:
        neg = {}
        for col in self.data.columns:
            column_dtype = self.data.select(col).dtypes.pop().__name__
            neg_count = (
                self.data.filter(pl.col(col) < 0).shape[0]
                if column_dtype in MetaGenDataType.numeric_data_types
                else None
            )
            neg[col] = neg_count
        return (
            pd.DataFrame([neg])
            .T.reset_index()
            .rename(columns={"index": "Name", 0: "# negative"})
            .astype({"# negative": "Int64"})
        )

    def minimal_string_length(self) -> pd.DataFrame:
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
        return (
            pd.DataFrame([min_str_length])
            .T.reset_index()
            .rename(columns={"index": "Name", 0: "Min Length"})
            .astype({"Min Length": "Int64"})
        )

    def maximal_string_length(self) -> pd.DataFrame:
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
        return (
            pd.DataFrame([max_str_length])
            .T.reset_index()
            .rename(columns={"index": "Name", 0: "Max Length"})
            .astype({"Max Length": "Int64"})
        )

    def number_of_unique_counts(self) -> pd.DataFrame:
        unique_counts = {}
        for col in self.data.columns:
            unique_counts[col] = self.data.select(col).n_unique()
        return (
            pd.DataFrame([unique_counts])
            .T.reset_index()
            .rename(columns={"index": "Name", 0: "# unique"})
            .astype({"# unique": "Int64"})
        )

    def number_of_unique_values(
        self, max_number_of_unique_to_show: int = 7
    ) -> pd.DataFrame:
        unique_values = {}
        for col in self.data.columns:
            unique_values[col] = self.data.select(col).unique()[col].to_list()

        unique_values = {
            col: _list if len(_list) < max_number_of_unique_to_show else None
            for col, _list in unique_values.items()
        }
        return (
            pd.DataFrame([unique_values])
            .T.reset_index()
            .rename(columns={"index": "Name", 0: "Values"})
        )

    def write_metadata(self, output_path: str | Path) -> None:
        output_path = Path(output_path)

        output_type_mapping = {
            ".csv": self._write_csv_metadata,
            ".xlsx": self._write_excel_metadata,
            ".json": self._write_json_metadata,
            ".parquet": self.write_parquet_metadata,
        }

        try:
            write_metadata = output_type_mapping[output_path.suffix]
        except KeyError:
            raise NotImplementedError(
                f"File type {output_path.suffix} not yet implemented. Only"
                " supported file extensions:"
                f" {MetaGenSupportedFileExtensions.list()}"
            )
        write_metadata(output_path)

    def _write_excel_metadata(self, output_path: str) -> None:
        metadata = self.compute_metadata()
        metadata.to_excel(output_path, index=False)

    def _write_csv_metadata(self, output_path: str) -> None:
        metadata = self.compute_metadata()
        metadata.to_csv(output_path, index=False)

    def _write_json_metadata(self, output_path: str) -> None:
        metadata = self.compute_metadata().set_index("Name").T.to_dict()
        json_to_dump = {"fields": metadata}
        with open(output_path, "w") as f:
            json.dump(json_to_dump, f, indent=4, ensure_ascii=False)

    def write_parquet_metadata(self, output_path: str) -> None:
        # NOTE: @vdiaz having problems due to type mixing in Min Max columns
        metadata = self.compute_metadata()
        metadata.to_parquet(output_path)
