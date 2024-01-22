"""
PyMetaGen
=========

Python Metadata Generator
"""

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import polars as pl

from pymetagen._typing import DataFrameT
from pymetagen.dataloader import DataLoader, LazyDataLoader
from pymetagen.datatypes import (
    MetaGenDataType,
    MetaGenSupportedFileExtensions,
    MetaGenSupportedLoadingModes,
    dtype_to_metagentype,
)
from pymetagen.exceptions import (
    FileTypeUnsupportedError,
    LoadingModeUnsupportedError,
)
from pymetagen.utils import InspectionMode, collect, extract_data


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
    """

    def __init__(
        self,
        data: DataFrameT,
        descriptions: dict[str, dict[str, str]] | None = None,
    ):
        self.data = data
        self.descriptions = descriptions or {}

    @classmethod
    def from_path(
        cls,
        path: Path,
        descriptions_path: Path | None = None,
        mode: MetaGenSupportedLoadingModes = MetaGenSupportedLoadingModes.EAGER,
    ) -> "MetaGen":
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
        """
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

        if descriptions_path is not None:
            func_map = {
                ".json": cls._load_descriptions_from_json,
                ".csv": cls._load_descriptions_from_csv,
            }
            descriptions = func_map[descriptions_path.suffix](
                descriptions_path
            )
        else:
            descriptions = None

        return cls(
            data=data,
            descriptions=descriptions,
        )

    @staticmethod
    def _load_descriptions_from_json(path: Path) -> dict[str, dict[str, str]]:
        return json.loads(path.read_text())["descriptions"]

    @staticmethod
    def _load_descriptions_from_csv(path: Path) -> dict[str, dict[str, str]]:
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

        metadata: dict[str, dict[str, Any]] = {}

        simple_metadata = self._get_simple_metadata(
            columns_to_drop=columns_to_drop
        )
        for column, data in simple_metadata.items():
            assert len(data) == len(self.data.columns), assert_msg.format(
                column
            )
        metadata.update(simple_metadata)

        number_of_null_and_zeros = self._number_of_null_and_zeros(
            metadata["Type"]
        )
        assert len(number_of_null_and_zeros) == len(
            self.data.columns
        ), assert_msg.format("null and zeros")
        metadata["# empty/zero"] = number_of_null_and_zeros

        number_of_positive_values = self._number_of_positive_values(
            metadata["Type"]
        )
        assert len(number_of_positive_values) == len(
            self.data.columns
        ), assert_msg.format("positive values")
        metadata["# positive"] = number_of_positive_values

        number_of_negative_values = self._number_of_negative_values(
            metadata["Type"]
        )
        assert len(number_of_negative_values) == len(
            self.data.columns
        ), assert_msg.format("negative values")
        metadata["# negative"] = number_of_negative_values

        minimal_string_length = self._minimal_string_length(metadata["Type"])
        assert len(minimal_string_length) == len(
            self.data.columns
        ), assert_msg.format("minimal string length")
        metadata["Min Length"] = minimal_string_length

        maximal_string_length = self._maximal_string_length(metadata["Type"])
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

        metadata["Description"] = {}
        metadata["Long Name"] = {}
        for column in self.data.columns:
            description_data = self.descriptions.get(column, {})
            metadata["Description"][column] = description_data.get(
                "description", ""
            )
            metadata["Long Name"][column] = description_data.get(
                "long_name", ""
            )

        metadata: pd.DataFrame = pd.DataFrame(metadata).replace(np.nan, None)
        metadata.index.name = "Name"

        return metadata[pymetagen_columns]

    def _get_simple_metadata(
        self, columns_to_drop: list[str] | None = None
    ) -> dict[str, dict[str, Any]]:
        columns_to_drop = columns_to_drop or []
        metadata_table = (
            self.data.with_columns(pl.col(pl.Categorical).cast(pl.Utf8))
            .pipe(collect)
            .describe()
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
            .astype({"# nulls": int})
            .to_dict()
        )

        types_ = {}
        for col, type_ in zip(self.data.columns, self.data.dtypes):
            types_[col] = dtype_to_metagentype(type_)
        metadata_table["Type"] = types_

        return metadata_table

    def _number_of_null_and_zeros(
        self, types: dict[str, MetaGenDataType]
    ) -> dict[str, int]:
        nulls = {}
        for col in self.data.columns:
            data = self.data.pipe(collect).select(col)
            null_count = data.null_count().row(0)[0]
            zero_count = (
                data.filter(pl.col(col) == 0).shape[0]
                if types[col] in MetaGenDataType.numeric_data_types
                else 0
            )
            nulls[col] = zero_count + null_count
        return nulls

    def _number_of_positive_values(
        self, types: dict[str, MetaGenDataType]
    ) -> dict[str, int | None]:
        pos = {}
        for col in self.data.columns:
            pos_count = (
                self.data.filter(pl.col(col) > 0).pipe(collect).shape[0]
                if types[col] in MetaGenDataType.numeric_data_types
                else None
            )
            pos[col] = pos_count
        return pos

    def _number_of_negative_values(
        self, types: dict[str, MetaGenDataType]
    ) -> dict[str, int | None]:
        neg = {}
        for col in self.data.columns:
            neg_count = (
                self.data.filter(pl.col(col) < 0).pipe(collect).shape[0]
                if types[col] in MetaGenDataType.numeric_data_types
                else None
            )
            neg[col] = neg_count
        return neg

    def _minimal_string_length(
        self, types: dict[str, MetaGenDataType]
    ) -> dict[str, int | None]:
        min_str_length = {}
        for col in self.data.columns:
            if types[col] in MetaGenDataType.categorical_data_types:
                min_str_length[col] = (
                    self.data.with_columns(
                        pl.col(col)
                        .cast(pl.Utf8)
                        .str.lengths()
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
        self, types: dict[str, MetaGenDataType]
    ) -> dict[str, int | None]:
        max_str_length = {}
        for col in self.data.columns:
            if types[col] in MetaGenDataType.categorical_data_types:
                max_str_length[col] = (
                    self.data.with_columns(
                        pl.col(col)
                        .cast(pl.Utf8)
                        .str.lengths()
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

    def _number_of_unique_counts(self) -> dict[str, int]:
        unique_counts = {}
        for col in self.data.columns:
            if not self._is_column_all_null(col):
                unique_counts[col] = (
                    self.data.select(col).pipe(collect).n_unique()
                )
            else:
                unique_counts[col] = 1

        return unique_counts

    def _number_of_unique_values(
        self, max_number_of_unique_to_show: int = 10
    ) -> dict[str, list[Any] | list[None]]:
        unique_values = {}
        for col in self.data.columns:
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
            col: _list if len(_list) < max_number_of_unique_to_show else None
            for col, _list in unique_values.items()
        }
        return unique_values

    def write_metadata(self, outpath: str | Path) -> None:
        """
        Write metadata to a file.

        Args:
            outpath: Path to write metadata to. File extension determines
                output format. Supported file extensions can be found in
                :class:`pymetagen.datatypes.MetaGenSupportedFileExtensions`.
        """
        outpath = Path(outpath)

        output_type_mapping = {
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
                f" {MetaGenSupportedFileExtensions.list()}"
            )
        write_metadata(outpath)

    def _write_excel_metadata(self, output_path: str) -> None:
        metadata = self.compute_metadata().reset_index()
        metadata.to_excel(output_path, sheet_name="Fields", index=False)

    def _write_csv_metadata(self, output_path: str) -> None:
        metadata = self.compute_metadata().reset_index()
        metadata.to_csv(output_path, index=False)

    def _write_json_metadata(self, output_path: str) -> None:
        metadata = self.compute_metadata().to_dict(orient="index")
        json_to_dump = {"fields": metadata}
        with open(output_path, "w") as f:
            json.dump(json_to_dump, f, indent=4, ensure_ascii=False)

    def _write_parquet_metadata(self, output_path: str) -> None:
        # NOTE: @vdiaz having problems due to type mixing in Min Max columns
        metadata = self.compute_metadata()
        metadata.to_parquet(output_path)

    def inspect_data(
        self,
        tbl_rows: int = 10,
        tbl_cols: int | None = None,
        fmt_str_lengths: int = 50,
    ) -> None:
        """
        Inspect the data.
        """
        tbl_cols = tbl_cols or len(self.data.columns)
        with pl.Config(
            fmt_str_lengths=fmt_str_lengths,
            tbl_cols=tbl_cols,
            tbl_rows=tbl_rows,
        ):
            return self.data.pipe(print)

    def extract_data(
        self,
        mode: MetaGenSupportedLoadingModes,
        tbl_rows: int = 10,
        inspection_mode: InspectionMode = InspectionMode.head,
        random_seed: int | None = None,
        with_replacement: bool = False,
        inplace: bool = False,
    ) -> DataFrameT:
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

    def write_data(self, outpath: str | Path) -> None:
        outpath = Path(outpath)

        output_type_mapping = {
            ".csv": self._write_csv_data,
            ".xlsx": self._write_excel_data,
            ".json": self._write_json_data,
            ".parquet": self._write_parquet_data,
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

    def _write_csv_data(self, output_path: str) -> None:
        self.data.pipe(collect).write_csv(output_path)

    def _write_excel_data(self, output_path: str) -> None:
        self.data.pipe(collect).write_excel(output_path, index=False)

    def _write_json_data(self, output_path: str) -> None:
        self.data.pipe(collect).to_pandas().to_json(
            output_path,
            orient="records",
            indent=4,
            force_ascii=False,
            date_format="iso",
            date_unit="s",
        )

    def _write_parquet_data(self, output_path: str) -> None:
        self.data.pipe(collect).write_parquet(output_path)


def json_metadata_to_pandas(path: Path) -> pd.DataFrame:
    with path.open() as f:
        metadata = json.load(f)
    metadata = metadata["fields"]
    return pd.DataFrame.from_dict(metadata, orient="index")
