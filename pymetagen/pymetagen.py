"""
PyMetaGen
=========

Python Metadata Generator
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
    dtype_to_metagentype,
)
from pymetagen.exceptions import (
    FileTypeUnsupportedError,
    LoadingModeUnsupportedError,
)
from pymetagen.typing import DataFrameT


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
        descriptions: dict[str, str] | None = None,
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
            descriptions_path: Path to a JSON file containing descriptions. The 'description' key will be read
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
            descriptions = json.loads(descriptions_path.read_text())[
                "descriptions"
            ]
        else:
            descriptions = None

        return cls(
            data=data,
            descriptions=descriptions,
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
            data = self.data.select(col)
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
    ) -> dict[str, int]:
        pos = {}
        for col in self.data.columns:
            pos_count = (
                self.data.filter(pl.col(col) > 0).shape[0]
                if types[col] in MetaGenDataType.numeric_data_types
                else None
            )
            pos[col] = pos_count
        return pos

    def _number_of_negative_values(
        self, types: dict[str, MetaGenDataType]
    ) -> dict[str, int]:
        neg = {}
        for col in self.data.columns:
            neg_count = (
                self.data.filter(pl.col(col) < 0).shape[0]
                if types[col] in MetaGenDataType.numeric_data_types
                else None
            )
            neg[col] = neg_count
        return neg

    def _minimal_string_length(
        self, types: dict[str, MetaGenDataType]
    ) -> dict[str, int]:
        min_str_length = {}
        for col in self.data.columns:
            if types[col] in MetaGenDataType.categorical_data_types:
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

    def _maximal_string_length(
        self, types: dict[str, MetaGenDataType]
    ) -> dict[str, int]:
        max_str_length = {}
        for col in self.data.columns:
            if types[col] in MetaGenDataType.categorical_data_types:
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
        metadata = self.compute_metadata().to_dict(orient="index")
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
    return pd.DataFrame.from_dict(metadata, orient="index")
