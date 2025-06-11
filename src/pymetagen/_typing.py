from __future__ import annotations

import typing

import pandas as pd
import polars as pl
from polars.datatypes import DataType, DataTypeClass

DataFrameT = typing.Union[pl.DataFrame, pl.LazyFrame]
Any = typing.Any
Hashable = typing.Hashable

OptionalPandasDataFrame: typing.TypeAlias = typing.Optional[pd.DataFrame]
OptionalAnyValueDict: typing.TypeAlias = typing.Optional[dict[Hashable, Any]]

PolarsDataType: typing.TypeAlias = typing.Union["DataTypeClass", "DataType"]
SchemaDict: typing.TypeAlias = typing.Mapping[str, PolarsDataType]

ColumnName: typing.TypeAlias = str
ColumnDescription: typing.TypeAlias = str
ColumnLongName: typing.TypeAlias = str


class ColumnSimpleMetadata(typing.TypedDict):
    """Simple metadata for a column.
    Attributes:
    - description: A brief description of the column.
    - long_name: A more detailed or human-readable name for the column.
    """

    description: ColumnDescription
    long_name: ColumnLongName
