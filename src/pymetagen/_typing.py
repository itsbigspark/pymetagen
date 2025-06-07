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
