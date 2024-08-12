from __future__ import annotations

from collections.abc import Mapping, Sequence
from enum import Enum

import polars as pl

from pymetagen._typing import SchemaDict
from pymetagen.utils import EnumListMixin


class MetaGenMetadataColumn(EnumListMixin, str, Enum):
    """
    Columns in the metadata table.
    """

    NAME = "Name"
    LONG_NAME = "Long Name"
    TYPE = "Type"
    DESCRIPTION = "Description"
    MIN = "Min"
    MAX = "Max"
    MEAN = "Mean"
    STD = "Std"
    MIN_LENGTH = "Min Length"
    MAX_LENGTH = "Max Length"
    NUMBER_NULLS = "# nulls"
    NUMBER_EMPTY_ZERO = "# empty/zero"
    NUMBER_POSITIVE = "# positive"
    NUMBER_NEGATIVE = "# negative"
    NUMBER_UNIQUE = "# unique"
    VALUES = "Values"

    @classmethod
    def interger_dtypes(cls) -> SchemaDict:
        return {
            cls.MIN_LENGTH: pl.Int64,
            cls.MAX_LENGTH: pl.Int64,
            cls.NUMBER_NULLS: pl.Int64,
            cls.NUMBER_EMPTY_ZERO: pl.Int64,
            cls.NUMBER_POSITIVE: pl.Int64,
            cls.NUMBER_NEGATIVE: pl.Int64,
            cls.NUMBER_UNIQUE: pl.Int64,
        }

    @classmethod
    def pymetagen_columns(
        cls, include_name_column: bool = False
    ) -> list[MetaGenMetadataColumn]:
        columns = [
            cls.LONG_NAME,
            cls.TYPE,
            cls.DESCRIPTION,
            cls.MIN,
            cls.MAX,
            cls.STD,
            cls.MIN_LENGTH,
            cls.MAX_LENGTH,
            cls.NUMBER_NULLS,
            cls.NUMBER_EMPTY_ZERO,
            cls.NUMBER_POSITIVE,
            cls.NUMBER_NEGATIVE,
            cls.NUMBER_UNIQUE,
            cls.VALUES,
        ]
        if include_name_column:
            columns.insert(0, cls.NAME)

        return columns

    @classmethod
    def as_dict(cls) -> Mapping[MetaGenMetadataColumn, str]:
        return {col: col.value for col in cls}


class MetaGenSupportedLoadingMode(EnumListMixin, str, Enum):
    """
    MetaGen supported loading modes.
    options: lazy, eager
    """

    LAZY = "lazy"
    EAGER = "eager"


class MetaGenSupportedFileExtension(EnumListMixin, str, Enum):
    CSV = ".csv"
    JSON = ".json"
    PARQUET = ".parquet"
    XLSX = ".xlsx"
    NONE = ""

    @classmethod
    def writable_extension(
        cls, extension: str
    ) -> MetaGenSupportedFileExtension:
        if extension:
            return MetaGenSupportedFileExtension(extension)
        raise ValueError("Extension cannot be empty.")


class MetaGenDataType(str, Enum):
    string = "string"
    float = "float"
    integer = "integer"
    bool = "bool"
    date = "date"
    datetime = "datetime"
    duration = "duration"
    time = "time"
    list = "list"
    dict = "dict"
    array = "array"
    binary = "binary"
    category = "category"
    object = "object"
    unknown = "unknown"
    null = "null"

    # Polars data types
    Decimal = "Decimal"
    Float32 = "Float32"
    Float64 = "Float64"
    Int8 = "Int8"
    Int16 = "Int16"
    Int32 = "Int32"
    Int64 = "Int64"
    UInt8 = "UInt8"
    UInt16 = "UInt16"
    UInt32 = "UInt32"
    UInt64 = "UInt64"
    Date = "Date"
    Datetime = "Datetime"
    Duration = "Duration"
    Time = "Time"
    Array = "Array"
    List = "List"
    Struct = "Struct"
    Boolean = "Boolean"
    Binary = "Binary"
    Categorical = "Categorical"
    Null = "Null"
    Object = "Object"
    Utf8 = "Utf8"
    String = "String"
    Unknown = "Unknown"

    @classmethod
    def numeric_data_types(cls) -> Sequence[MetaGenDataType]:
        return [
            MetaGenDataType.Decimal,
            MetaGenDataType.Float32,
            MetaGenDataType.Float64,
            MetaGenDataType.Int8,
            MetaGenDataType.Int16,
            MetaGenDataType.Int32,
            MetaGenDataType.Int64,
            MetaGenDataType.UInt8,
            MetaGenDataType.UInt16,
            MetaGenDataType.UInt32,
            MetaGenDataType.UInt64,
            MetaGenDataType.float,
            MetaGenDataType.integer,
        ]

    @classmethod
    def date_data_types(cls) -> Sequence[MetaGenDataType]:
        return [
            MetaGenDataType.Date,
            MetaGenDataType.Datetime,
            MetaGenDataType.Duration,
            MetaGenDataType.Time,
            MetaGenDataType.date,
            MetaGenDataType.datetime,
            MetaGenDataType.duration,
            MetaGenDataType.time,
        ]

    @classmethod
    def categorical_data_types(cls) -> Sequence[MetaGenDataType]:
        return [
            MetaGenDataType.Categorical,
            MetaGenDataType.Utf8,
            MetaGenDataType.String,
            MetaGenDataType.category,
            MetaGenDataType.string,
        ]


def dtype_to_metagen_type(dtype):
    d_type = str(dtype)

    starts_with_map = {
        "Utf": MetaGenDataType.string.value,
        "Float": MetaGenDataType.float.value,
        "Int": MetaGenDataType.integer.value,
        "UInt": MetaGenDataType.integer.value,
        "Datetime": MetaGenDataType.datetime.value,
        "Duration": MetaGenDataType.duration.value,
        "Time": MetaGenDataType.time.value,
        "Array": MetaGenDataType.array.value,
        "List": MetaGenDataType.list.value,
        "Struct": MetaGenDataType.dict.value,
        "Date": MetaGenDataType.date.value,
        "Categorical": MetaGenDataType.string.value,
        "Boolean": MetaGenDataType.bool.value,
        "Null": MetaGenDataType.null.value,
        "String": MetaGenDataType.string.value,
    }

    for key, value in starts_with_map.items():
        if d_type.startswith(key):
            return value

    raise ValueError(f"Unknown dtype: {d_type}")
