from __future__ import annotations

from enum import Enum
from typing import Any

from pymetagen.utils import EnumListMixin


class MetaGenSupportedLoadingModes(EnumListMixin, str, Enum):
    LAZY = "lazy"
    EAGER = "eager"


class MetaGenSupportedFileExtensions(EnumListMixin, str, Enum):
    CSV = ".csv"
    JSON = ".json"
    PARQUET = ".parquet"
    XLSX = ".xlsx"


class MetaGenDataType(str, Enum):
    string = "string"
    float = "float"
    integer = "integer"
    bool = "bool"
    date = "date"
    datetime = "datetime"
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
    Unknown = "Unknown"

    numeric_data_types: list[MetaGenDataType] = [
        Decimal,
        Float32,
        Float64,
        Int8,
        Int16,
        Int32,
        Int64,
        UInt8,
        UInt16,
        UInt32,
        UInt64,
        float,
        integer,
    ]

    date_data_types: list[MetaGenDataType] = [
        Date,
        Datetime,
        Duration,
        Time,
        date,
        datetime,
    ]

    categorical_data_types: list[MetaGenDataType] = [
        Categorical,
        Utf8,
        string,
    ]


def dtype_to_metagentype(dtype: Any):
    dtype: str = str(dtype)

    starts_with_map = {
        "Utf": MetaGenDataType.string.value,
        "Float": MetaGenDataType.float.value,
        "Int": MetaGenDataType.integer.value,
        "UInt": MetaGenDataType.integer.value,
        "Datetime": MetaGenDataType.datetime.value,
        "Date": MetaGenDataType.date.value,
        "Categorical": MetaGenDataType.string.value,
        "Boolean": MetaGenDataType.bool.value,
        "Null": MetaGenDataType.null.value,
    }

    for key, value in starts_with_map.items():
        if dtype.startswith(key):
            return value

    raise ValueError(f"Unknown dtype: {dtype}")
