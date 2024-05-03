from __future__ import annotations

from enum import Enum

from pymetagen.utils import EnumListMixin


class MetaGenSupportedLoadingModes(EnumListMixin, str, Enum):
    """
    MetaGen supported loading modes.
    options: lazy, eager
    """

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
        duration,
        time,
    ]

    categorical_data_types: list[MetaGenDataType] = [
        Categorical,
        Utf8,
        string,
        category,
        String,
    ]


def dtype_to_metagen_type(dtype):
    d_type: str = str(dtype)

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
