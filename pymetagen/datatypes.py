from __future__ import annotations

from enum import Enum

import polars as pl

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
    ]

    date_data_types: list[MetaGenDataType] = [
        Date,
        Datetime,
        Duration,
        Time,
    ]

    categorical_data_types: list[MetaGenDataType] = [
        Categorical,
        Utf8,
    ]

    @classmethod
    def polars_datatypes(cls) -> list[MetaGenDataType]:
        return [
            item
            for item in list(map(lambda c: c, MetaGenDataType))
            if not item.islower()
        ]

    @classmethod
    def polars_to_metagen_type(
        cls, polars_data_type: pl.DataType
    ) -> MetaGenDataType:
        if type(polars_data_type) != type(pl.DataType):  # noqa: E721
            raise TypeError(f"{polars_data_type} is not a polars.DataType")

        _type = polars_data_type.base_type().__name__
        if _type not in cls.polars_datatypes():
            raise NotImplementedError(
                f"Polar data type {_type} not yet implemented"
            )

        if _type in FloatTypes:
            return cls.float
        if _type in IntegerTypes:
            return cls.integer
        if _type in DateTypes:
            return cls.date
        if _type == cls.Utf8:
            return cls.string
        if _type == cls.Categorical:
            return cls.category
        if _type == cls.Datetime:
            return cls.datetime
        if _type == cls.Null:
            return cls.null
        if _type == cls.Boolean:
            return cls.bool
        if _type == cls.Binary:
            return cls.binary
        if _type == cls.Unknown:
            return cls.unknown
        if _type == cls.Struct:
            return cls.dict
        if _type == cls.List:
            return cls.list
        if _type == cls.Array:
            return cls.array
        if _type == cls.Object:
            return cls.object

    @classmethod
    def readable_data_type(cls, polars_data_type: pl.DataType) -> str:
        return cls.polars_to_metagen_type(polars_data_type).value


FloatTypes = [
    MetaGenDataType.Decimal,
    MetaGenDataType.Float32,
    MetaGenDataType.Float64,
]

IntegerTypes = [
    MetaGenDataType.Int8,
    MetaGenDataType.Int16,
    MetaGenDataType.Int32,
    MetaGenDataType.Int64,
    MetaGenDataType.UInt8,
    MetaGenDataType.UInt16,
    MetaGenDataType.UInt32,
    MetaGenDataType.UInt64,
]

DateTypes = [
    MetaGenDataType.Date,
    MetaGenDataType.Duration,
    MetaGenDataType.Time,
]
