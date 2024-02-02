from __future__ import annotations

import datetime
import json
import os
from enum import Enum
from glob import glob
from typing import TYPE_CHECKING, Any

import numpy as np
import polars as pl

from pymetagen._typing import DataFrameT

if TYPE_CHECKING:
    from pymetagen.datatypes import MetaGenSupportedLoadingModes


class EnumListMixin:
    @classmethod
    def list(cls) -> list[str]:
        return list(map(lambda c: c.value, cls))


class InspectionMode(EnumListMixin, str, Enum):
    """
    Inspection mode for data.
    options: head, tail, sample
    """

    head = "head"
    tail = "tail"
    sample = "sample"


def selectively_update_dict(d: dict[str, Any], new_d: dict[str, Any]) -> None:
    """
    Selectively update dictionary d with any values that are in new_d,
    but being careful only to update keys in dictionaries that are present
    in new_d.

    Args:
        d: dictionary with string keys
        new_d: dictionary with string keys
    """
    for k, v in new_d.items():
        if isinstance(v, dict) and k in d:
            if isinstance(d[k], dict):
                selectively_update_dict(d[k], v)
            else:
                d[k] = v
        else:
            d[k] = v


def collect(df: DataFrameT, streaming: bool = True) -> DataFrameT:
    """
    Collects a dataframe. If the dataframe is a polars DataFrame, does nothing,
    if it is a polars LazyFrame, collects it.

    Usage:
        result = df.pipe(collect).method()
    """
    if isinstance(df, pl.LazyFrame):
        return df.collect(streaming=streaming)
    return df


def get_nested_parquet_path(base_path: str) -> str:
    """
    Recursively search for a parquet file in a nested directory structure.
    For example, if the base path is:
            - base_path = /path/foo.parquet
    but foo.parquet is a directory of partitioned parquet files, such as:
            - /path/foo.parquet/month=01/partition0.parquet
            - /path/foo.parquet/month=01/partition1.parquet
            - /path/foo.parquet/month=02/partition0.parquet
            - /path/foo.parquet/month=02/partition1.parquet
    then this function will return:
            - /path/foo.parquet/*/*.parquet
    It will add a wildcard "*" for each partitioned directory that it finds.

    Args:
        base_path: base parquet directory

    Returns:
        recursive path to parquet file
    """
    nested_path = str(base_path)
    list_of_paths = glob(nested_path)
    path_in_nested_paths = list_of_paths.pop() if list_of_paths else ""
    if os.path.isdir(path_in_nested_paths):
        new_nested_base_path = os.path.join(base_path, "*")
        new_nested_path = os.path.join(base_path, "*.parquet")
        if glob(new_nested_path):
            return new_nested_path
        else:
            return get_nested_parquet_path(new_nested_base_path)
    else:
        return nested_path


def sample(
    df: DataFrameT,
    mode: MetaGenSupportedLoadingModes,
    tbl_rows: int = 10,
    random_seed: int | None = None,
    with_replacement: bool = False,
) -> DataFrameT:
    if mode == "eager":
        return df.sample(
            n=tbl_rows, with_replacement=with_replacement, seed=random_seed
        )
    elif mode == "lazy":
        np.random.seed(random_seed)
        row_depth = (
            df.select(pl.first()).select(pl.count()).pipe(collect)[0, 0]
        )
        row_indexes = np.random.choice(
            row_depth,
            size=min(tbl_rows, row_depth),
            replace=with_replacement,
        )
        return (
            df.with_columns(
                pl.Series(pl.arange(0, row_depth, eager=True)).alias(
                    "row_index"
                )
            )
            .filter(pl.col("row_index").is_in(row_indexes))
            .drop("row_index")
        )


def extract_data(
    df: DataFrameT,
    mode: MetaGenSupportedLoadingModes,
    tbl_rows: int = 10,
    inspection_mode: InspectionMode = InspectionMode.head,
    random_seed: int | None = None,
    with_replacement: bool = False,
) -> DataFrameT:
    """
    Extract a data.
    """
    if inspection_mode == InspectionMode.sample:
        df = df.pipe(sample, mode, tbl_rows, random_seed, with_replacement)
    elif inspection_mode == InspectionMode.tail:
        df = df.tail(tbl_rows)
    else:
        df = df.head(tbl_rows)

    return df.pipe(collect, False)


class CustomEncoder(json.JSONEncoder):
    def default(self, obj: object):
        if isinstance(obj, set):
            return list(obj)
        if isinstance(obj, datetime.datetime | datetime.date | datetime.time):
            return obj.isoformat()
        if isinstance(obj, datetime.timedelta):
            return str(obj)
        if isinstance(obj, Enum):
            return str(obj)
        return json.JSONEncoder.default(self, obj)
