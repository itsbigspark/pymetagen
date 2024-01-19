from __future__ import annotations

import os
from glob import glob
from typing import Any

import polars as pl

from pymetagen._typing import DataFrameT


class EnumListMixin:
    @classmethod
    def list(cls) -> list[str]:
        return list(map(lambda c: c.value, cls))


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


def collect(df: DataFrameT):
    """
    Collects a dataframe. If the dataframe is a polars DataFrame, does nothing,
    if it is a polars LazyFrame, collects it.

    Usage:
        result = df.pipe(collect).method()
    """
    if isinstance(df, pl.LazyFrame):
        return df.collect()

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
