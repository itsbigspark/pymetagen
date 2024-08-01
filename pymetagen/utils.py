from __future__ import annotations

import datetime
import json
import os
from collections.abc import Sequence
from enum import Enum
from glob import glob
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
import polars as pl

from pymetagen._typing import DataFrameT

if TYPE_CHECKING:
    from typing import Self

    from pymetagen.datatypes import MetaGenSupportedLoadingMode


class EnumListMixin:
    @classmethod
    def list(cls) -> list[Self]:
        """
        List all available enum attributes.
        """
        return list(map(lambda c: c, cls))  # type: ignore

    @classmethod
    def values(cls):
        """
        Get the values of the enum as strings.
        """

        def get_value(c: Enum) -> str:
            return c.value

        return list(map(get_value, cls))  # type: ignore


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


def collect(df: DataFrameT, streaming: bool = True) -> pl.DataFrame:
    """
    Collects a dataframe. If the dataframe is a polars DataFrame, does nothing,
    if it is a polars LazyFrame, collects it.

    Usage:
        result = df.pipe(collect).method()
    """
    if isinstance(df, pl.LazyFrame):
        return df.collect(streaming=streaming)
    return df


def get_nested_path(
    base_path: Path | str, file_extension: str = "parquet"
) -> str:
    """
    Recursively search for a file with the given file_extension in a
    nested directory structure.

    For example, if the base path is:
            - base_path = /path/foo.parquet
            - file_extension = parquet
    but foo.parquet is a directory of partitioned parquet files, such as:
            - /path/foo.parquet/month=01/partition0.parquet
            - /path/foo.parquet/month=01/partition1.parquet
            - /path/foo.parquet/month=02/partition0.parquet
            - /path/foo.parquet/month=02/partition1.parquet
    then this function will return:
            - /path/foo.parquet/*/*.parquet
    It will add a wildcard "*" for each partitioned directory that it finds.

    Args:
        base_path: base directory
        file_extension: file extension to recursively search.
                        Defaults to parquet

    Returns:
        recursive path to file
    """
    nested_path = str(base_path)
    list_of_paths = glob(nested_path)
    path_in_nested_paths = list_of_paths.pop() if list_of_paths else ""
    if os.path.isdir(path_in_nested_paths):
        new_nested_base_path = os.path.join(base_path, "*")
        new_nested_path = os.path.join(base_path, f"*.{file_extension}")
        if glob(new_nested_path):
            return new_nested_path
        else:
            return get_nested_path(new_nested_base_path)
    else:
        return nested_path


def sample(
    df: DataFrameT,
    mode: MetaGenSupportedLoadingMode,
    tbl_rows: int = 10,
    random_seed: int | None = None,
    with_replacement: bool = False,
) -> DataFrameT:

    if mode == "eager":
        assert isinstance(df, pl.DataFrame)
        row_depth = df.height
        return df.sample(
            n=min(tbl_rows, row_depth),
            with_replacement=with_replacement,
            seed=random_seed,
        )
    elif mode == "lazy":
        assert isinstance(df, pl.LazyFrame)
        random_generator = np.random.default_rng(random_seed)
        row_depth = int(df.select(pl.first()).select(pl.len()).collect()[0, 0])

        row_indexes = random_generator.choice(
            a=row_depth,
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
    else:
        raise NotImplementedError(
            f"mode must be one of {MetaGenSupportedLoadingMode.list()}"
        )


def extract_data(
    df: DataFrameT,
    mode: MetaGenSupportedLoadingMode,
    tbl_rows: int = 10,
    inspection_mode: InspectionMode = InspectionMode.head,
    random_seed: int | None = None,
    with_replacement: bool = False,
) -> pl.DataFrame:
    """
    Extract a data.
    """
    if inspection_mode not in InspectionMode.list():
        raise NotImplementedError(
            f"inspection_mode must be one of {InspectionMode.list()}"
        )
    if inspection_mode == InspectionMode.sample:
        df = df.pipe(sample, mode, tbl_rows, random_seed, with_replacement)
    elif inspection_mode == InspectionMode.tail:
        df = df.tail(tbl_rows)
    elif inspection_mode == InspectionMode.head:
        df = df.head(tbl_rows)

    return df.pipe(collect, False)


class CustomEncoder(json.JSONEncoder):
    def default(self, obj: object):
        if isinstance(obj, set):
            return list(obj)
        if isinstance(obj, datetime.datetime | datetime.date | datetime.time):
            return obj.isoformat()
        if isinstance(obj, datetime.datetime):
            return obj.isoformat(sep="T", timespec="seconds")
        if isinstance(obj, datetime.timedelta):
            return str(obj)
        if isinstance(obj, Enum):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


class CustomDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(
            self, object_hook=self.object_hook, *args, **kwargs
        )

    def object_hook(self, obj):
        ret = {}
        for key, value in obj.items():
            if isinstance(value, dict):
                ret[key] = self.object_hook(value)
            elif isinstance(value, list):
                ret[key] = [
                    self.object_hook(item) if isinstance(item, dict) else item
                    for item in value
                ]
            elif isinstance(value, str):
                try:
                    if "date" in key or key in {"dob"}:
                        ret[key] = datetime.date.fromisoformat(value)
                    elif "datetime" in key or key in {"dt", "timestamp"}:
                        ret[key] = datetime.datetime.fromisoformat(value)
                    else:
                        ret[key] = value
                except (AttributeError, ValueError):
                    pass
            else:
                ret[key] = value
        return ret


def map_inspection_modes(
    inspection_modes: Sequence[str],
) -> Sequence[InspectionMode]:
    """
    Map inspection modes to InspectionMode enum. If the inspection mode is not
    supported, raise an error.  Supported inspection modes are head, tail, and
    sample.

    Args:
        inspection_modes: list of inspection modes as strings

    Returns:
        list of InspectionMode enums
    """
    if not all(
        inspection_mode in InspectionMode.list()
        for inspection_mode in inspection_modes
    ):
        raise ValueError(
            f"inspection_modes must be one of {InspectionMode.list()}"
        )

    return [
        InspectionMode(inspection_mode) for inspection_mode in inspection_modes
    ]


def map_string_to_list_inspection_modes(
    inspection_modes: str | None,
) -> Sequence[InspectionMode]:
    """
    Map inspection modes to a list of InspectionMode enum.
    If the inspection mode is not supported, raise an error.
    Supported inspection modes are head, tail, and sample.
    In case the inspection_modes is None, return all InspectionMode enums.


    Args:
        inspection_modes: list of inspection modes as strings separated
        by commas

    Returns:
        list of InspectionMode enums
    """
    if inspection_modes is None:
        return InspectionMode.list()
    list_of_ignored_inspection_modes = inspection_modes.replace(" ", "").split(
        ","
    )
    return map_inspection_modes(
        inspection_modes=list_of_ignored_inspection_modes
    )
