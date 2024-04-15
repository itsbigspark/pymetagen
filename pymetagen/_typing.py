from __future__ import annotations

import typing

import polars as pl

if typing.TYPE_CHECKING:
    from typing import TypeAlias

DataFrameT: TypeAlias = typing.Union[pl.DataFrame, pl.LazyFrame]  # noqa: F821
Any = typing.Any
Hashable = typing.Hashable
