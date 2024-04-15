from __future__ import annotations

import typing

import polars as pl

DataFrameT: typing.TypeAlias = pl.DataFrame | pl.LazyFrame
Any = typing.Any
Hashable = typing.Hashable
