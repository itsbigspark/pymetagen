from typing import TypeAlias

import polars as pl

DataFrameT: TypeAlias = pl.DataFrame | pl.LazyFrame
