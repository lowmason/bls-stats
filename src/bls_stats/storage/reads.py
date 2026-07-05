"""The three canonical vintage reads (ARCH §4.4), with the deterministic tie-break."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

import polars as pl


def latest(lf: pl.LazyFrame, unit_columns: Sequence[str]) -> pl.LazyFrame:
    key = [*unit_columns, "ref_date"]
    return (
        lf.with_columns(
            pl.when(pl.col("source") == "increment").then(1).otherwise(0).alias("_src_rank")
        )
        .sort(
            ["release_date", "_src_rank", "benchmark", "revision"],
            descending=True,
            nulls_last=True,
        )
        .unique(subset=key, keep="first", maintain_order=True)
        .drop("_src_rank")
    )


def as_of(lf: pl.LazyFrame, unit_columns: Sequence[str], when: date) -> pl.LazyFrame:
    return latest(lf.filter(pl.col("release_date") <= when), unit_columns)


def prints(
    lf: pl.LazyFrame, revision: int | None = None, benchmark: int | None = None
) -> pl.LazyFrame:
    if revision is not None:
        lf = lf.filter(pl.col("revision") == revision)
    if benchmark is not None:
        lf = lf.filter(pl.col("benchmark") == benchmark)
    return lf
