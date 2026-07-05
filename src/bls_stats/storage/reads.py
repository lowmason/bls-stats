"""The three canonical vintage reads (ARCH §4.4), with the deterministic tie-break."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

import polars as pl


def latest(lf: pl.LazyFrame, unit_columns: Sequence[str]) -> pl.LazyFrame:
    """Pick the most current row per (unit, `ref_date`): max `release_date`, ties broken.

    "Most current" means highest `release_date`; when multiple rows tie on `release_date` —
    possible when a backfill snapshot lands the same morning as a routine release — the tie is
    broken deterministically by sorting all four columns descending, nulls last, then keeping
    the first row per key:

    1. `release_date` — the primary ordering; higher is more current.
    2. `_src_rank` — `source == "increment"` ranks above `"backfill"`. An increment is a
       genuine new print for that release date; a same-day backfill snapshot is a bulk re-seed
       that happens to land then. The increment reflects the actual release and should win.
    3. `benchmark` — higher benchmark counter wins among same-`release_date` rows.
    4. `revision` — higher revision counter wins last, after benchmark.

    Args:
        lf: Observation frame (or filtered slice of one) with the ARCH §4.3 vintage columns.
        unit_columns: Registry-defined unit-identity columns for this program (e.g.
            `["series_id"]` for LABSTAT programs; see ARCH §4.3 for QCEW/OEWS/EP).

    Returns:
        A `LazyFrame` with exactly one row per (unit columns, `ref_date`) — the winning vintage.
    """
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
    """Point-in-time read: the `latest` row per unit as it was known on or before `when`.

    Filters to `release_date <= when` (inclusive of `when` itself) before applying `latest`'s
    tie-break. Never returns a row whose `release_date` is after `when` — this is the
    no-future-leakage guarantee that as-of correctness depends on (ARCH §4.4, §9).

    Args:
        lf: Observation frame with the ARCH §4.3 vintage columns.
        unit_columns: Registry-defined unit-identity columns for this program.
        when: Point-in-time cutoff date, inclusive.

    Returns:
        A `LazyFrame` with one row per (unit columns, `ref_date`), reflecting only vintages
        released on or before `when`.
    """
    return latest(lf.filter(pl.col("release_date") <= when), unit_columns)


def prints(
    lf: pl.LazyFrame, revision: int | None = None, benchmark: int | None = None
) -> pl.LazyFrame:
    """Filter to a specific print by its (`revision`, `benchmark`) counters.

    Unlike `latest`/`as_of`, this does not collapse to one row per unit — it returns every row
    matching the given counter(s), which may span multiple units and `ref_date`s. Passing
    neither argument returns `lf` unfiltered (all prints). Filters use plain equality, not
    null-safe matching — pass `None` to skip filtering on that counter rather than to match
    null rows.

    Args:
        lf: Observation frame with the ARCH §4.3 vintage columns.
        revision: Exact routine print counter to filter on, or `None` to skip.
        benchmark: Exact benchmark counter to filter on, or `None` to skip.

    Returns:
        A `LazyFrame` filtered to rows matching the given counter(s).
    """
    if revision is not None:
        lf = lf.filter(pl.col("revision") == revision)
    if benchmark is not None:
        lf = lf.filter(pl.col("benchmark") == benchmark)
    return lf
