"""Append-only slot ledger (ARCH §4.5): one row per slot, latest-status-wins resolution."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime

import polars as pl

from bls_stats.storage.backend import Store

LEDGER_SCHEMA: dict[str, pl.DataType] = {
    "program": pl.Utf8,
    "ref_date": pl.Date,
    "release_date": pl.Date,
    "revision": pl.Int16,
    "benchmark": pl.Int16,
    "source": pl.Utf8,
    "row_count": pl.Int64,
    "status": pl.Utf8,
    "ingested_at": pl.Datetime("us", "UTC"),
}
SLOT_KEY = ["program", "ref_date", "release_date", "revision", "benchmark"]


@dataclass(frozen=True)
class SlotRecord:
    program: str
    ref_date: date | None
    release_date: date
    revision: int | None
    benchmark: int | None
    source: str
    row_count: int
    status: str  # ingested | deferred | missed
    ingested_at: datetime


class Ledger:
    TABLE = "ledger"

    def __init__(self, store: Store) -> None:
        self.store = store

    def record(self, records: list[SlotRecord]) -> None:
        if not records:
            return
        df = pl.DataFrame([asdict(r) for r in records], schema=LEDGER_SCHEMA)
        self.store.append_state(self.TABLE, df)

    def _raw(self) -> pl.DataFrame:
        raw = self.store.read_state(self.TABLE)
        return raw if raw is not None else pl.DataFrame(schema=LEDGER_SCHEMA)

    def resolved(self) -> pl.DataFrame:
        """Latest row per slot key. Polars group keys treat nulls as equal — null-safe."""
        return (
            self._raw()
            .sort("ingested_at", descending=True)
            .unique(subset=SLOT_KEY, keep="first", maintain_order=True)
        )

    def slot_status(
        self,
        program: str,
        ref_date: date | None,
        release_date: date,
        revision: int | None,
        benchmark: int | None,
    ) -> str | None:
        hit = self.resolved().filter(
            (pl.col("program") == program)
            & pl.col("ref_date").eq_missing(pl.lit(ref_date))
            & (pl.col("release_date") == release_date)
            & pl.col("revision").eq_missing(pl.lit(revision, dtype=pl.Int16))
            & pl.col("benchmark").eq_missing(pl.lit(benchmark, dtype=pl.Int16))
        )
        return hit["status"][0] if hit.height else None

    def prior_benchmark_count(self, program: str, ref_date: date | None) -> int:
        got = (
            self.resolved()
            .filter(
                (pl.col("program") == program)
                & pl.col("ref_date").eq_missing(pl.lit(ref_date))
                & (pl.col("status") == "ingested")
            )["benchmark"]
            .max()
        )
        return int(got) if got is not None else 0
