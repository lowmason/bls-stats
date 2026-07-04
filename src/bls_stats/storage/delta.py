"""Delta Lake vintage store (ARCH §4). One Delta table per program, partitioned by release_date."""

from __future__ import annotations

from datetime import date

import polars as pl
from deltalake.exceptions import TableNotFoundError

VINTAGE_COLUMNS: dict[str, pl.DataType] = {
    "ref_date": pl.Date,
    "release_date": pl.Date,
    "revision": pl.Int16,
    "benchmark": pl.Int16,
    "source": pl.Utf8,
    "downloaded": pl.Datetime("us", "UTC"),
}


def _eq_missing(col: str, value) -> pl.Expr:
    return pl.col(col).eq_missing(pl.lit(value))


class VintageStore:
    def __init__(self, uri: str, storage_options: dict[str, str] | None = None) -> None:
        self.uri = uri.rstrip("/")
        self.storage_options = storage_options or None

    # -- observations ---------------------------------------------------
    def observations_uri(self, program: str) -> str:
        return f"{self.uri}/{program}/observations"

    def append_observations(self, program: str, df: pl.DataFrame) -> None:
        missing = [c for c in VINTAGE_COLUMNS if c not in df.columns]
        if missing:
            raise ValueError(f"frame missing vintage columns: {missing}")
        for col, dtype in VINTAGE_COLUMNS.items():
            if df.schema[col] != dtype:
                raise ValueError(f"{col}: expected {dtype}, got {df.schema[col]}")
        df.write_delta(
            self.observations_uri(program),
            mode="append",
            storage_options=self.storage_options,
            delta_write_options={"partition_by": ["release_date"]},
        )

    def scan_observations(self, program: str) -> pl.LazyFrame | None:
        return self._scan(self.observations_uri(program))

    def slot_exists(
        self,
        program: str,
        ref_date: date | None,
        release_date: date,
        revision: int | None,
        benchmark: int | None,
    ) -> bool:
        lf = self.scan_observations(program)
        if lf is None:
            return False
        hit = (
            lf.filter(
                _eq_missing("ref_date", ref_date)
                & (pl.col("release_date") == release_date)
                & _eq_missing("revision", revision)
                & _eq_missing("benchmark", benchmark)
            )
            .head(1)
            .collect()
        )
        return hit.height > 0

    # -- state tables ----------------------------------------------------
    def append_state(self, table: str, df: pl.DataFrame) -> None:
        df.write_delta(
            f"{self.uri}/state/{table}", mode="append", storage_options=self.storage_options
        )

    def read_state(self, table: str) -> pl.DataFrame | None:
        lf = self._scan(f"{self.uri}/state/{table}")
        return None if lf is None else lf.collect()

    def _scan(self, uri: str) -> pl.LazyFrame | None:
        # pl.scan_delta builds its LazyFrame lazily in this deltalake/polars combo: a
        # missing table doesn't raise until the plan is resolved (e.g. on collect()).
        # Force resolution here with a cheap schema probe so callers reliably get None
        # for an absent table instead of a LazyFrame that blows up later.
        # This tuple is deliberately narrow: transient I/O and permission errors PROPAGATE
        # rather than masquerade as "table absent" to prevent duplicate append scenarios.
        try:
            lf = pl.scan_delta(uri, storage_options=self.storage_options)
            lf.collect_schema()
            return lf
        except (TableNotFoundError, FileNotFoundError):
            return None
