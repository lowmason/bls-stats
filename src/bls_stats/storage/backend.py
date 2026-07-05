"""Storage protocol — the ARCH §4.1 backend swap boundary."""

from __future__ import annotations

from datetime import date
from typing import Protocol

import polars as pl


class Store(Protocol):
    """The storage interface `VintageStore` implements and pipeline code depends on.

    Exists so the Delta Lake backend can be swapped for the plain-Parquet escape hatch
    (`s3_parquet.py`, ARCH §4.1) without touching callers: both share this identical logical
    shape, so the swap cost is one module. Callers (ingest pipeline, `Ledger`, `reads` helpers)
    should type against `Store`, not `VintageStore`, wherever backend-agnostic.

    Attributes:
        uri: Store root URI (e.g. `s3://bucket/prefix` or a local path).
        storage_options: Backend credentials/config (e.g. S3 endpoint, conditional-PUT mode),
            or `None` for a local filesystem store.
    """

    uri: str
    storage_options: dict[str, str] | None

    def observations_uri(self, program: str) -> str:
        """Return the table URI for a program's observations."""
        ...

    def append_observations(self, program: str, df: pl.DataFrame) -> None:
        """Append a validated vintage-column frame as one atomic commit (ARCH §4.3)."""
        ...

    def scan_observations(self, program: str) -> pl.LazyFrame | None:
        """Lazily scan a program's full print history, or `None` if never written."""
        ...

    def slot_exists(
        self,
        program: str,
        ref_date: date | None,
        release_date: date,
        revision: int | None,
        benchmark: int | None,
    ) -> bool:
        """Null-safe presence check for one exact slot — the ARCH §7.2 crash-repair primitive."""
        ...

    def append_state(self, table: str, df: pl.DataFrame) -> None:
        """Append rows to a small state table (e.g. `ledger`, `release_calendar`)."""
        ...

    def read_state(self, table: str) -> pl.DataFrame | None:
        """Read a full state table eagerly, or `None` if it has never been written."""
        ...
