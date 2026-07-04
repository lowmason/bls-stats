"""Storage protocol — the ARCH §4.1 backend swap boundary."""

from __future__ import annotations

from datetime import date
from typing import Protocol

import polars as pl


class Store(Protocol):
    def observations_uri(self, program: str) -> str: ...
    def append_observations(self, program: str, df: pl.DataFrame) -> None: ...
    def scan_observations(self, program: str) -> pl.LazyFrame | None: ...
    def slot_exists(
        self, program: str, ref_date: date | None, release_date: date,
        revision: int | None, benchmark: int | None,
    ) -> bool: ...
    def append_state(self, table: str, df: pl.DataFrame) -> None: ...
    def read_state(self, table: str) -> pl.DataFrame | None: ...
