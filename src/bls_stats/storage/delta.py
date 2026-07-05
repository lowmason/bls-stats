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
"""Vintage columns every program's observation frame must carry (ARCH §4.3), keyed by name with
their required Polars dtype. `append_observations` validates a frame against this exactly:
presence and dtype match, both fail loud rather than silently coercing. `ref_date` is nullable
(null for EP, which is non-periodic); `revision`/`benchmark` are nullable Int16 (null for
`source='backfill'` rows — the backfill-honesty rule, ARCH §4.3, never fabricates print history
that was not observed)."""


def _eq_missing(col: str, value) -> pl.Expr:
    """Build a null-safe equality filter: ``col`` equals ``value``, treating null == null as True.

    Plain `==` is false when either side is null, which would make the presence check in
    `slot_exists` miss backfill rows (null `revision`/`benchmark`) and let a crashed backfill
    re-append its whole seed vintage. Mirrors SQL `IS NOT DISTINCT FROM` (ARCH §7.2).
    """
    return pl.col(col).eq_missing(pl.lit(value))


class VintageStore:
    """Delta Lake–backed implementation of the `Store` protocol (ARCH §4).

    One Delta table per program under `{uri}/{program}/observations`, partitioned by
    `release_date`; small state tables (ledger, release calendar) live under `{uri}/state/{name}`.
    A release event lands as one atomic multi-file Delta commit, avoiding a hand-rolled
    commit-marker scheme over plain Parquet (ARCH §4.1).

    Attributes:
        uri: Store root, e.g. `s3://bucket/prefix` or a local path. Trailing slashes stripped.
        storage_options: `deltalake`/object-store credentials and endpoint config (e.g.
            `AWS_ENDPOINT_URL`, conditional-PUT mode), or `None` for a local filesystem store.
    """

    def __init__(self, uri: str, storage_options: dict[str, str] | None = None) -> None:
        self.uri = uri.rstrip("/")
        self.storage_options = storage_options or None

    # -- observations ---------------------------------------------------
    def observations_uri(self, program: str) -> str:
        """Return the Delta table URI for a program's observations (ARCH §4.2 layout)."""
        return f"{self.uri}/{program}/observations"

    def append_observations(self, program: str, df: pl.DataFrame) -> None:
        """Append a validated observation frame as one atomic Delta commit.

        Validates `df` against `VINTAGE_COLUMNS` — every vintage column must be present with
        the exact expected dtype — before writing. This is a schema gate, not a dedup check:
        callers are responsible for the ARCH §7.2 presence check (`slot_exists`) beforehand to
        preserve event-slot idempotency. The write partitions by `release_date` and never
        rewrites existing files (ARCH §4.2).

        Args:
            program: Program key, e.g. `"ces"`, `"qcew"` — selects the target Delta table.
            df: Observation frame. Must include every column in `VINTAGE_COLUMNS` at its
                declared dtype, plus the program's native columns.

        Raises:
            ValueError: A vintage column is missing, or present with the wrong dtype.
        """
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
        """Lazily scan a program's observation table, or `None` if it has never been written.

        Returns:
            A `LazyFrame` over the full print history (all vintages, all slots), or `None` if
            the Delta table does not exist yet — see `_scan` for the absent-table contract.
        """
        return self._scan(self.observations_uri(program))

    def slot_exists(
        self,
        program: str,
        ref_date: date | None,
        release_date: date,
        revision: int | None,
        benchmark: int | None,
    ) -> bool:
        """Check whether an exact slot has already been committed (ARCH §7.2 presence check).

        This is the crash-repair primitive: the pipeline commits observation data *then* records
        the ledger row (ARCH §7.2), so the only crash-inconsistent state is "data committed,
        ledger missing." Calling this before an append lets a re-run detect that case, skip the
        duplicate append, and repair the ledger instead — convergent re-runs without a
        hand-rolled transaction log. Matching is null-safe (`eq_missing`) on every slot-key
        column because backfill rows carry null `ref_date`/`revision`/`benchmark`; plain `==`
        would never match them and would let a crashed backfill duplicate its entire seed
        vintage on re-run.

        Args:
            program: Program key selecting the observation table.
            ref_date: Canonical period date, or `None` (matches only other-null rows, e.g. EP).
            release_date: Which BLS release produced the row — required, not nullable.
            revision: Routine print counter, or `None` (backfill rows).
            benchmark: Benchmark counter, or `None` (backfill rows).

        Returns:
            `True` if a row matching every slot-key column (null-safe) already exists;
            `False` if the table is absent or no row matches.
        """
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
        """Append rows to a small state table (e.g. `ledger`, `release_calendar`) under `state/`.

        No schema validation is performed here — callers (e.g. `Ledger.record`) are responsible
        for conforming `df` to the table's schema before calling.

        Args:
            table: State table name, e.g. `"ledger"`.
            df: Rows to append, in the table's schema.
        """
        df.write_delta(
            f"{self.uri}/state/{table}", mode="append", storage_options=self.storage_options
        )

    def read_state(self, table: str) -> pl.DataFrame | None:
        """Read a full state table, or `None` if it has never been written.

        Unlike observations, state tables are small enough to read eagerly in full — callers
        (e.g. `Ledger.resolved`) then do the latest-row-per-key resolution in memory.

        Args:
            table: State table name, e.g. `"ledger"`.

        Returns:
            The full table as a `DataFrame`, or `None` if the table does not exist yet.
        """
        lf = self._scan(f"{self.uri}/state/{table}")
        return None if lf is None else lf.collect()

    def _scan(self, uri: str) -> pl.LazyFrame | None:
        """Resolve a Delta table URI to a `LazyFrame`, or `None` if the table doesn't exist.

        Contract every caller (`scan_observations`, `read_state`) relies on: a missing table
        returns `None` rather than a `LazyFrame` that raises later on `collect()` — `pl.scan_delta`
        builds its plan lazily and would otherwise defer the "table not found" error past the
        point where callers can cheaply branch on it. Achieved by forcing resolution with a
        `collect_schema()` probe. Only `TableNotFoundError`/`FileNotFoundError` are swallowed;
        transient I/O and permission errors propagate rather than masquerade as "table absent,"
        which would risk a duplicate append on top of a table the caller can't actually see yet.

        Args:
            uri: Full Delta table URI to scan.

        Returns:
            A `LazyFrame` over the table, or `None` if the table has never been created.
        """
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
