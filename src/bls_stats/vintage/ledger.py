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
"""Column schema of the `state/ledger` Delta table (ARCH §4.5). One row per ledger event; the
slot-identifying columns (`SLOT_KEY`) are nullable (`ref_date`/`revision`/`benchmark` null for
backfill/EP slots), matched null-safely throughout this module. `status` is one of `ingested`
| `deferred` | `missed` (ARCH §5.3); `ingested_at` is the wall-clock append time used to resolve
the latest row per slot."""

SLOT_KEY = ["program", "ref_date", "release_date", "revision", "benchmark"]
"""The ledger's logical primary key (ARCH §4.5): one slot per (program, ref_date, release_date,
revision, benchmark). Not a uniqueness constraint on the append-only table — the same slot key
can appear in multiple rows across a status lifecycle; `resolved()` collapses to one row per key
by keeping the latest `ingested_at`."""


@dataclass(frozen=True)
class SlotRecord:
    """One ledger event: an ingest attempt's outcome for a single slot (ARCH §4.5).

    Immutable and append-only by design — a status transition (e.g. `deferred` → `ingested`) is
    recorded by constructing and appending a *new* `SlotRecord` for the same slot key, never by
    mutating or replacing an existing row.

    Attributes:
        program: Program key, e.g. `"ces"`, `"qcew"`.
        ref_date: Canonical period date, or `None` for non-periodic slots (EP) and for
            counting purposes matched null-safely against other null `ref_date` rows.
        release_date: Which BLS release this event corresponds to.
        revision: Routine print counter, or `None` for backfill slots.
        benchmark: Benchmark counter, or `None` for backfill slots.
        source: `"backfill"` or `"increment"`.
        row_count: Number of observation rows fetched for this slot at record time.
        status: Lifecycle outcome — `"ingested"`, `"deferred"` (retried every run until
            superseded), or `"missed"` (permanent gap once the slot's live-vintage window
            closes; ARCH §5.3).
        ingested_at: Wall-clock time this event was recorded (injected clock, UTC).
    """

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
    """Append-only record of ingest outcomes, one row per slot event (ARCH §4.5).

    Backed by a `Store`'s `state/ledger` table. Every write is an append (`record`); every read
    resolves the latest status per slot key by `ingested_at` (`resolved`, `slot_status`). This
    module never edits or deletes a row — a status transition is always a new row, which keeps
    the ledger's history auditable and makes crash-repair (ARCH §7.2) a matter of appending one
    more `ingested` row rather than reconciling in place.

    Attributes:
        store: Backing `Store` (typically a `VintageStore`) whose `state/ledger` table holds
            the raw, unresolved event rows.
    """

    TABLE = "ledger"

    def __init__(self, store: Store) -> None:
        self.store = store

    def record(self, records: list[SlotRecord]) -> None:
        """Append one or more ledger events in a single write.

        An ingest event that spans multiple slots (e.g. a CES release carrying prints for
        t, t-1, t-2) should pass all of its `SlotRecord`s together here so they land in one
        transaction. A `None`/empty list is a no-op — no table is created or touched.

        Args:
            records: Ledger events to append, in `LEDGER_SCHEMA` field order per record.
        """
        if not records:
            return
        df = pl.DataFrame([asdict(r) for r in records], schema=LEDGER_SCHEMA)
        self.store.append_state(self.TABLE, df)

    def _raw(self) -> pl.DataFrame:
        raw = self.store.read_state(self.TABLE)
        return raw if raw is not None else pl.DataFrame(schema=LEDGER_SCHEMA)

    def resolved(self) -> pl.DataFrame:
        """Collapse the append-only ledger to its current state: latest row per slot key.

        Sorts by `ingested_at` descending and keeps the first row per `SLOT_KEY`. Polars'
        `unique`/group-by treats nulls within the subset columns as equal to each other, so
        this is null-safe by construction for backfill slots (null `ref_date`/`revision`/
        `benchmark`) without an explicit `eq_missing` — unlike `slot_status` and
        `prior_benchmark_count`, which filter with an externally supplied slot key and do need
        `eq_missing` for that.

        Returns:
            A `DataFrame` in `LEDGER_SCHEMA` with exactly one row per distinct slot key present
            in the ledger, reflecting each slot's most recently recorded status.
        """
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
        """Look up the current (resolved) status of one exact slot.

        Matches every slot-key column null-safely (`eq_missing`) so a backfill slot (null
        `ref_date`/`revision`/`benchmark`) can be looked up with `None` arguments and match
        only other null rows, not be excluded or mismatched by ordinary `==` semantics.

        Args:
            program: Program key.
            ref_date: Canonical period date, or `None` to match null `ref_date` rows.
            release_date: Release date identifying the event.
            revision: Routine print counter, or `None` to match null `revision` rows.
            benchmark: Benchmark counter, or `None` to match null `benchmark` rows.

        Returns:
            `"ingested"`, `"deferred"`, or `"missed"` if the slot has any recorded event;
            `None` if the slot has never appeared in the ledger.
        """
        hit = self.resolved().filter(
            (pl.col("program") == program)
            & pl.col("ref_date").eq_missing(pl.lit(ref_date))
            & (pl.col("release_date") == release_date)
            & pl.col("revision").eq_missing(pl.lit(revision, dtype=pl.Int16))
            & pl.col("benchmark").eq_missing(pl.lit(benchmark, dtype=pl.Int16))
        )
        return hit["status"][0] if hit.height else None

    def prior_benchmark_count(
        self, program: str, ref_date: date | None, before_release: date | None = None
    ) -> int:
        """Highest benchmark counter successfully ingested so far for a (program, ref_date).

        The pipeline passes this as the prior-count callback into `expand()`, which uses it to
        assign the next benchmark counter to a new benchmark event. Only `status == "ingested"`
        rows count — a `deferred` benchmark slot has not actually landed data yet, so it must
        not advance the counter (ARCH §5.3).

        When `before_release` is given, only rows with `release_date < before_release` count.
        The pipeline passes the release being processed, so the counter is derived from prints
        at strictly-earlier release dates — a re-poll of the same benchmark release therefore
        recomputes the same counter and its slots resolve as already-ingested (idempotent,
        ARCH §4.3/§7.2), instead of climbing 1→2→3 and re-appending the window every run.

        Args:
            program: Program key.
            ref_date: Canonical period date; matched null-safely.
            before_release: If given, exclude rows whose `release_date` is not strictly earlier.

        Returns:
            The maximum `benchmark` among qualifying ingested rows, or `0` if none.
        """
        rows = self.resolved().filter(
            (pl.col("program") == program)
            & pl.col("ref_date").eq_missing(pl.lit(ref_date))
            & (pl.col("status") == "ingested")
        )
        if before_release is not None:
            rows = rows.filter(pl.col("release_date") < before_release)
        got = rows["benchmark"].max()
        return int(got) if got is not None else 0
