"""Orchestrator (ARCH §7): detect → expand → fetch → validate → commit → record."""

from __future__ import annotations

import logging
import tempfile
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

from bls_stats.core.config import Settings
from bls_stats.core.http import build_client
from bls_stats.core.periods import Period, ref_date, reference_periods
from bls_stats.registry import REGISTRY
from bls_stats.releases import feeds
from bls_stats.releases.calendar import filter_published
from bls_stats.releases.profiles import Slot, expand
from bls_stats.storage.backend import Store
from bls_stats.vintage.ledger import Ledger, SlotRecord

log = logging.getLogger(__name__)


class ValidationError(RuntimeError):
    pass


def _utcnow() -> datetime:
    return datetime.now(UTC)


def stamp(
    df: pl.DataFrame,
    ref: date | None,
    release: date,
    revision: int | None,
    benchmark: int | None,
    source: str,
    downloaded: datetime,
) -> pl.DataFrame:
    out = df.with_columns(
        pl.lit(release).alias("release_date"),
        pl.lit(revision, dtype=pl.Int16).alias("revision"),
        pl.lit(benchmark, dtype=pl.Int16).alias("benchmark"),
        pl.lit(source).alias("source"),
        pl.lit(downloaded).dt.cast_time_unit("us").alias("downloaded"),
    )
    if "ref_date" not in out.columns:
        out = out.with_columns(pl.lit(ref).alias("ref_date"))
    return out


def validate(df: pl.DataFrame, program: str, comparator_count: int | None) -> None:
    """ARCH §7.3 gates 1 & 3 (gate 2 — emptiness — is handled by the caller as a deferral)."""
    spec = REGISTRY[program]
    for col in spec.unit_columns:
        if col not in df.columns:
            raise ValidationError(f"{program}: missing unit column {col!r}")
        if df.schema[col] != pl.Utf8:
            raise ValidationError(f"{program}: {col} must be Utf8, got {df.schema[col]}")
    if "value" in df.columns:
        null_rate = df["value"].null_count() / max(df.height, 1)
        if null_rate > spec.null_rate_max:
            raise ValidationError(
                f"{program}: value null-rate {null_rate:.1%} > {spec.null_rate_max:.0%}"
            )
    if comparator_count is not None and comparator_count > 0:
        lo, hi = comparator_count * (1 - spec.row_band), comparator_count * (1 + spec.row_band)
        if not lo <= df.height <= hi:
            raise ValidationError(
                f"{program}: row count {df.height} outside "
                f"±{spec.row_band:.0%} of {comparator_count}"
            )


def _fetch_event(
    client, program: str, slots: list[Slot], dest_dir: Path, downloaded: datetime
) -> pl.DataFrame:
    """Default fetch dispatch: one download per event (ARCH §6.3)."""
    spec = REGISTRY[program]
    refs = [s.ref_date for s in slots]
    if program == "qcew":
        from bls_stats.engines.qcew import fetch_year

        frames = []
        for year in sorted({r.year for r in refs}):
            quarters = sorted({(r.month + 2) // 3 for r in refs if r.year == year})
            frames.append(fetch_year(client, year, quarters, dest_dir, downloaded))
        return pl.concat(frames, how="vertical_relaxed")
    if program == "oews":
        from bls_stats.engines.oews import fetch_year as fetch_oews

        return fetch_oews(client, refs[0].year, dest_dir, downloaded)
    if program == "ep":
        from bls_stats.engines.ep import fetch_matrix

        return fetch_matrix(client, downloaded=downloaded)
    from bls_stats.engines.labstat import fetch

    periods: list[Period] = [
        (r.year, r.month if spec.frequency == "monthly" else (r.month + 2) // 3) for r in refs
    ]
    url = (
        spec.benchmark_url
        if any(s.kind == "benchmark_window" for s in slots)
        else spec.increment_url
    )
    assert url is not None
    return fetch(client, program, url, periods, dest_dir, downloaded)


def _expire_superseded(
    ledger: Ledger, program: str, newer: date, now: datetime, dry_run: bool
) -> None:
    """ARCH §5.3: a deferred slot whose live-vintage window closed (a newer release for the
    same program ingested) transitions to missed — never by wall-clock timeout."""
    stale = ledger.resolved().filter(
        (pl.col("program") == program)
        & (pl.col("status") == "deferred")
        & (pl.col("release_date") < newer)
    )
    if stale.height and not dry_run:
        ledger.record(
            [
                SlotRecord(
                    program,
                    r["ref_date"],
                    r["release_date"],
                    r["revision"],
                    r["benchmark"],
                    "increment",
                    0,
                    "missed",
                    now,
                )
                for r in stale.iter_rows(named=True)
            ]
        )
        log.warning(
            "%s: %d deferred slot(s) superseded by %s -> missed", program, stale.height, newer
        )


def _comparator(ledger: Ledger, program: str, revision: int | None) -> int | None:
    """ARCH §7.3: most recent ingested row_count for the same program and slot type."""
    got = (
        ledger.resolved()
        .filter(
            (pl.col("program") == program)
            & (pl.col("status") == "ingested")
            & pl.col("revision").eq_missing(pl.lit(revision, dtype=pl.Int16))
        )
        .sort("ingested_at", descending=True)
    )
    return int(got["row_count"][0]) if got.height else None


def run_ingest(
    settings: Settings,
    store: Store,
    programs: list[str] | None = None,
    *,
    dry_run: bool = False,
    clock: Callable[[], datetime] | None = None,
    poll_fn=None,
    fetch_fn=None,
    fresh_fn=None,
) -> int:
    clock = clock or _utcnow
    programs = programs or [p for p in REGISTRY if p != "ep"]  # ep: ARCH §5.2 exception
    poll_fn = poll_fn or feeds.poll
    fetch_fn = fetch_fn or _fetch_event
    if fresh_fn is None:
        from bls_stats.engines.labstat import is_fresh as fresh_fn  # noqa: PLW0127

    client = build_client(settings)
    ledger = Ledger(store)
    outcomes: list[str] = []
    for release in poll_fn(client, programs):
        slots = [
            s
            for s in expand(
                release,
                lambda rd, program=release.program: ledger.prior_benchmark_count(program, rd),
            )
            if ledger.slot_status(
                release.program, s.ref_date, release.release_date, s.revision, s.benchmark
            )
            not in ("ingested", "missed")
        ]
        if not slots:
            continue
        outcome = _process_event(
            release,
            slots,
            settings,
            store,
            ledger,
            client,
            dry_run=dry_run,
            now=clock(),
            fetch_fn=fetch_fn,
            fresh_fn=fresh_fn,
        )
        if outcome == "ok":
            _expire_superseded(ledger, release.program, release.release_date, clock(), dry_run)
        outcomes.append(outcome)
    failed = outcomes.count("failed")
    if failed and failed == len(outcomes):
        return 2
    return 1 if failed or "partial" in outcomes else 0


def _process_event(
    release,
    slots,
    settings,
    store,
    ledger,
    client,
    *,
    dry_run: bool,
    now: datetime,
    fetch_fn,
    fresh_fn,
) -> str:
    program = release.program
    label = f"{program} release {release.release_date}"
    appended = 0

    def _record(status: str, slot: Slot, row_count: int = 0) -> None:
        if not dry_run:
            ledger.record(
                [
                    SlotRecord(
                        program,
                        slot.ref_date,
                        release.release_date,
                        slot.revision,
                        slot.benchmark,
                        "increment",
                        row_count,
                        status,
                        now,
                    )
                ]
            )

    try:
        spec = REGISTRY[program]
        if (
            spec.increment_url
            and spec.increment_url.startswith("https://download.bls.gov")
            and not fresh_fn(client, program, release.release_date)
        ):
            log.warning("%s: file not yet fresh — deferring %d slot(s)", label, len(slots))
            for slot in slots:
                _record("deferred", slot)
            return "deferred"
        with tempfile.TemporaryDirectory() as td:
            df = fetch_fn(client, program, slots, Path(td), now)
        committed = 0
        for slot in slots:
            piece = (
                df.filter(pl.col("ref_date") == slot.ref_date) if "ref_date" in df.columns else df
            )
            if piece.is_empty():
                log.warning("%s: empty slice for %s — deferred", label, slot.ref_date)
                _record("deferred", slot)
                continue
            validate(piece, program, _comparator(ledger, program, slot.revision))
            stamped = stamp(
                piece,
                slot.ref_date,
                release.release_date,
                slot.revision,
                slot.benchmark,
                "increment",
                now,
            )
            if store.slot_exists(
                program, slot.ref_date, release.release_date, slot.revision, slot.benchmark
            ):
                log.warning("%s: slot %s already committed — repairing ledger only", label, slot)
            elif not dry_run:
                store.append_observations(program, stamped)
                appended += 1
            _record("ingested", slot, stamped.height)
            committed += 1
        log.info("%s: %d/%d slots committed", label, committed, len(slots))
        return "ok" if committed else "deferred"
    except Exception:
        log.exception("%s: event failed", label)
        return "partial" if appended else "failed"  # data committed => partial (ARCH §7.4)


def run_backfill(
    settings: Settings,
    store: Store,
    program: str,
    start: str,
    end: str,
    *,
    dry_run: bool = False,
    clock: Callable[[], datetime] | None = None,
    fetch_fn=None,
) -> int:
    clock = clock or _utcnow
    now = clock()
    snapshot_date = now.date()
    cal = store.read_state("release_calendar")
    if cal is None:
        log.error("release calendar missing — run `bls-stats calendar build` first (ARCH §8)")
        return 2
    periods = filter_published(program, reference_periods(program, start, end), cal)
    if not periods:
        log.warning("%s: no published periods in range", program)
        return 0
    client = build_client(settings)
    ledger = Ledger(store)
    fetch_fn = fetch_fn or _fetch_event
    slots = [Slot(ref_date(program, y, p), None, None, "backfill") for y, p in periods]  # type: ignore[arg-type]
    todo = [
        s
        for s in slots
        if ledger.slot_status(program, s.ref_date, snapshot_date, None, None) != "ingested"
    ]
    if not todo:
        log.info("%s: backfill already complete for range", program)
        return 0
    try:
        with tempfile.TemporaryDirectory() as td:
            df = fetch_fn(client, program, todo, Path(td), now)
        records = []
        for slot in todo:
            piece = (
                df.filter(pl.col("ref_date") == slot.ref_date) if "ref_date" in df.columns else df
            )
            if piece.is_empty():
                continue
            stamped = stamp(piece, slot.ref_date, snapshot_date, None, None, "backfill", now)
            if not store.slot_exists(program, slot.ref_date, snapshot_date, None, None):
                if not dry_run:
                    store.append_observations(program, stamped)
            records.append(
                SlotRecord(
                    program,
                    slot.ref_date,
                    snapshot_date,
                    None,
                    None,
                    "backfill",
                    stamped.height,
                    "ingested",
                    now,
                )
            )
        if not dry_run:
            ledger.record(records)
        log.info("%s: backfilled %d period(s)", program, len(records))
        return 0
    except Exception:
        log.exception("%s: backfill failed", program)
        return 2
