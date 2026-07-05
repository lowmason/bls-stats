"""Orchestrator (ARCH §7): detect → expand → fetch → validate → commit → record.

Drives both entry points that land data in the vintage store — `run_ingest` (the daily
incremental crontab line) and `run_backfill` (the one-time historical seed) — plus the
shared pieces they call: `stamp` (vintage columns), `validate` (pre-commit gates), and
the crash-safe commit-then-record sequencing (ARCH §7.2). `ep` is excluded from both
paths pending a storage-schema decision for its non-periodic wide frames (ARCH §12).
"""

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
from bls_stats.registry import REGISTRY, Frequency
from bls_stats.releases import feeds
from bls_stats.releases.calendar import filter_published
from bls_stats.releases.profiles import Slot, expand
from bls_stats.storage.backend import Store
from bls_stats.vintage.ledger import Ledger, SlotRecord

log = logging.getLogger(__name__)


class ValidationError(RuntimeError):
    """Raised by `validate` when a frame fails a pre-commit gate (ARCH §7.3).

    Failing validation fails only the event that raised it; the run continues with the
    next event (ARCH §7.4).
    """


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
    """Append the vintage columns (ARCH §4.3) to a fetched frame.

    Adds `release_date` (`pl.Date`), `revision`/`benchmark` (`pl.Int16`, nullable — null
    for backfill rows per the honesty rule), `source` (`pl.Utf8`, `"increment"` or
    `"backfill"`), and `downloaded` (`pl.Datetime("us", "UTC")`, cast to microsecond
    precision regardless of the input clock's resolution). `ref_date` is added as a
    typed-null `pl.Date` column only when the frame doesn't already carry one (engines
    that stream multiple ref_dates per event, e.g. QCEW/CES, stamp `ref_date` themselves
    upstream); when `ref` is `None` the added column is a null of the correct dtype, not
    a Python `None` that would infer as another type.

    Args:
        df: Fetched, unstamped frame for one slot or event.
        ref: Canonical period date for this slot, or `None` for non-periodic programs
            (EP) and for events that already stamped `ref_date` per-row.
        release: The BLS release date (increment) or snapshot date (backfill); becomes
            the partition key `release_date`.
        revision: Structural print counter (ARCH §2.1), or `None` for backfill rows.
        benchmark: Benchmark counter (ARCH §2.1), or `None` for backfill rows.
        source: `"increment"` or `"backfill"`.
        downloaded: Wall-clock ingestion time (injected, never `datetime.now()` inline).

    Returns:
        `df` with the vintage columns appended (and `ref_date` added if absent).
    """
    out = df.with_columns(
        pl.lit(release).alias("release_date"),
        pl.lit(revision, dtype=pl.Int16).alias("revision"),
        pl.lit(benchmark, dtype=pl.Int16).alias("benchmark"),
        pl.lit(source).alias("source"),
        pl.lit(downloaded).dt.cast_time_unit("us").alias("downloaded"),
    )
    if "ref_date" not in out.columns:
        out = out.with_columns(pl.lit(ref, dtype=pl.Date).alias("ref_date"))
    return out


def validate(df: pl.DataFrame, program: str, comparator_count: int | None) -> None:
    """Pre-commit validation gates 1 and 3 of ARCH §7.3.

    Gate 2 (emptiness) is not checked here — the caller treats an empty slice as a
    deferral, not a validation failure, since it means data lags the announcement rather
    than being malformed (ARCH §7.3-2).

    Checked, in order:
    1. **Schema** — every one of the program's registry-declared `unit_columns` is
       present and typed `pl.Utf8` (leading zeros in codes like `series_id`/`area_fips`
       must survive; an inferred numeric dtype would silently corrupt them).
    2. **Null-rate** — if a `value` column is present, its null fraction must not exceed
       the program's `null_rate_max` (registry default 5%).
    3. **Row-count band** — if `comparator_count` is a positive int, `df.height` must
       fall within ±`row_band` (registry default 20%) of it. A `None` or `0` comparator
       (no prior ingested slot of this type — e.g. a program's first increment) skips
       this gate rather than failing it.

    Args:
        df: Stamped or unstamped frame for one slot, already sliced to its `ref_date`.
        program: Registry key selecting the `ProgramSpec` (unit columns, thresholds).
        comparator_count: Row count of the most recent ingested slot of the same program
            and slot type (see `_comparator`), or `None` if there is none yet.

    Raises:
        ValidationError: On any gate failure, naming the program and the specific
            violation (missing/mistyped unit column, null-rate, or row-count band).
    """
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
    if program == "ep":  # scrape-date vintages need a storage schema decision (ARCH §12)
        raise ValidationError("ep fetch is not wired to the vintage store")
    from bls_stats.engines.labstat import fetch

    periods: list[Period] = [
        (r.year, r.month if spec.frequency == Frequency.MONTHLY else (r.month + 2) // 3)
        for r in refs
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
    """ARCH §7.3: most recent ingested row_count for the same program and slot type.

    Prefers a same-revision comparator; falls back to the latest ingested row for the program
    regardless of revision (e.g. a backfill baseline with null revision) so the first live
    increment after a backfill is still row-band-checked rather than ungated (C-16)."""
    ingested = ledger.resolved().filter(
        (pl.col("program") == program) & (pl.col("status") == "ingested")
    )
    same = ingested.filter(pl.col("revision").eq_missing(pl.lit(revision, dtype=pl.Int16))).sort(
        "ingested_at", descending=True
    )
    if same.height:
        return int(same["row_count"][0])
    any_row = ingested.sort("ingested_at", descending=True)
    return int(any_row["row_count"][0]) if any_row.height else None


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
    """Daily incremental ingest: detect → expand → fetch → validate → commit → record.

    The one daily crontab line (ARCH §8). Polls each program's release feed, expands new
    releases into slots via `profiles.expand`, and drives each event through
    `_process_event`. An event is skipped entirely if every one of its slots already
    resolves to `"ingested"` or `"missed"` in the ledger — the anti-join that makes
    re-running the whole ingest idempotent and safe as a dumb cron (ARCH §5.1, §7.2).
    After an event that made forward progress (`_process_event` returns `"ok"`), any
    older `deferred` slot for the same program that the new release's date has now
    superseded is flipped to `"missed"` (ARCH §5.3) — never by wall-clock timeout.

    Crash safety: for each slot, data is appended to the Delta table before the ledger
    records it, so the only crash-inconsistent state is "committed, not yet recorded" —
    the safe direction. On re-run, `store.slot_exists` detects the already-committed slot
    (null-safe on the backfill counters) and `_process_event` repairs the ledger without
    re-appending (ARCH §7.2).

    Args:
        settings: Environment-derived config used to build the HTTP client.
        store: Vintage store backend (Delta or the Parquet escape hatch).
        programs: Programs to poll; defaults to every registry program except `ep`,
            which is not yet wired to the vintage store (ARCH §12). Passing `["ep"]`
            explicitly (or an all-`ep` list) logs an error and returns `2` rather than
            silently no-opping.
        dry_run: If `True`, run the full pipeline including validation but skip both the
            Delta append and the ledger write — nothing durable changes.
        clock: Injected wall clock for `downloaded`/`ingested_at` timestamps; defaults to
            `datetime.now(UTC)`. Tests pass a fixed clock so multi-run scenarios can
            control ledger ordering deterministically.
        poll_fn (Callable | None): Injected replacement for `feeds.poll(client, programs)`;
            defaults to the live Atom-feed poller. Tests substitute a canned list of
            `Release` events.
        fetch_fn (Callable | None): Injected replacement for the per-event fetch dispatch
            (default `_fetch_event`); tests substitute a fake that returns a fixed frame
            or raises.
        fresh_fn (Callable | None): Injected replacement for the stale-file guard (default
            `engines.labstat.is_fresh`); tests substitute a fixed freshness verdict to
            exercise the deferral path without live `Last-Modified` checks.

    Returns:
        Exit code per ARCH §7.4: `0` if there were no new events, or every event that ran
        ended in `"ok"` or `"deferred"` (a benign lag — a stale upstream file or an empty
        slice — never pages ops). `1` if at least one event `"failed"` but not all of them
        did, or any event was `"partial"` (it raised after already appending data for some
        of its slots — the commit happened, so this is not a clean failure and must not be
        silently reported as success). `2` if every event that ran `"failed"` outright.
    """
    clock = clock or _utcnow
    programs = programs or [p for p in REGISTRY if p != "ep"]  # ep: ARCH §5.2 exception
    if "ep" in programs:
        log.error("ep ingest is not yet wired to the vintage store (ARCH §12 open item)")
        programs = [p for p in programs if p != "ep"]
        if not programs:
            return 2
    poll_fn = poll_fn or feeds.poll
    fetch_fn = fetch_fn or _fetch_event
    if fresh_fn is None:
        from bls_stats.engines.labstat import is_fresh as fresh_fn  # noqa: PLW0127

    client = build_client(settings)
    ledger = Ledger(store)
    outcomes: list[str] = []
    releases = list(poll_fn(client, programs))
    newest_in_batch: dict[str, date] = {}
    for r in releases:
        d = newest_in_batch.get(r.program)
        if d is None or r.release_date > d:
            newest_in_batch[r.program] = r.release_date
    resolved = ledger.resolved()

    def _is_backdated(r) -> bool:
        # A strictly-newer release for this program in the batch, or already ingested,
        # means r's live-vintage window has closed — the current file no longer reflects
        # what r published, so fetching it would fabricate a print (ARCH §2.1 / C-14).
        if r.release_date < newest_in_batch[r.program]:
            return True
        ingested = resolved.filter(
            (pl.col("program") == r.program) & (pl.col("status") == "ingested")
        )
        latest = ingested["release_date"].max()
        return latest is not None and r.release_date < latest

    for release in releases:
        if _is_backdated(release):
            # Record only not-yet-resolved slots as missed. Never downgrade an already-
            # ingested slot: a correct live-vintage print stays ingested forever (ARCH §5.3,
            # _expire_superseded only expires deferred slots), and re-polling a back-dated
            # release must be idempotent — the same guard the live branch below uses.
            missed_slots = [
                s
                for s in expand(
                    release,
                    lambda rd, program=release.program, before=release.release_date: (
                        ledger.prior_benchmark_count(program, rd, before_release=before)
                    ),
                )
                if ledger.slot_status(
                    release.program, s.ref_date, release.release_date, s.revision, s.benchmark
                )
                not in ("ingested", "missed")
            ]
            if missed_slots and not dry_run:
                ledger.record([
                    SlotRecord(release.program, s.ref_date, release.release_date,
                               s.revision, s.benchmark, "increment", 0, "missed", clock())
                    for s in missed_slots
                ])
            log.warning(
                "%s release %s is back-dated (a newer release exists) — not fetched; "
                "recorded %d new slot(s) missed, use `backfill` for history (C-14)",
                release.program, release.release_date, len(missed_slots),
            )
            continue
        slots = [
            s
            for s in expand(
                release,
                lambda rd, program=release.program, before=release.release_date: (
                    ledger.prior_benchmark_count(program, rd, before_release=before)
                ),
            )
            if ledger.slot_status(
                release.program, s.ref_date, release.release_date, s.revision, s.benchmark
            )
            not in ("ingested", "missed")
        ]
        if not slots:
            continue
        outcome = _process_event(
            release, slots, settings, store, ledger, client,
            dry_run=dry_run, now=clock(), fetch_fn=fetch_fn, fresh_fn=fresh_fn,
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
    """Run one release event through fetch → validate → commit → record.

    Returns one of four outcome strings that `run_ingest` aggregates into its exit code
    (ARCH §7.4): `"ok"` (at least one slot committed or ledger-repaired), `"deferred"`
    (every slot deferred — stale upstream file or an empty slice — nothing committed),
    `"partial"` (an exception was raised after this event had already appended data for
    at least one slot), or `"failed"` (an exception was raised with no prior append).
    """
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
        # QCEW/OEWS/EP are freshness_checked=False: their sources don't serve a LABSTAT-style
        # Last-Modified the flat-file probe understands. QCEW's newest quarter is still guarded
        # by the year_to_date empty-slice deferral; a QCEW-native freshness probe is future work.
        if spec.freshness_checked and not fresh_fn(client, program, release.release_date):
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
    """Stage-1 historical seed: fetch, stamp, and commit one program's published history.

    Resolves `start`/`end` to concrete periods via `reference_periods` and
    `filter_published` (which needs the `release_calendar` state table already built —
    ARCH §5.4), fetches them in one event, and stamps each row as a snapshot-date vintage:
    `release_date` = the run's snapshot date and `revision`/`benchmark` = `null` — print
    history that predates this pipeline was never observed, so it is not fabricated (the
    ARCH §4.3 backfill honesty rule). Idempotent the same way as `run_ingest`: periods
    already `"ingested"` in the ledger for this snapshot are skipped, and `store.slot_exists`
    (null-safe on the null counters) guards against a duplicate append on re-run after a
    crash between commit and record.

    Args:
        settings: Environment-derived config used to build the HTTP client.
        store: Vintage store backend (Delta or the Parquet escape hatch).
        program: Registry program key. `"ep"` is rejected — EP has no reference periods
            to backfill (it is snapshot/cycle-based, not periodic).
        start: Range start as `YYYY/MM`, `YYYY/Q`, or `YYYY`, per the program's frequency.
        end: Range end, same format as `start`.
        dry_run: If `True`, fetch and validate but skip both the Delta append and the
            ledger write.
        clock: Injected wall clock for `downloaded`/`ingested_at` and the snapshot date
            itself; defaults to `datetime.now(UTC)`.
        fetch_fn (Callable | None): Injected replacement for the fetch dispatch (default
            `_fetch_event`); tests substitute a fixed frame.

    Returns:
        `0` on success, including the no-op cases of an already-complete backfill or a
        range with no published periods. `2` if `program` is `"ep"`, the release calendar
        hasn't been built yet, `start`/`end` fail to parse as period strings, or the fetch
        step raises. There is no `1` (partial) outcome — a single event covers the whole
        range, so a failure here has no partially-successful state to distinguish.
    """
    clock = clock or _utcnow
    now = clock()
    if program == "ep":
        log.error("ep is snapshot-based (no reference periods); backfill does not apply")
        return 2
    snapshot_date = now.date()
    cal = store.read_state("release_calendar")
    if cal is None:
        log.error("release calendar missing — run `bls-stats calendar build` first (ARCH §8)")
        return 2
    try:
        periods = filter_published(program, reference_periods(program, start, end), cal)
    except ValueError as exc:  # PeriodError subclasses ValueError; also empty program calendar
        log.error("%s: %s", program, exc)
        return 2
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
