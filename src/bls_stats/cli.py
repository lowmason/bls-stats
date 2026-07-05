"""typer CLI (ARCH §8). Thin adapters only — logic lives in pipeline/releases/storage.

Commands parse options, call into `bls_stats.pipeline`, `bls_stats.releases`, or
`bls_stats.storage`, and translate the result into stdout/stderr text and a process exit
code. No orchestration, validation, or storage logic lives in this module.
"""

from __future__ import annotations

import logging
import sys
from datetime import date

import typer

from bls_stats.core.config import load_settings, storage_options
from bls_stats.registry import REGISTRY

app = typer.Typer(help="Vintage-aware BLS data downloads and ingest.")
calendar_app = typer.Typer(help="Release-date calendar.")
store_app = typer.Typer(help="Inspect and maintain the vintage store.")
metadata_app = typer.Typer(help="CPS dimension tables.")
app.add_typer(calendar_app, name="calendar")
app.add_typer(store_app, name="store")
app.add_typer(metadata_app, name="metadata")

PROGRAMS = list(REGISTRY)


def _require_program(program: str) -> None:
    if program not in REGISTRY:
        typer.echo(f"unknown program {program!r} — choose from {PROGRAMS}", err=True)
        raise typer.Exit(2)


def _parse_date(value: str, label: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        typer.echo(f"{label}: invalid date {value!r} (expected YYYY-MM-DD)", err=True)
        raise typer.Exit(2) from None


def _setup() -> tuple:
    settings = load_settings()
    level = logging.getLevelNamesMapping().get(settings.log_level.upper())
    if level is None:
        typer.echo(f"unknown BLS_LOG_LEVEL {settings.log_level!r} — using INFO", err=True)
        level = logging.INFO
    logging.basicConfig(
        stream=sys.stderr,
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    from bls_stats.storage.delta import VintageStore

    return settings, VintageStore(settings.store_uri, storage_options(settings))


@app.command()
def ingest(
    program: str | None = typer.Option(None, help="One program; default: all feed-driven."),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    """Daily incremental ingest — the one daily crontab line (ARCH §8).

    Exit code is `bls_stats.pipeline.run_ingest`'s return value, unchanged: `0` on
    success or deferrals-only, `1` on partial failure, `2` if every event failed.
    """
    import bls_stats.pipeline as pipeline

    settings, store = _setup()
    if program is not None:
        _require_program(program)
    programs = [program] if program else None
    raise typer.Exit(pipeline.run_ingest(settings, store, programs, dry_run=dry_run))


@app.command()
def backfill(
    program: str = typer.Option(...),
    start: str = typer.Option(..., help="YYYY/MM, YYYY/Q, or YYYY per program frequency"),
    end: str = typer.Option(...),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    """Stage-1 historical seed (ARCH §8). QCEW runs per year for memory discipline.

    For `qcew`, `start`/`end` are parsed to whole calendar years and each year is
    backfilled as its own `run_backfill` call (one year of quarters at a time, to keep
    peak memory bounded); the exit code is the worst (`max`) of the per-year codes. Every
    other program runs as a single `run_backfill` call. Exit code is `0` on success
    (including an already-complete or empty range) or `2` on a bad period string, an
    unbuilt release calendar, `program="ep"`, or a fetch failure — propagated unchanged
    from `bls_stats.pipeline.run_backfill`.
    """
    import bls_stats.pipeline as pipeline
    from bls_stats.core.periods import reference_periods

    settings, store = _setup()
    _require_program(program)
    if program == "qcew":
        try:
            years = sorted({y for y, _ in reference_periods("qcew", start, end)})
        except ValueError as exc:  # PeriodError subclasses ValueError
            typer.echo(f"qcew: {exc}", err=True)
            raise typer.Exit(2) from None
        codes = [
            pipeline.run_backfill(settings, store, "qcew", f"{y}/1", f"{y}/4", dry_run=dry_run)
            for y in years
        ]
        raise typer.Exit(max(codes))
    raise typer.Exit(pipeline.run_backfill(settings, store, program, start, end, dry_run=dry_run))


@calendar_app.command("build")
def calendar_build() -> None:
    """Scrape and append a full archive+schedule calendar with lapse overlay (ARCH §5.4).

    Appends the freshly scraped rows to the `release_calendar` state table (it does not
    replace it — downstream reads dedupe on `(program, ref_date, release_date)`). Run once to
    bootstrap before the first `backfill`; `calendar refresh` is the cheap day-to-day update.
    Always exits `0`.
    """
    from bls_stats.core.http import build_client
    from bls_stats.releases.calendar import build

    settings, store = _setup()
    cal = build(build_client(settings), [p for p in PROGRAMS if p != "ep"])
    store.append_state("release_calendar", cal)
    typer.echo(f"calendar: {cal.height} rows")


@calendar_app.command("refresh")
def calendar_refresh() -> None:
    """Cheap keep-current poll from the feeds.

    Appends the current feed state (up to 12 entries per program, ARCH §5.2) to the
    `release_calendar` table rather than rebuilding it — much cheaper than `calendar
    build`, suitable for frequent runs. Always exits `0`.
    """
    import polars as pl

    from bls_stats.core.http import build_client
    from bls_stats.core.periods import ref_date as _rd
    from bls_stats.releases.calendar import CALENDAR_SCHEMA
    from bls_stats.releases.feeds import poll

    settings, store = _setup()
    releases = poll(build_client(settings), [p for p in PROGRAMS if p != "ep"])
    rows = [
        {
            "program": r.program,
            "ref_date": _rd(r.program, r.ref_year, r.ref_period),
            "release_date": r.release_date,
            "original_release": None,
            "is_benchmark": r.is_benchmark,
        }
        for r in releases
    ]
    store.append_state("release_calendar", pl.DataFrame(rows, schema=CALENDAR_SCHEMA))
    typer.echo(f"calendar: appended {len(rows)} rows from feeds")


@calendar_app.command("show")
def calendar_show(program: str = typer.Option(...)) -> None:
    """Print one program's release-calendar rows, sorted by `ref_date`.

    Exits `1` if the `release_calendar` state table hasn't been built yet (run
    `calendar build` first); `0` otherwise, even if `program` has no rows.
    """
    import polars as pl

    _, store = _setup()
    _require_program(program)
    cal = store.read_state("release_calendar")
    if cal is None:
        typer.echo("no calendar — run `bls-stats calendar build`", err=True)
        raise typer.Exit(1)
    typer.echo(str(cal.filter(pl.col("program") == program).sort("ref_date")))


@app.command()
def gaps(
    program: str | None = typer.Option(None),
    strict: bool = typer.Option(False, "--strict", help="missed prints also exit non-zero"),
    as_of_date: str | None = typer.Option(
        None, "--as-of-date", help="audit expected releases published on/before this YYYY-MM-DD"
    ),
) -> None:
    """Audit expected releases (from the calendar) against the ledger (ARCH §5.3, §8).

    A calendar row with a non-null `release_date` at or before the reference date is an
    *expected* release. If the ledger has no row for its `(program, ref_date)` (of any status),
    it is *unexplained* — a release the pipeline never recorded. Recorded `missed`/`deferred`
    slots are *acknowledged*: printed but not failing, unless `--strict` (which also fails on
    `missed`). The reference date defaults to the latest published `release_date` in the
    calendar; `--as-of-date` overrides it. Exits `1` on any unexplained gap, on a `--strict`
    missed slot, or if the calendar is unbuilt; `0` otherwise.
    """
    import polars as pl

    from bls_stats.vintage.ledger import Ledger

    _, store = _setup()
    if program:
        _require_program(program)
    cal = store.read_state("release_calendar")
    if cal is None:
        typer.echo("no calendar — run `bls-stats calendar build`", err=True)
        raise typer.Exit(1)
    ledger = Ledger(store).resolved()
    if program:
        cal = cal.filter(pl.col("program") == program)
        ledger = ledger.filter(pl.col("program") == program)
    published = cal.filter(pl.col("release_date").is_not_null())
    ref = (
        _parse_date(as_of_date, "--as-of-date")
        if as_of_date
        else (published["release_date"].max() if published.height else None)
    )
    expected = (
        published.filter(pl.col("release_date") <= ref).select("program", "ref_date").unique()
        if ref is not None
        else published.select("program", "ref_date").unique()
    )
    recorded = ledger.select("program", "ref_date").unique()
    unexplained = expected.join(recorded, on=["program", "ref_date"], how="anti")
    acknowledged = ledger.filter(pl.col("status").is_in(["missed", "deferred"]))
    typer.echo(f"unexplained: {unexplained.height}  acknowledged: {acknowledged.height}")
    if unexplained.height:
        typer.echo(str(unexplained.sort("program", "ref_date")))
    missed = acknowledged.filter(pl.col("status") == "missed")
    raise typer.Exit(1 if (unexplained.height or (strict and missed.height)) else 0)


@store_app.command("info")
def store_info(program: str | None = typer.Option(None)) -> None:
    """Print row count and vintage span (min/max `release_date`) per program.

    Reports `(empty)` for a program with no observations table yet rather than erroring.
    Always exits `0`.
    """
    import polars as pl

    _, store = _setup()
    for name in [program] if program else PROGRAMS:
        lf = store.scan_observations(name)
        if lf is None:
            typer.echo(f"{name}: (empty)")
            continue
        summary = lf.select(
            pl.len().alias("rows"),
            pl.col("release_date").min().alias("first_vintage"),
            pl.col("release_date").max().alias("latest_vintage"),
        ).collect()
        typer.echo(f"{name}: {summary.row(0)}")


@store_app.command("maintain")
def store_maintain() -> None:
    """Delta optimize/compact + vacuum — the weekly crontab line (ARCH §4.1).

    Skips programs with no observations table. Vacuum retention is 7 days, so it cannot
    remove a file still referenced by any live vintage. Always exits `0`.
    """
    from deltalake import DeltaTable

    _, store = _setup()
    for name in PROGRAMS:
        if store.scan_observations(name) is None:
            continue
        dt = DeltaTable(store.observations_uri(name), storage_options=store.storage_options)
        dt.optimize.compact()
        dt.vacuum(retention_hours=24 * 7, enforce_retention_duration=True, dry_run=False)
        typer.echo(f"{name}: optimized + vacuumed")


@store_app.command("query")
def store_query(
    program: str = typer.Option(...),
    ref_date: str = typer.Option(..., help="YYYY-MM-DD"),
    as_of: str | None = typer.Option(None, help="YYYY-MM-DD point-in-time (inclusive)"),
    all_vintages: bool = typer.Option(False, "--all-vintages"),
) -> None:
    """Vintage-aware read for one program/`ref_date`: latest, as-of, or full history.

    Default (neither flag) prints one row per unit — the latest print, using the §4.4
    tie-break. `--as-of D` restricts to vintages with `release_date <= D` (inclusive)
    before taking the latest, guaranteeing no future vintage leaks into a point-in-time
    read. `--all-vintages` dumps every print for the `ref_date`, sorted oldest to newest.

    Exits `1` if the program has no observations table yet; `0` otherwise.
    """
    import polars as pl

    from bls_stats.storage.reads import as_of as as_of_read
    from bls_stats.storage.reads import latest

    _, store = _setup()
    _require_program(program)
    lf = store.scan_observations(program)
    if lf is None:
        typer.echo(f"{program}: (empty)", err=True)
        raise typer.Exit(1)
    lf = lf.filter(pl.col("ref_date") == _parse_date(ref_date, "--ref-date"))
    units = list(REGISTRY[program].unit_columns)
    if all_vintages:
        out = lf.sort("release_date").collect()
    elif as_of:
        out = as_of_read(lf, units, _parse_date(as_of, "--as-of")).collect()
    else:
        out = latest(lf, units).collect()
    typer.echo(str(out))


@metadata_app.command("fetch")
def metadata_fetch(refresh: bool = typer.Option(False)) -> None:
    """Download and cache the CPS dimension tables (series catalog + `ln.*` mappings).

    Cached under `settings.metadata_cache_dir` (env `BLS_METADATA_CACHE`, default
    `data/cps_metadata`) with an integrity manifest; `--refresh` forces a re-download
    even if the cache looks valid. Always exits `0`.
    """
    from pathlib import Path

    from bls_stats.core.http import build_client
    from bls_stats.enrich.cps import fetch_metadata

    settings, _ = _setup()
    meta = fetch_metadata(
        build_client(settings), Path(settings.metadata_cache_dir), refresh=refresh
    )
    typer.echo(f"fetched {len(meta)} metadata tables")


@metadata_app.command("export")
def metadata_export() -> None:
    """Push the (fetched/cached) CPS dimension tables into the store's metadata tables.

    Snapshot-replaces `cps/metadata/series` and `cps/metadata/mappings/{name}` with a
    fresh `downloaded` timestamp; these tables carry no vintage columns (ARCH §8).
    Fetches from cache first (see `metadata fetch`). Always exits `0`.
    """
    from pathlib import Path

    from bls_stats.core.http import build_client
    from bls_stats.enrich.cps import export_metadata, fetch_metadata

    settings, store = _setup()
    meta = fetch_metadata(build_client(settings), Path(settings.metadata_cache_dir))
    export_metadata(store, meta)
    typer.echo("exported")


@metadata_app.command("enrich")
def metadata_enrich(ref_date_opt: str = typer.Option(..., "--ref-date")) -> None:
    """Full BEH §2.5 enrichment of one CPS slice — spot-check view.

    Left-joins every stored vintage's CPS observations for `--ref-date` (unfiltered by
    `release_date` — this is a raw inspection tool, not a vintage-aware read) against the
    series catalog and all `ln.*` mapping tables, resolving footnote codes, and prints
    the result. Exits `1` if the `cps` observations table is empty; `0` otherwise.
    """
    from pathlib import Path

    import polars as pl

    from bls_stats.core.http import build_client
    from bls_stats.enrich.cps import enrich, fetch_metadata

    settings, store = _setup()
    lf = store.scan_observations("cps")
    if lf is None:
        typer.echo("cps: (empty)", err=True)
        raise typer.Exit(1)
    obs = lf.filter(pl.col("ref_date") == _parse_date(ref_date_opt, "--ref-date")).collect()
    meta = fetch_metadata(build_client(settings), Path(settings.metadata_cache_dir))
    typer.echo(str(enrich(obs, meta)))


@app.command()
def doctor() -> None:
    """Pre-flight probes (ARCH §8): green/red checklist; non-zero exit on any failure.

    Runs `storage.doctor.run_all` — store reachability, the conditional-PUT probe that
    selects Delta commit-safety mode, delta-rs availability, BLS reachability under the
    configured User-Agent, and presence/validity of contact-email and API-key
    configuration — and prints one line per check. Exits `1` if any check fails, `0` if
    every check passes.
    """
    from bls_stats.storage.doctor import run_all

    settings, _ = _setup()
    results = run_all(settings)
    for r in results:
        mark = "!" if r.warn else ("✓" if r.ok else "✗")
        typer.echo(f"{mark} {r.name}: {r.detail}")
    raise typer.Exit(0 if all(r.ok for r in results) else 1)
