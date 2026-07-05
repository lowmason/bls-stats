"""typer CLI (ARCH §8). Thin adapters only — logic lives in pipeline/releases/storage."""

from __future__ import annotations

import logging
import sys
from datetime import date

import typer

from bls_stats.core.config import load_settings, storage_options

app = typer.Typer(help="Vintage-aware BLS data downloads and ingest.")
calendar_app = typer.Typer(help="Release-date calendar.")
store_app = typer.Typer(help="Inspect and maintain the vintage store.")
metadata_app = typer.Typer(help="CPS dimension tables.")
app.add_typer(calendar_app, name="calendar")
app.add_typer(store_app, name="store")
app.add_typer(metadata_app, name="metadata")

PROGRAMS = ["ces", "sae", "jolts", "cps", "bed", "qcew", "oews", "ep"]


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
    """Daily incremental ingest — the one daily crontab line (ARCH §8)."""
    import bls_stats.pipeline as pipeline

    settings, store = _setup()
    programs = [program] if program else None
    raise typer.Exit(pipeline.run_ingest(settings, store, programs, dry_run=dry_run))


@app.command()
def backfill(
    program: str = typer.Option(...),
    start: str = typer.Option(..., help="YYYY/MM, YYYY/Q, or YYYY per program frequency"),
    end: str = typer.Option(...),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    """Stage-1 historical seed (ARCH §8). QCEW runs per year for memory discipline."""
    import bls_stats.pipeline as pipeline
    from bls_stats.core.periods import reference_periods

    settings, store = _setup()
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
    """Full archive+schedule scrape with lapse overlay (ARCH §5.4)."""
    from bls_stats.core.http import build_client
    from bls_stats.releases.calendar import build

    settings, store = _setup()
    cal = build(build_client(settings), [p for p in PROGRAMS if p != "ep"])
    store.append_state("release_calendar", cal)
    typer.echo(f"calendar: {cal.height} rows")


@calendar_app.command("refresh")
def calendar_refresh() -> None:
    """Cheap keep-current poll from the feeds."""
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
    import polars as pl

    _, store = _setup()
    cal = store.read_state("release_calendar")
    if cal is None:
        typer.echo("no calendar — run `bls-stats calendar build`", err=True)
        raise typer.Exit(1)
    typer.echo(str(cal.filter(pl.col("program") == program).sort("ref_date")))


@app.command()
def gaps(
    program: str | None = typer.Option(None),
    strict: bool = typer.Option(False, "--strict", help="missed prints also exit non-zero"),
) -> None:
    """Unexplained gaps exit non-zero; recorded missed/deferred are acknowledged (ARCH §8)."""
    import polars as pl

    from bls_stats.releases.calendar import find_gaps
    from bls_stats.vintage.ledger import Ledger

    _, store = _setup()
    cal = store.read_state("release_calendar")
    if cal is None:
        typer.echo("no calendar — run `bls-stats calendar build`", err=True)
        raise typer.Exit(1)
    if program:
        cal = cal.filter(pl.col("program") == program)
    calendar_gaps = find_gaps(cal)
    ledger = Ledger(store).resolved()
    acknowledged = ledger.filter(pl.col("status").is_in(["missed", "deferred"]))
    unexplained = calendar_gaps.join(
        ledger.select("program", "ref_date").unique(), on=["program", "ref_date"], how="anti"
    )
    typer.echo(f"unexplained: {unexplained.height}  acknowledged: {acknowledged.height}")
    if unexplained.height:
        typer.echo(str(unexplained))
    missed = acknowledged.filter(pl.col("status") == "missed")
    raise typer.Exit(1 if (unexplained.height or (strict and missed.height)) else 0)


@store_app.command("info")
def store_info(program: str | None = typer.Option(None)) -> None:
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
    """Delta optimize/compact + vacuum — the weekly crontab line (ARCH §4.1)."""
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
    import polars as pl

    from bls_stats.registry import REGISTRY
    from bls_stats.storage.reads import as_of as as_of_read
    from bls_stats.storage.reads import latest

    _, store = _setup()
    lf = store.scan_observations(program)
    if lf is None:
        typer.echo(f"{program}: (empty)", err=True)
        raise typer.Exit(1)
    lf = lf.filter(pl.col("ref_date") == date.fromisoformat(ref_date))
    units = list(REGISTRY[program].unit_columns)
    if all_vintages:
        out = lf.sort("release_date").collect()
    elif as_of:
        out = as_of_read(lf, units, date.fromisoformat(as_of)).collect()
    else:
        out = latest(lf, units).collect()
    typer.echo(str(out))


@metadata_app.command("fetch")
def metadata_fetch(refresh: bool = typer.Option(False)) -> None:
    from pathlib import Path

    from bls_stats.core.http import build_client
    from bls_stats.enrich.cps import fetch_metadata

    settings, _ = _setup()
    meta = fetch_metadata(build_client(settings), Path("data/cps_metadata"), refresh=refresh)
    typer.echo(f"fetched {len(meta)} metadata tables")


@metadata_app.command("export")
def metadata_export() -> None:
    from pathlib import Path

    from bls_stats.core.http import build_client
    from bls_stats.enrich.cps import export_metadata, fetch_metadata

    settings, store = _setup()
    meta = fetch_metadata(build_client(settings), Path("data/cps_metadata"))
    export_metadata(store, meta)
    typer.echo("exported")


@metadata_app.command("enrich")
def metadata_enrich(ref_date_opt: str = typer.Option(..., "--ref-date")) -> None:
    """Full BEH §2.5 enrichment of one CPS slice — spot-check view."""
    from pathlib import Path

    import polars as pl

    from bls_stats.core.http import build_client
    from bls_stats.enrich.cps import enrich, fetch_metadata

    settings, store = _setup()
    lf = store.scan_observations("cps")
    if lf is None:
        typer.echo("cps: (empty)", err=True)
        raise typer.Exit(1)
    obs = lf.filter(pl.col("ref_date") == date.fromisoformat(ref_date_opt)).collect()
    meta = fetch_metadata(build_client(settings), Path("data/cps_metadata"))
    typer.echo(str(enrich(obs, meta)))


@app.command()
def doctor() -> None:
    """Pre-flight probes (ARCH §8): green/red checklist; non-zero exit on any failure."""
    from bls_stats.storage.doctor import run_all

    settings, _ = _setup()
    results = run_all(settings)
    for r in results:
        mark = "✓" if r.ok else "✗"
        typer.echo(f"{mark} {r.name}: {r.detail}")
    raise typer.Exit(0 if all(r.ok for r in results) else 1)
