"""Flat-file engine for ces/sae/jolts/cps/bed (BEH §2.1, ARCH §6.2–§6.3).

These five LABSTAT programs share one tab-separated flat-file format, so one parser and one
freshness check serve all of them; per-program differences (URL, frequency, embargo time) come
from the registry (`bls_stats.registry.REGISTRY`), not from branching in this module.

Two format quirks drive the parsing choices below:

- LABSTAT headers are **space-padded** (e.g. `"series_id       "`), so columns must be renamed
  by stripped name rather than matched literally.
- `infer_schema=False` is required: BLS pads numeric-looking columns (`year`, `value`) with
  spaces too, so Polars' type sniffer would misread them or choke on the padding. Every column
  is read as `Utf8` and cast explicitly after stripping.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import polars as pl

from bls_stats.core.http import download, head_last_modified
from bls_stats.core.periods import Period, ref_date
from bls_stats.registry import REGISTRY, Frequency

log = logging.getLogger(__name__)
_ET = ZoneInfo("America/New_York")


def parse_flat_file(
    path: Path, program: str, periods: list[Period] | None = None, *, downloaded: datetime
) -> pl.DataFrame:
    """Parse a downloaded LABSTAT flat file into the observation contract for `program`.

    Reads the whole file as `Utf8` (headers space-stripped, values trimmed), drops the `M13`
    annual-average row that monthly programs carry (BEH §2.1), derives `ref_date` from
    `(year, period)` via the program's `RefDateRule`, and optionally semi-joins down to a
    caller-supplied period allowlist — the mechanism `fetch` uses to pull only the periods an
    increment event touches instead of the whole history.

    Args:
        path: Local path to the already-downloaded flat file (tab-separated, LABSTAT layout).
        program: Registry key selecting frequency, period regex, and `ref_date` rule — one of
            `ces`, `sae`, `jolts`, `cps`, `bed`.
        periods: `(year, period_number)` pairs to keep; `None` keeps every period in the file
            (the backfill case).
        downloaded: Wall-clock ingestion timestamp (injected, never `datetime.now()`); stamped
            onto every row as `downloaded`, cast to microsecond precision.

    Returns:
        A `pl.DataFrame` with columns `series_id` (`Utf8`), `value` (`Float64`, non-strict cast
        — malformed values become null rather than raising), `footnote_codes` (`Utf8`),
        `ref_date` (`Date`), and `downloaded` (`Datetime("us")`). `year`, `period`, and the
        internal `_pnum` helper column are dropped.
    """
    spec = REGISTRY[program]
    period_re = r"^M(0[1-9]|1[0-2])$" if spec.frequency == Frequency.MONTHLY else r"^Q0[1-4]$"
    lf = (
        pl.scan_csv(
            path,
            separator="\t",
            infer_schema=False,
            missing_utf8_is_empty_string=True,
        )
        .rename(lambda c: c.strip())  # LABSTAT headers are space-padded
        .with_columns(pl.col("series_id", "period", "value", "footnote_codes").str.strip_chars())
        .with_columns(pl.col("year").str.strip_chars().cast(pl.Int32))
        .filter(pl.col("period").str.contains(period_re))  # drops M13 (BEH §2.1)
        .with_columns(
            pl.col("period").str.slice(1).cast(pl.Int8).alias("_pnum"),
            pl.col("value").cast(pl.Float64, strict=False),
        )
    )
    if periods is not None:
        allowed = pl.DataFrame(
            {"year": [y for y, _ in periods], "_pnum": [p for _, p in periods]},
            schema={"year": pl.Int32, "_pnum": pl.Int8},
        ).lazy()
        lf = lf.join(allowed, on=["year", "_pnum"], how="semi")
    df = lf.collect(engine="streaming")
    ref_dates = pl.DataFrame(
        [
            {"year": y, "_pnum": p, "ref_date": ref_date(program, y, p)}
            for y, p in df.select("year", "_pnum").unique().iter_rows()
        ],
        schema={"year": pl.Int32, "_pnum": pl.Int8, "ref_date": pl.Date},
    )
    return (
        df.join(ref_dates, on=["year", "_pnum"], how="left")
        .drop("year", "period", "_pnum")
        .with_columns(pl.lit(downloaded).dt.cast_time_unit("us").alias("downloaded"))
    )


def fetch(
    client: httpx.Client,
    program: str,
    url: str,
    periods: list[Period],
    dest_dir: Path,
    downloaded: datetime,
) -> pl.DataFrame:
    """Download `url`, parse it to the requested periods, and remove the scratch file.

    Thin wrapper around `parse_flat_file` that adds the download step and the scratch-disk
    discipline (ARCH §10): the flat file is deleted as soon as parsing finishes (or raises),
    since these files can run into the hundreds of MB and nothing persists locally.

    Args:
        client: Shared `httpx.Client` (UA, retry/backoff policy from `core.http`).
        program: Registry key — see `parse_flat_file`.
        url: Source URL (backfill, increment, or benchmark URL from the registry).
        periods: `(year, period_number)` pairs to keep.
        dest_dir: Scratch directory for the downloaded file; not durable storage.
        downloaded: Wall-clock ingestion timestamp, forwarded to `parse_flat_file`.

    Returns:
        The parsed `pl.DataFrame`, per `parse_flat_file`.
    """
    local = download(client, url, dest_dir / url.rsplit("/", 1)[-1])
    try:
        df = parse_flat_file(local, program, periods, downloaded=downloaded)
    finally:
        local.unlink(missing_ok=True)  # scratch discipline (ARCH §10)
    log.info("%s: parsed %d rows for %d period(s)", program, df.height, len(periods))
    return df


def embargo_utc(program: str, release_date: date) -> datetime:
    """Compute the program's scheduled embargo instant for `release_date`, in UTC.

    BLS releases flip their flat files at a fixed local-time embargo (08:30 or 10:00 America/New
    York, per the registry's `release_time_et`). Converting through `America/New_York` rather
    than a fixed UTC offset keeps the instant correct across the EST/EDT transition.

    Args:
        program: Registry key supplying `release_time_et` (defaults to `"08:30"` if unset).
        release_date: Calendar date of the release, parsed from the archive link (ARCH §6.3) —
            not from feed timestamps, which are unreliable (ARCH §5.2).

    Returns:
        The embargo instant as a timezone-aware `datetime` in UTC.
    """
    hh, mm = (REGISTRY[program].release_time_et or "08:30").split(":")
    return datetime.combine(release_date, time(int(hh), int(mm)), tzinfo=_ET).astimezone(
        ZoneInfo("UTC")
    )


def is_fresh(client: httpx.Client, program: str, release_date: date) -> bool:
    """ARCH §6.3 stale-file guard: Last-Modified ≥ scheduled embargo on the release date.

    Guards against fetching a flat file before BLS has actually flipped it to the new vintage —
    a plain HTTP 200 proves nothing about *which* vintage came back. Callers that see `False`
    should retry with backoff and, after bounded attempts, record the event as `deferred`
    (ARCH §6.3, §5.3) rather than failing the run.

    Args:
        client: Shared `httpx.Client` used for the `HEAD` request.
        program: Registry key selecting the increment URL to probe.
        release_date: Calendar date the release was expected to land.

    Returns:
        `True` if the file's `Last-Modified` is at or after the computed embargo instant.
        `False` if the file is older, or if the server sent no `Last-Modified` header at all
        (treated as stale rather than assumed fresh — logged at WARNING).
    """
    url = REGISTRY[program].increment_url
    assert url is not None
    last_modified = head_last_modified(client, url)
    if last_modified is None:
        log.warning("%s: no Last-Modified header — treating as stale", program)
        return False
    return last_modified >= embargo_utc(program, release_date)
