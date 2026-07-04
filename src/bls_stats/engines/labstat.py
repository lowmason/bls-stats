"""Flat-file engine for ces/sae/jolts/cps/bed (BEH §2.1, ARCH §6.2–§6.3)."""

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
    local = download(client, url, dest_dir / url.rsplit("/", 1)[-1])
    try:
        df = parse_flat_file(local, program, periods, downloaded=downloaded)
    finally:
        local.unlink(missing_ok=True)  # scratch discipline (ARCH §10)
    log.info("%s: parsed %d rows for %d period(s)", program, df.height, len(periods))
    return df


def embargo_utc(program: str, release_date: date) -> datetime:
    hh, mm = (REGISTRY[program].release_time_et or "08:30").split(":")
    return datetime.combine(release_date, time(int(hh), int(mm)), tzinfo=_ET).astimezone(
        ZoneInfo("UTC")
    )


def is_fresh(client: httpx.Client, program: str, release_date: date) -> bool:
    """ARCH §6.3 stale-file guard: Last-Modified ≥ scheduled embargo on the release date."""
    url = REGISTRY[program].increment_url
    assert url is not None
    last_modified = head_last_modified(client, url)
    if last_modified is None:
        log.warning("%s: no Last-Modified header — treating as stale", program)
        return False
    return last_modified >= embargo_utc(program, release_date)
