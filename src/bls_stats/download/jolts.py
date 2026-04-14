"""JOLTS (Job Openings and Labor Turnover Survey) downloader — bulk flat file.

Downloads the complete JT data file from the BLS FTP server and filters
to the requested periods.
"""

from __future__ import annotations

import calendar
import logging
from datetime import date, datetime
from pathlib import Path

import polars as pl

from bls_stats.config import JOLTS_DIR, JOLTS_ESTIMATES_FILE
from bls_stats.download.fetch import read_tsv

logger = logging.getLogger(__name__)

JOLTS_DATA_URL = "https://download.bls.gov/pub/time.series/jt/jt.data.0.Current"


def _last_business_day(year: int, month: int) -> date:
    """Return the last weekday (Mon-Fri) of the given year/month."""
    last_day = calendar.monthrange(year, month)[1]
    d = date(year, month, last_day)
    while d.weekday() >= 5:
        d = d.replace(day=d.day - 1)
    return d


def _period_to_month(period: str) -> int | None:
    if not period.startswith("M"):
        return None
    try:
        m = int(period[1:])
    except ValueError:
        return None
    return m if 1 <= m <= 12 else None


def _filter_to_periods(
    df: pl.DataFrame, periods: set[tuple[int, int]]
) -> pl.DataFrame:
    df = df.with_columns(
        pl.col("period").map_elements(_period_to_month, return_dtype=pl.Int32).alias("_month")
    )
    df = df.filter(pl.col("_month").is_not_null())
    df = df.filter(
        pl.struct(["year", "_month"]).map_elements(
            lambda s: (int(s["year"]), int(s["_month"])) in periods,
            return_dtype=pl.Boolean,
        )
    )
    df = df.with_columns(
        pl.struct(["year", "_month"])
        .map_elements(
            lambda s: _last_business_day(int(s["year"]), int(s["_month"])),
            return_dtype=pl.Date,
        )
        .alias("ref_date")
    )
    return df.drop("_month")


def download_jolts(
    periods: list[tuple[int, int]],
    out_dir: Path | None = None,
) -> pl.DataFrame:
    """Download JOLTS data for the requested (year, month) periods."""
    out_dir = out_dir or JOLTS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info("JOLTS: downloading flat file")
    df = read_tsv(JOLTS_DATA_URL)
    logger.info("JOLTS: %d total rows downloaded", len(df))

    df = df.with_columns(pl.col("year").cast(pl.Int32))
    df = _filter_to_periods(df, set(periods))

    df = df.with_columns(pl.lit(datetime.now()).alias("downloaded"))

    out_path = out_dir / JOLTS_ESTIMATES_FILE
    df.write_parquet(out_path)
    logger.info("JOLTS: wrote %d rows to %s", len(df), out_path)
    return df
