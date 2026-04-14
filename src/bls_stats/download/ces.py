"""CES (Current Employment Statistics) downloader — bulk flat file.

Downloads the complete CES data file from the BLS FTP server and filters
to the requested periods.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path

import polars as pl

from bls_stats.config import CES_DIR, CES_ESTIMATES_FILE
from bls_stats.download.fetch import read_tsv

logger = logging.getLogger(__name__)

CES_DATA_URL = "https://download.bls.gov/pub/time.series/ce/ce.data.0.AllCESSeries"


def _period_to_month(period: str) -> int | None:
    """Convert BLS period code (M01-M12) to month number. M13=annual avg."""
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
        pl.col("period").map_elements(_period_to_month, return_dtype=pl.Int32).alias("month")
    )
    df = df.filter(pl.col("month").is_not_null())
    df = df.filter(
        pl.struct(["year", "month"]).map_elements(
            lambda s: (int(s["year"]), int(s["month"])) in periods,
            return_dtype=pl.Boolean,
        )
    )
    return df.with_columns(
        pl.struct(["year", "month"])
        .map_elements(
            lambda s: date(int(s["year"]), int(s["month"]), 12),
            return_dtype=pl.Date,
        )
        .alias("ref_date")
    )


def download_ces(
    periods: list[tuple[int, int]],
    out_dir: Path | None = None,
) -> pl.DataFrame:
    """Download CES data for the requested (year, month) periods."""
    out_dir = out_dir or CES_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info("CES: downloading flat file")
    df = read_tsv(CES_DATA_URL)
    logger.info("CES: %d total rows downloaded", len(df))

    df = df.with_columns(pl.col("year").cast(pl.Int32))
    df = _filter_to_periods(df, set(periods))

    df = df.with_columns(pl.lit(datetime.now()).alias("downloaded"))

    out_path = out_dir / CES_ESTIMATES_FILE
    df.write_parquet(out_path)
    logger.info("CES: wrote %d rows to %s", len(df), out_path)
    return df
