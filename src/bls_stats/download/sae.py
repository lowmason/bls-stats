"""SAE (State and Area Employment) downloader — bulk flat file.

Downloads the complete SM data file from the BLS FTP server and filters
to the requested periods.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path

import polars as pl

from bls_stats.config import SAE_DIR, SAE_ESTIMATES_FILE
from bls_stats.download.fetch import read_tsv

logger = logging.getLogger(__name__)

SAE_DATA_URL = "https://download.bls.gov/pub/time.series/sm/sm.data.0.Current"


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


def download_sae(
    periods: list[tuple[int, int]],
    out_dir: Path | None = None,
) -> pl.DataFrame:
    """Download SAE (State and Area Employment) data for the requested (year, month) periods."""
    out_dir = out_dir or SAE_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info("SAE: downloading flat file")
    df = read_tsv(SAE_DATA_URL)
    logger.info("SAE: %d total rows downloaded", len(df))

    df = df.with_columns(pl.col("year").cast(pl.Int32))
    df = _filter_to_periods(df, set(periods))

    df = df.with_columns(pl.lit(datetime.now()).alias("downloaded"))

    out_path = out_dir / SAE_ESTIMATES_FILE
    df.write_parquet(out_path)
    logger.info("SAE: wrote %d rows to %s", len(df), out_path)
    return df
