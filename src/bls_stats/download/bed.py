"""BED (Business Employment Dynamics) downloader — bulk flat file.

Downloads the complete BD data file from the BLS FTP server and filters
to the requested periods.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path

import polars as pl

from bls_stats.config import BED_DIR, BED_ESTIMATES_FILE
from bls_stats.download.fetch import read_tsv

logger = logging.getLogger(__name__)

BED_DATA_URL = "https://download.bls.gov/pub/time.series/bd/bd.data.0.Current"

_QUARTER_LAST_MONTH = {1: 3, 2: 6, 3: 9, 4: 12}


def _period_to_quarter(period: str) -> int | None:
    """Convert BLS period code (Q01-Q04) to quarter int."""
    if not period.startswith("Q"):
        return None
    try:
        q = int(period[1:])
    except ValueError:
        return None
    return q if 1 <= q <= 4 else None


def _filter_to_periods(
    df: pl.DataFrame, periods: set[tuple[int, int]]
) -> pl.DataFrame:
    df = df.with_columns(
        pl.col("period").str.strip_chars().map_elements(
            _period_to_quarter, return_dtype=pl.Int32
        ).alias("_quarter")
    )
    df = df.filter(pl.col("_quarter").is_not_null())
    df = df.filter(
        pl.struct(["year", "_quarter"]).map_elements(
            lambda s: (int(s["year"]), int(s["_quarter"])) in periods,
            return_dtype=pl.Boolean,
        )
    )
    df = df.with_columns(
        pl.col("_quarter")
        .replace_strict(_QUARTER_LAST_MONTH, return_dtype=pl.Int32)
        .alias("_last_month")
    )
    df = df.with_columns(
        pl.struct(["year", "_last_month"])
        .map_elements(
            lambda s: date(int(s["year"]), int(s["_last_month"]), 12),
            return_dtype=pl.Date,
        )
        .alias("ref_date")
    )
    return df.drop("_quarter", "_last_month")


def download_bed(
    periods: list[tuple[int, int]],
    out_dir: Path | None = None,
) -> pl.DataFrame:
    """Download BED data for the requested (year, quarter) periods."""
    out_dir = out_dir or BED_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info("BED: downloading flat file")
    df = read_tsv(BED_DATA_URL)
    logger.info("BED: %d total rows downloaded", len(df))

    df = df.with_columns(pl.col("year").cast(pl.Int32))
    df = _filter_to_periods(df, set(periods))

    df = df.with_columns(pl.lit(datetime.now()).alias("downloaded"))

    out_path = out_dir / BED_ESTIMATES_FILE
    df.write_parquet(out_path)
    logger.info("BED: wrote %d rows to %s", len(df), out_path)
    return df
