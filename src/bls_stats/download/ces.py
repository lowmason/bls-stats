"""CES (Current Employment Statistics) downloader — bulk flat file.

Downloads the complete CES data file from the BLS FTP server, parses
series IDs into component fields, and filters to the requested period.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import polars as pl

from bls_stats.bls.programs import PROGRAMS
from bls_stats.config import CES_DIR, CES_ESTIMATES_FILE
from bls_stats.download.fetch import read_tsv

logger = logging.getLogger(__name__)

CES_DATA_URL = "https://download.bls.gov/pub/time.series/ce/ce.data.0.AllCESSeries"

_PROGRAM = PROGRAMS["CE"]


def _period_to_month(period: str) -> int | None:
    """Convert BLS period code (M01-M12) to month number. M13=annual avg."""
    if not period.startswith("M"):
        return None
    try:
        m = int(period[1:])
    except ValueError:
        return None
    return m if 1 <= m <= 12 else None


def _add_parsed_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Parse series_id into component fields using positional slicing."""
    for name, offset, length in _PROGRAM.field_slices():
        if name == "prefix":
            continue
        df = df.with_columns(
            pl.col("series_id").str.slice(offset, length).str.strip_chars().alias(name)
        )
    return df


def _filter_to_range(
    df: pl.DataFrame, start_date: date, end_date: date
) -> pl.DataFrame:
    """Filter to rows within the requested date range (monthly)."""
    df = df.with_columns(
        pl.col("period").map_elements(_period_to_month, return_dtype=pl.Int32).alias("month")
    )
    df = df.filter(pl.col("month").is_not_null())
    df = df.with_columns(
        pl.struct(["year", "month"])
        .map_elements(
            lambda s: date(int(s["year"]), int(s["month"]), 1),
            return_dtype=pl.Date,
        )
        .alias("ref_date")
    )
    return df.filter(
        (pl.col("ref_date") >= start_date.replace(day=1))
        & (pl.col("ref_date") <= end_date.replace(day=1))
    )


def download_ces(
    start_date: date,
    end_date: date,
    out_dir: Path | None = None,
) -> pl.DataFrame:
    """Download all CES data for the requested date range."""
    out_dir = out_dir or CES_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info("CES: downloading flat file")
    df = read_tsv(CES_DATA_URL)
    logger.info("CES: %d total rows downloaded", len(df))

    df = df.with_columns(pl.col("year").cast(pl.Int32))
    df = df.filter(
        (pl.col("year") >= start_date.year) & (pl.col("year") <= end_date.year)
    )

    df = _add_parsed_fields(df)
    df = _filter_to_range(df, start_date, end_date)

    df = df.with_columns(
        pl.lit("ces").alias("source"),
        (pl.col("seasonal") == "S").alias("seasonally_adjusted"),
        pl.lit("national").alias("geographic_type"),
        pl.lit("US").alias("geographic_code"),
    )

    out_path = out_dir / CES_ESTIMATES_FILE
    df.write_parquet(out_path)
    logger.info("CES: wrote %d rows to %s", len(df), out_path)
    return df
