"""BED (Business Employment Dynamics) downloader — bulk flat file.

Downloads the complete BD data file from the BLS FTP server, parses
series IDs into component fields, and filters to the requested period.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path

import polars as pl

from bls_stats.bls.programs import PROGRAMS
from bls_stats.config import BED_DIR, BED_ESTIMATES_FILE
from bls_stats.download.fetch import read_tsv

logger = logging.getLogger(__name__)

BED_DATA_URL = "https://download.bls.gov/pub/time.series/bd/bd.data.0.Current"

_PROGRAM = PROGRAMS["BD"]

_QUARTER_MAP = {"Q01": 1, "Q02": 4, "Q03": 7, "Q04": 10}


def _period_to_quarter_date(year: int, period: str) -> date | None:
    month = _QUARTER_MAP.get(period)
    if month is None:
        return None
    return date(year, month, 1)


def _add_parsed_fields(df: pl.DataFrame) -> pl.DataFrame:
    for name, offset, length in _PROGRAM.field_slices():
        if name == "prefix":
            continue
        df = df.with_columns(
            pl.col("series_id").str.slice(offset, length).str.strip_chars().alias(name)
        )
    return df


def _classify_geo(area_code: str) -> tuple[str, str]:
    """Derive geographic_type and geographic_code from BD 10-digit area code."""
    if area_code == "0000000000":
        return "national", "US"
    state_fips = area_code[:2]
    if area_code[2:] == "00000000":
        return "state", state_fips
    return "area", area_code


def _filter_to_range(
    df: pl.DataFrame, start_date: date, end_date: date
) -> pl.DataFrame:
    df = df.with_columns(
        pl.struct(["year", "period"])
        .map_elements(
            lambda s: _period_to_quarter_date(int(s["year"]), s["period"].strip()),
            return_dtype=pl.Date,
        )
        .alias("ref_date")
    )
    df = df.filter(pl.col("ref_date").is_not_null())
    return df.filter(
        (pl.col("ref_date") >= start_date.replace(day=1))
        & (pl.col("ref_date") <= end_date.replace(day=1))
    )


def download_bed(
    start_date: date,
    end_date: date,
    out_dir: Path | None = None,
) -> pl.DataFrame:
    """Download all BED data for the requested date range."""
    out_dir = out_dir or BED_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info("BED: downloading flat file")
    df = read_tsv(BED_DATA_URL)
    logger.info("BED: %d total rows downloaded", len(df))

    df = df.with_columns(pl.col("year").cast(pl.Int32))
    df = df.filter(
        (pl.col("year") >= start_date.year) & (pl.col("year") <= end_date.year)
    )

    df = _add_parsed_fields(df)
    df = _filter_to_range(df, start_date, end_date)

    geo_rows = df.select("area_code").to_dicts()
    geo_types = []
    geo_codes = []
    for row in geo_rows:
        gt, gc = _classify_geo(row["area_code"])
        geo_types.append(gt)
        geo_codes.append(gc)

    df = df.with_columns(
        pl.lit("bed").alias("source"),
        (pl.col("seasonal") == "S").alias("seasonally_adjusted"),
        pl.Series("geographic_type", geo_types),
        pl.Series("geographic_code", geo_codes),
    )

    df = df.with_columns(pl.lit(datetime.now()).alias("downloaded"))

    out_path = out_dir / BED_ESTIMATES_FILE
    df.write_parquet(out_path)
    logger.info("BED: wrote %d rows to %s", len(df), out_path)
    return df
