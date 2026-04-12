"""SAE (State and Area Employment) downloader — bulk flat file.

Downloads the complete SM data file from the BLS FTP server, parses
series IDs into component fields, and filters to the requested period.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import polars as pl

from bls_stats.bls.programs import PROGRAMS
from bls_stats.config import SAE_DIR, SAE_ESTIMATES_FILE
from bls_stats.download.fetch import read_tsv

logger = logging.getLogger(__name__)

SAE_DATA_URL = "https://download.bls.gov/pub/time.series/sm/sm.data.0.Current"

_PROGRAM = PROGRAMS["SM"]


def _period_to_month(period: str) -> int | None:
    if not period.startswith("M"):
        return None
    try:
        m = int(period[1:])
    except ValueError:
        return None
    return m if 1 <= m <= 12 else None


def _add_parsed_fields(df: pl.DataFrame) -> pl.DataFrame:
    for name, offset, length in _PROGRAM.field_slices():
        if name == "prefix":
            continue
        df = df.with_columns(
            pl.col("series_id").str.slice(offset, length).str.strip_chars().alias(name)
        )
    return df


def _classify_geo(state: str, area: str) -> tuple[str, str]:
    """Derive geographic_type and geographic_code from SM state/area fields."""
    if state == "00" and area == "00000":
        return "national", "US"
    if area == "00000":
        return "state", state
    return "area", f"{state}{area}"


def _filter_to_range(
    df: pl.DataFrame, start_date: date, end_date: date
) -> pl.DataFrame:
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


def download_sae(
    start_date: date,
    end_date: date,
    out_dir: Path | None = None,
) -> pl.DataFrame:
    """Download all SAE (State and Area Employment) data for the requested date range."""
    out_dir = out_dir or SAE_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info("SAE: downloading flat file")
    df = read_tsv(SAE_DATA_URL)
    logger.info("SAE: %d total rows downloaded", len(df))

    df = df.with_columns(pl.col("year").cast(pl.Int32))
    df = df.filter(
        (pl.col("year") >= start_date.year) & (pl.col("year") <= end_date.year)
    )

    df = _add_parsed_fields(df)
    df = _filter_to_range(df, start_date, end_date)

    geo_rows = df.select("state", "area").to_dicts()
    geo_types = []
    geo_codes = []
    for row in geo_rows:
        gt, gc = _classify_geo(row["state"], row["area"])
        geo_types.append(gt)
        geo_codes.append(gc)

    df = df.with_columns(
        pl.lit("sae").alias("source"),
        (pl.col("seasonal") == "S").alias("seasonally_adjusted"),
        pl.Series("geographic_type", geo_types),
        pl.Series("geographic_code", geo_codes),
    )

    out_path = out_dir / SAE_ESTIMATES_FILE
    df.write_parquet(out_path)
    logger.info("SAE: wrote %d rows to %s", len(df), out_path)
    return df
