"""QCEW downloader — bulk zip files from BLS open data.

Downloads the complete quarterly CSV for each year in the requested range,
containing all industries, areas, ownership types, and size classes.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import polars as pl

from bls_stats.config import QCEW_DIR, QCEW_ESTIMATES_FILE
from bls_stats.download.fetch import read_zip_csvs

logger = logging.getLogger(__name__)

QCEW_BULK_URL = (
    "https://data.bls.gov/cew/data/files/{year}/csv/{year}_qtrly_singlefile.zip"
)

# area_fips mixes numeric county codes (e.g. "01001") with alpha-prefixed MSA
# codes (e.g. "C1010"), so Polars' schema inference can mistype it as i64.
QCEW_SCHEMA_OVERRIDES: dict[str, pl.DataType] = {
    "area_fips": pl.Utf8,
}


def _quarter_for_month(month: int) -> int:
    return (month - 1) // 3 + 1


def _classify_area(area_fips: str) -> tuple[str, str]:
    """Return (geographic_type, geographic_code) from a QCEW area_fips."""
    area_fips = str(area_fips).strip()
    if area_fips == "US000":
        return "national", "US"
    if area_fips.startswith("C"):
        return "msa", area_fips
    if area_fips.endswith("000") and len(area_fips) == 5:
        return "state", area_fips[:2]
    return "county", area_fips


def _filter_to_range(
    df: pl.DataFrame, start_date: date, end_date: date
) -> pl.DataFrame:
    """Filter QCEW rows to the requested quarter range."""
    start_q = _quarter_for_month(start_date.month)
    end_q = _quarter_for_month(end_date.month)

    return df.filter(
        (
            (pl.col("year") > start_date.year)
            | ((pl.col("year") == start_date.year) & (pl.col("qtr") >= start_q))
        )
        & (
            (pl.col("year") < end_date.year)
            | ((pl.col("year") == end_date.year) & (pl.col("qtr") <= end_q))
        )
    )


def download_qcew(
    start_date: date,
    end_date: date,
    out_dir: Path | None = None,
) -> pl.DataFrame:
    """Download QCEW bulk data for the requested date range.

    Downloads one zip per year, extracts all CSVs, filters to the requested
    quarter range, and writes a single parquet file.
    """
    out_dir = out_dir or QCEW_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    start_year = start_date.year
    end_year = end_date.year

    yearly_frames: list[pl.DataFrame] = []
    for year in range(start_year, end_year + 1):
        url = QCEW_BULK_URL.format(year=year)
        logger.info("QCEW: downloading bulk data for %d", year)
        try:
            df = read_zip_csvs(url, schema_overrides=QCEW_SCHEMA_OVERRIDES)
            if not df.is_empty():
                yearly_frames.append(df)
                logger.info("QCEW: %d rows for year %d", len(df), year)
        except Exception:
            logger.error("QCEW: failed to download %d", year, exc_info=True)

    if not yearly_frames:
        logger.warning("QCEW: no data downloaded")
        return pl.DataFrame()

    combined = pl.concat(yearly_frames, how="diagonal_relaxed")

    if "year" in combined.columns and "qtr" in combined.columns:
        combined = combined.with_columns(
            pl.col("year").cast(pl.Int32),
            pl.col("qtr").cast(pl.Int32),
        )
        combined = _filter_to_range(combined, start_date, end_date)

    out_path = out_dir / QCEW_ESTIMATES_FILE
    combined.write_parquet(out_path)
    logger.info("QCEW: wrote %d rows to %s", len(combined), out_path)
    return combined
