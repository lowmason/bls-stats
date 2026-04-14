"""QCEW downloader — bulk zip files from BLS open data.

Downloads the complete quarterly CSV for each year in the requested range,
containing all industries, areas, ownership types, and size classes.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
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

_QUARTER_LAST_MONTH = {1: 3, 2: 6, 3: 9, 4: 12}


def _filter_to_periods(
    df: pl.DataFrame, periods: set[tuple[int, int]]
) -> pl.DataFrame:
    """Filter QCEW rows to the requested (year, quarter) pairs."""
    return df.filter(
        pl.struct(["year", "qtr"]).map_elements(
            lambda s: (int(s["year"]), int(s["qtr"])) in periods,
            return_dtype=pl.Boolean,
        )
    )


def _ref_date_from_quarter(year: int, qtr: int) -> date:
    return date(year, _QUARTER_LAST_MONTH[qtr], 12)


def download_qcew(
    periods: list[tuple[int, int]],
    out_dir: Path | None = None,
) -> pl.DataFrame:
    """Download QCEW bulk data for the requested (year, quarter) periods.

    Downloads one zip per year, extracts all CSVs, filters to the requested
    quarters, and writes a single parquet file.
    """
    out_dir = out_dir or QCEW_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    years = sorted({y for y, _ in periods})

    yearly_frames: list[pl.DataFrame] = []
    for year in years:
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
        combined = _filter_to_periods(combined, set(periods))

    combined = combined.with_columns(
        pl.struct(["year", "qtr"])
        .map_elements(
            lambda s: _ref_date_from_quarter(int(s["year"]), int(s["qtr"])),
            return_dtype=pl.Date,
        )
        .alias("ref_date")
    )

    combined = combined.with_columns(pl.lit(datetime.now()).alias("downloaded"))

    out_path = out_dir / QCEW_ESTIMATES_FILE
    combined.write_parquet(out_path)
    logger.info("QCEW: wrote %d rows to %s", len(combined), out_path)
    return combined
