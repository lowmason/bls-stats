"""OEWS annual workbook engine (BEH §2.3).

OEWS publishes one Excel workbook per year, zipped, containing many sheets; the one this
engine wants is `"All May {year} data"` (OEWS is a May reference-period survey, always
`ref_date = May 12`). Each annual release is treated as its own vintage with no revision
history (registry: `RevisionProfile(1, "fixed", None, None)`) — there is nothing to compare
across releases the way there is for the LABSTAT programs.
"""

from __future__ import annotations

import logging
import tempfile
import zipfile
from datetime import date, datetime
from pathlib import Path

import httpx
import polars as pl

from bls_stats.core.http import download

log = logging.getLogger(__name__)
URL = "https://www.bls.gov/oes/special-requests/oesm{yy}all.zip"


def parse_workbook_zip(zip_path: Path, year: int, *, downloaded: datetime) -> pl.DataFrame:
    """Extract and parse the May-data sheet from one year's OEWS workbook ZIP.

    Reads sheet `"All May {year} data"` via `fastexcel`/`pl.read_excel`, normalizes headers
    (stripped, lowercased), and re-casts `area`/`occ_code` to `Utf8` — Excel round-trips these
    code columns as numeric by default, which would silently drop leading zeros (e.g. area or
    occupation codes) had they not already come through as strings; the explicit post-read cast
    guarantees the contract regardless of how `fastexcel` inferred the column.

    Args:
        zip_path: Local path to the downloaded `oesm{yy}all.zip`.
        year: Reference year; selects both the sheet name and the `ref_date` (always May 12).
        downloaded: Wall-clock ingestion timestamp; stamped onto every row as `downloaded`.

    Returns:
        A `pl.DataFrame` with lowercased/stripped column names, `area` and `occ_code` as
        `Utf8`, `ref_date` (`Date`, fixed at `date(year, 5, 12)`), and `downloaded`
        (`Datetime("us")`). All other workbook columns pass through unchanged.
    """
    with zipfile.ZipFile(zip_path) as zf:
        member = next(n for n in zf.namelist() if n.endswith(".xlsx"))
        with tempfile.TemporaryDirectory() as td:
            xlsx = Path(zf.extract(member, td))
            df = pl.read_excel(xlsx, sheet_name=f"All May {year} data")
    df.columns = [c.strip().lower() for c in df.columns]
    return df.with_columns(
        pl.col("area", "occ_code").cast(pl.Utf8),
        pl.lit(date(year, 5, 12)).alias("ref_date"),
        pl.lit(downloaded).dt.cast_time_unit("us").alias("downloaded"),
    )


def fetch_year(
    client: httpx.Client, year: int, dest_dir: Path, downloaded: datetime
) -> pl.DataFrame:
    """Download one year's OEWS workbook ZIP, parse it, and remove the scratch file.

    Args:
        client: Shared `httpx.Client`.
        year: Reference year to fetch.
        dest_dir: Scratch directory for the downloaded ZIP; not durable storage.
        downloaded: Wall-clock ingestion timestamp, forwarded to `parse_workbook_zip`.

    Returns:
        The parsed `pl.DataFrame`, per `parse_workbook_zip`.
    """
    url = URL.format(yy=f"{year % 100:02d}")
    zip_path = download(client, url, dest_dir / f"oesm{year % 100:02d}all.zip")
    try:
        df = parse_workbook_zip(zip_path, year, downloaded=downloaded)
    finally:
        zip_path.unlink(missing_ok=True)
    log.info("oews %d: %d rows", year, df.height)
    return df
