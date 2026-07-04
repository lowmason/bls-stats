"""OEWS annual workbook engine (BEH §2.3)."""

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
    url = URL.format(yy=f"{year % 100:02d}")
    zip_path = download(client, url, dest_dir / f"oesm{year % 100:02d}all.zip")
    try:
        df = parse_workbook_zip(zip_path, year, downloaded=downloaded)
    finally:
        zip_path.unlink(missing_ok=True)
    log.info("oews %d: %d rows", year, df.height)
    return df
