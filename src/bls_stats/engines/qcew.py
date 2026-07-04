"""QCEW per-year ZIP engine (BEH §2.2). Strictly one year at a time (ARCH §10)."""

from __future__ import annotations

import logging
import zipfile
from datetime import datetime
from pathlib import Path

import httpx
import polars as pl

from bls_stats.core.http import download
from bls_stats.core.periods import ref_date

log = logging.getLogger(__name__)

_CODE_COLS = (
    "area_fips",
    "own_code",
    "industry_code",
    "agglvl_code",
    "size_code",
    "disclosure_code",
)
_VALUE_COLS = (
    "qtrly_estabs",
    "month1_emplvl",
    "month2_emplvl",
    "month3_emplvl",
    "total_qtrly_wages",
    "taxable_qtrly_wages",
    "qtrly_contributions",
    "avg_wkly_wage",
)
URL = "https://data.bls.gov/cew/data/files/{year}/csv/{year}_qtrly_singlefile.zip"
SIZE_URL = "https://data.bls.gov/cew/data/files/{year}/csv/{year}_q1_by_size.zip"


def _read_zip_csv(zip_path: Path) -> pl.DataFrame:
    with zipfile.ZipFile(zip_path) as zf:
        member = next(n for n in zf.namelist() if n.endswith(".csv"))
        with zf.open(member) as fh:
            return pl.read_csv(
                fh.read(),
                schema_overrides={c: pl.Utf8 for c in _CODE_COLS}
                | {"year": pl.Int32, "qtr": pl.Int8}
                | {c: pl.Float64 for c in _VALUE_COLS},
            )


def parse_year_zip(
    zip_path: Path,
    quarters: list[int],
    *,
    downloaded: datetime,
    by_size_zip: Path | None = None,
) -> pl.DataFrame:
    df = _read_zip_csv(zip_path).filter(
        pl.col("qtr").is_in(quarters) & (pl.col("size_code") == "0")
    )
    if by_size_zip is not None:
        size_df = _read_zip_csv(by_size_zip).filter(
            pl.col("qtr").is_in(quarters) & (pl.col("size_code") != "0")
        )
        df = pl.concat([df, size_df], how="vertical_relaxed")
    ref_dates = pl.DataFrame(
        [
            {"year": y, "qtr": q, "ref_date": ref_date("qcew", y, q)}
            for y, q in df.select("year", "qtr").unique().iter_rows()
        ],
        schema={"year": pl.Int32, "qtr": pl.Int8, "ref_date": pl.Date},
    )
    return (
        df.join(ref_dates, on=["year", "qtr"], how="left")
        .drop("year", "qtr")
        .select(*_CODE_COLS, *_VALUE_COLS, "ref_date")
        .with_columns(pl.lit(downloaded).dt.cast_time_unit("us").alias("downloaded"))
    )


def fetch_year(
    client: httpx.Client,
    year: int,
    quarters: list[int],
    dest_dir: Path,
    downloaded: datetime,
    *,
    with_size: bool = False,
) -> pl.DataFrame:
    zip_path = download(client, URL.format(year=year), dest_dir / f"qcew_{year}.zip")
    size_path = None
    if with_size:
        size_path = download(client, SIZE_URL.format(year=year), dest_dir / f"qcew_{year}_size.zip")
    try:
        df = parse_year_zip(zip_path, quarters, downloaded=downloaded, by_size_zip=size_path)
    finally:
        zip_path.unlink(missing_ok=True)
        if size_path:
            size_path.unlink(missing_ok=True)
    log.info("qcew %d: %d rows for quarters %s", year, df.height, quarters)
    return df
