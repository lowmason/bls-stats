"""QCEW per-year ZIP engine (BEH §2.2). Strictly one year at a time (ARCH §10).

QCEW publishes one ZIP per year containing every quarter, plus a separate Q1-only "by size
class" ZIP. The two files split the `size_code` domain: the singlefile ZIP carries only
`size_code == "0"` (all establishment sizes combined) at every `agglvl_code`, while the by-size
ZIP carries the non-"0" size-class breakdowns. A benchmark/finalization event needs both; a
routine touched-quarter event needs only the singlefile. Processing one year (and, within
that, one size variant) at a time keeps peak memory bounded per ARCH §10's < 8 GB target —
QCEW is the largest per-file program in the registry.
"""

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
    """Read the single CSV member of a QCEW ZIP with locked dtypes, streaming from disk.

    Code columns are read as `Utf8` to preserve leading zeros and alpha `area_fips` values
    (e.g. `"C1010"` for CSA-level rows) that a type-inferred read would corrupt. The member is
    extracted to a temp file and scanned lazily with a streaming collect so peak memory stays
    bounded on multi-GB years (ARCH §10 < 8 GB target, C-19) — never decompressed whole into
    memory.
    """
    import tempfile

    schema = (
        {c: pl.Utf8 for c in _CODE_COLS}
        | {"year": pl.Int32, "qtr": pl.Int8}
        | {c: pl.Float64 for c in _VALUE_COLS}
    )
    with zipfile.ZipFile(zip_path) as zf:
        member = next(n for n in zf.namelist() if n.endswith(".csv"))
        with tempfile.TemporaryDirectory() as td:
            csv_path = Path(zf.extract(member, td))
            return pl.scan_csv(csv_path, schema_overrides=schema).collect(engine="streaming")


def parse_year_zip(
    zip_path: Path,
    quarters: list[int],
    *,
    downloaded: datetime,
    by_size_zip: Path | None = None,
) -> pl.DataFrame:
    """Parse one year's QCEW singlefile ZIP (and optionally its by-size companion).

    Filters both inputs to the requested quarters, splits them on `size_code` (singlefile
    keeps `"0"`, by-size keeps everything else — see module docstring), concatenates, and
    attaches `ref_date` per quarter via `QUARTER_END_12`.

    Args:
        zip_path: Local path to the year's `{year}_qtrly_singlefile.zip`.
        quarters: Quarter numbers (1-4) to keep — the event's touched-quarter set (ARCH §6.2).
        downloaded: Wall-clock ingestion timestamp; stamped onto every row as `downloaded`.
        by_size_zip: Local path to the year's `{year}_q1_by_size.zip`, or `None` to skip the
            by-size breakdown (routine touched-quarter events don't need it; benchmark/
            finalization events do).

    Returns:
        A `pl.DataFrame` with the QCEW code columns (`area_fips`, `own_code`, `industry_code`,
        `agglvl_code`, `size_code`, `disclosure_code`, all `Utf8`), the value columns
        (`Float64`), `ref_date` (`Date`), and `downloaded` (`Datetime("us")`). `year` and `qtr`
        are dropped after being consumed for the `ref_date` join.
    """
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
    """Download one year's QCEW ZIP(s), parse to the requested quarters, and clean up scratch.

    Args:
        client: Shared `httpx.Client`.
        year: Calendar year to fetch — QCEW is processed strictly one year at a time (module
            docstring; ARCH §10 memory discipline).
        quarters: Quarter numbers (1-4) to keep.
        dest_dir: Scratch directory for the downloaded ZIP(s); not durable storage.
        downloaded: Wall-clock ingestion timestamp, forwarded to `parse_year_zip`.
        with_size: If `True`, also download and merge the by-size ZIP (needed for benchmark/
            finalization events; skipped for routine touched-quarter events).

    Returns:
        The parsed `pl.DataFrame`, per `parse_year_zip`.
    """
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
