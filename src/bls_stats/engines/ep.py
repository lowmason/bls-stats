"""EP national matrix scraper (BEH §2.4). No feed — annual/on-demand trigger (ARCH §5.2)."""

from __future__ import annotations

import logging
import re
from datetime import datetime
from pathlib import Path

import httpx
import polars as pl
from bs4 import BeautifulSoup

from bls_stats.core.http import Throttle, get

log = logging.getLogger(__name__)

INDEX_URL = "https://www.bls.gov/emp/tables/industry-occupation-matrix-occupation.htm"
MATRIX_URL = "https://data.bls.gov/projections/nationalMatrix?queryParams={soc}&ioType=o"
_SOC = re.compile(r"queryParams=(\d{2}-\d{4})")

_HEADER_MAP: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"^\d{4} Employment$"), "base_year_employment"),
    (re.compile(r"^\d{4} Percent of Occupation$"), "base_year_pct_of_occupation"),
    (re.compile(r"^\d{4} Percent of Industry$"), "base_year_pct_of_industry"),
    (re.compile(r"^Projected \d{4} Employment$"), "projected_year_employment"),
    (re.compile(r"^Projected \d{4} Percent of Occupation$"), "projected_year_pct_of_occupation"),
    (re.compile(r"^Projected \d{4} Percent of Industry$"), "projected_year_pct_of_industry"),
    (re.compile(r"^Employment Change"), "employment_change"),
    (re.compile(r"^Employment Percent Change"), "employment_pct_change"),
    (re.compile(r"^Industry Title$"), "industry_title"),
    (re.compile(r"^Industry Code$"), "industry_code"),
    (re.compile(r"^Industry Type$"), "industry_type"),
]
_NUMERIC = {
    "base_year_employment",
    "base_year_pct_of_occupation",
    "base_year_pct_of_industry",
    "projected_year_employment",
    "projected_year_pct_of_occupation",
    "projected_year_pct_of_industry",
    "employment_change",
    "employment_pct_change",
}


class EpScrapeError(RuntimeError):
    pass


def _normalize(header: str) -> str:
    text = header.strip()
    for pattern, name in _HEADER_MAP:
        if pattern.match(text):
            return name
    return text.lower().replace(" ", "_")


def parse_index(html: bytes) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    socs = []
    for a in soup.find_all("a", href=_SOC):
        socs.append(_SOC.search(a["href"]).group(1))
    return sorted(set(socs), key=socs.index)


def parse_matrix(html: bytes, soc: str) -> pl.DataFrame:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if table is None:
        raise EpScrapeError(f"{soc}: no table in response")
    headers = [_normalize(th.get_text(" ", strip=True)) for th in table.find_all("th")]
    rows = []
    for tr in table.find_all("tr"):
        cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        if len(cells) == len(headers):
            rows.append(dict(zip(headers, cells, strict=True)))
    df = pl.DataFrame(rows)
    numeric = [c for c in df.columns if c in _NUMERIC]
    return df.with_columns(
        pl.col(numeric)
        .str.replace_all(",", "")
        .str.replace_all(r"^[–—-]$", "")
        .replace("", None)
        .cast(pl.Float64, strict=False),
        pl.lit(soc).alias("occupation_code"),
    )


def fetch_matrix(
    client: httpx.Client,
    *,
    throttle: Throttle | None = None,
    downloaded: datetime,
    cache: Path | None = None,
    refresh: bool = False,
) -> pl.DataFrame:
    if cache is not None and cache.exists() and not refresh:
        log.info("ep: using cached matrix %s", cache)
        return pl.read_parquet(cache)
    throttle = throttle or Throttle(2.0)
    socs = parse_index(get(client, INDEX_URL).content)
    frames: list[pl.DataFrame] = []
    for soc in socs:
        throttle.wait()
        try:
            frames.append(parse_matrix(get(client, MATRIX_URL.format(soc=soc)).content, soc))
        except (httpx.HTTPError, EpScrapeError) as exc:
            log.warning("ep %s failed (%s) — continuing", soc, exc)  # BEH §2.4
    if not frames:
        raise EpScrapeError("all occupations failed")
    df = pl.concat(frames, how="diagonal").with_columns(
        pl.lit(downloaded).dt.cast_time_unit("us").alias("downloaded_at")
    )
    if cache is not None:
        cache.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(cache)
    return df
