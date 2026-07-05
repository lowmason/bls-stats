"""EP national matrix scraper (BEH §2.4). No feed — annual/on-demand trigger (ARCH §5.2).

Employment Projections has no bulk flat file: the data lives only as one HTML table per SOC
occupation code, so this engine scrapes an index page for the occupation list, then fetches
each occupation's matrix page individually, throttled (`core.http.Throttle`) to be a polite
crawler. A per-occupation failure is logged and skipped rather than aborting the whole run
(BEH §2.4); the run only fails if *every* occupation fails. Results are cached to Parquet
(`fetch_matrix`'s `cache` argument) since a full scrape is slow and EP changes at most once a
year.

**Not yet wired into the vintage store.** EP's wide per-occupation matrix (no `series_id` /
`value` columns) does not fit the shared observations schema (ARCH §4.3, §12 item 6); the
pipeline currently guards this path with an explicit error and exit code 2 instead of silently
no-opping. The functions in this module are exercised directly by tests and by the eventual
wiring, not yet by `pipeline.py`.
"""

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
    """Raised when an EP page cannot be parsed into the expected table shape."""


def _normalize(header: str) -> str:
    """Map a raw matrix-page column header to its contract name via `_HEADER_MAP`.

    Year-bearing headers (e.g. `"2024 Employment"`, `"Projected 2034 Employment"`) are matched
    by pattern rather than literal value since the base/projection years advance every cycle;
    unrecognized headers fall back to a lowercased, underscore-joined slug rather than raising.
    """
    text = header.strip()
    for pattern, name in _HEADER_MAP:
        if pattern.match(text):
            return name
    return text.lower().replace(" ", "_")


def parse_index(html: bytes) -> list[str]:
    """Extract the ordered, deduplicated list of SOC occupation codes from the index page.

    Args:
        html: Raw bytes of the industry-occupation-matrix index page.

    Returns:
        SOC codes (e.g. `"11-1011"`) in first-seen order, one per occupation.
    """
    soup = BeautifulSoup(html, "lxml")
    socs = []
    for a in soup.find_all("a", href=_SOC):
        socs.append(_SOC.search(a["href"]).group(1))
    return sorted(set(socs), key=socs.index)


def parse_matrix(html: bytes, soc: str) -> pl.DataFrame:
    """Parse one occupation's national matrix page into a row-per-industry frame.

    Numeric columns (`_NUMERIC`) are stripped of thousands separators, en/em-dash and hyphen
    placeholders (BLS's "not applicable"/suppressed marker) are treated as null, and the
    result is cast to `Float64` non-strictly so any remaining oddity becomes null rather than
    raising.

    Args:
        html: Raw bytes of one occupation's matrix page (`MATRIX_URL` response).
        soc: The occupation's SOC code, stamped onto every row as `occupation_code`.

    Returns:
        A `pl.DataFrame`, one row per industry crossed with `soc`, with normalized column
        names (see `_normalize`), numeric columns as `Float64`, and `occupation_code` (`Utf8`).

    Raises:
        EpScrapeError: The page contains no `<table>` element.
    """
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
    """Scrape the full EP national matrix: index page, then every occupation's matrix page.

    Serves a Parquet cache when present (unless `refresh` forces a re-scrape) since EP changes
    at most once a year and a full scrape walks every SOC code one throttled request at a time.
    Per-occupation HTTP or parse failures are logged and skipped (BEH §2.4) so one bad page
    doesn't sink the whole run; the run only fails if every occupation fails.

    Args:
        client: Shared `httpx.Client`.
        throttle: Rate limiter between occupation requests; defaults to `Throttle(2.0)`
            (2-second spacing) to keep the crawl polite.
        downloaded: Wall-clock ingestion timestamp; stamped onto the result as `downloaded_at`
            (note: not `downloaded` — EP predates the vintage column convention used by the
            other engines, since it isn't wired into the vintage store yet).
        cache: Parquet path to read from / write to. `None` disables caching entirely.
        refresh: If `True`, ignore an existing cache and re-scrape.

    Returns:
        A `pl.DataFrame` concatenating every occupation's `parse_matrix` output
        (`how="diagonal"`, so occupations with slightly different column sets still concat)
        plus `downloaded_at` (`Datetime("us")`).

    Raises:
        EpScrapeError: Every occupation failed to scrape (nothing to return).
    """
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
