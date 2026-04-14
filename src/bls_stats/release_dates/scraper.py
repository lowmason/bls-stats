"""BLS archive and schedule scraper for publication release dates.

Scrapes archive index pages (historical releases back to 2003) and
current-year schedule pages, returning a Polars DataFrame with columns:
``program``, ``release_date``, ``ref_date``, ``revised_date``.
"""

from __future__ import annotations

import calendar
import logging
import re
from datetime import date, datetime
from urllib.parse import urljoin

import httpx
import polars as pl
from bs4 import BeautifulSoup

from bls_stats.download.fetch import USER_AGENT
from bls_stats.release_dates.config import LAPSE_URL, PUBLICATIONS, Publication

logger = logging.getLogger(__name__)

SCHEMA = {
    "program": pl.Utf8,
    "release_date": pl.Date,
    "ref_date": pl.Date,
}

_EMBARGO_RE = re.compile(
    r"embargoed\s+until.*?"
    r"(?:January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+\d{1,2},\s+\d{4}",
    re.IGNORECASE | re.DOTALL,
)

_DATE_RE = re.compile(
    r"(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+(\d{1,2}),\s+(\d{4})"
)

_MONTH_PERIOD_RE = re.compile(
    r"(?:January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+\d{4}",
    re.IGNORECASE,
)

_MONTHS = (
    "January|February|March|April|May|June|July|August|September"
    "|October|November|December"
)
_MONTHS_ABBR = (
    "Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec"
)

_MONTH_ONLY_RE = re.compile(
    rf"(?:^|\bin\s+)({_MONTHS})(?:\b|;)",
    re.IGNORECASE,
)

_MONTH_ABBR_RE = re.compile(
    rf"\b({_MONTHS_ABBR})\.?\s",
    re.IGNORECASE,
)

_QUARTER_PERIOD_RE = re.compile(
    r"(?:First|Second|Third|Fourth|1st|2nd|3rd|4th)\s+Quarter\s+\d{4}",
    re.IGNORECASE,
)

_QUARTER_LOOSE_RE = re.compile(
    r"(1st|2nd|3rd|4th)\s+quarter\s+(?:of\s+)?(\d{4})",
    re.IGNORECASE,
)

_QUARTER_NOYEAR_RE = re.compile(
    r"(1st|2nd|3rd|4th)\s+quarter",
    re.IGNORECASE,
)

_QUARTER_MAP = {
    "first": 1, "second": 2, "third": 3, "fourth": 4,
    "1st": 1, "2nd": 2, "3rd": 3, "4th": 4,
}

_QUARTER_LAST_MONTH = {1: 3, 2: 6, 3: 9, 4: 12}


# ---------------------------------------------------------------------------
# ref_date helpers
# ---------------------------------------------------------------------------

def _last_business_day(year: int, month: int) -> date:
    """Return the last weekday (Mon-Fri) of the given month."""
    last_day = calendar.monthrange(year, month)[1]
    d = date(year, month, last_day)
    while d.weekday() >= 5:  # Sat=5, Sun=6
        d = d.replace(day=d.day - 1)
    return d


def _parse_ref_date(
    program: str, text: str, release_date: date | None = None
) -> date | None:
    """Extract a reference-period date from title text.

    Monthly programs: "MARCH 2026" -> date(2026, 3, 12) (or last biz day for JOLTS).
    Quarterly programs: "FIRST QUARTER 2025" -> date(2025, 3, 12).

    For feed titles that mention only the month name (e.g. "… in March"),
    *release_date* is used to infer the year.  The reference month typically
    precedes the release month, so if the month name is after the release
    month we assume the previous year.
    """
    m = _MONTH_PERIOD_RE.search(text)
    if m:
        dt = datetime.strptime(m.group(), "%B %Y")
        if program == "jolts":
            return _last_business_day(dt.year, dt.month)
        return date(dt.year, dt.month, 12)

    m = _QUARTER_PERIOD_RE.search(text)
    if m:
        parts = m.group().lower().split()
        q = _QUARTER_MAP.get(parts[0])
        year = int(parts[-1])
        if q is None:
            return None
        month = _QUARTER_LAST_MONTH[q]
        return date(year, month, 12)

    m = _QUARTER_LOOSE_RE.search(text)
    if m:
        q = _QUARTER_MAP.get(m.group(1).lower())
        year = int(m.group(2))
        if q is not None:
            month = _QUARTER_LAST_MONTH[q]
            return date(year, month, 12)

    if release_date is not None:
        ref_month = _extract_month_name(text)
        if ref_month is not None:
            year = release_date.year
            if ref_month >= release_date.month:
                year -= 1
            if program == "jolts":
                return _last_business_day(year, ref_month)
            return date(year, ref_month, 12)

        m = _QUARTER_NOYEAR_RE.search(text)
        if m:
            q = _QUARTER_MAP.get(m.group(1).lower())
            if q is not None:
                ref_month = _QUARTER_LAST_MONTH[q]
                year = release_date.year
                if ref_month >= release_date.month:
                    year -= 1
                return date(year, ref_month, 12)

    return None


_MONTH_NUM = {
    name.lower(): i
    for i, name in enumerate(
        ["january", "february", "march", "april", "may", "june",
         "july", "august", "september", "october", "november", "december"],
        1,
    )
}
_ABBR_NUM = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _extract_month_name(text: str) -> int | None:
    """Try to find a month name (full or abbreviated) in *text* and return its number."""
    m = _MONTH_ONLY_RE.search(text)
    if m:
        return _MONTH_NUM.get(m.group(1).lower())

    m = _MONTH_ABBR_RE.search(text)
    if m:
        return _ABBR_NUM.get(m.group(1).lower().rstrip("."))

    return None


# ---------------------------------------------------------------------------
# HTML extraction helpers
# ---------------------------------------------------------------------------

def _extract_embargo_date(soup: BeautifulSoup) -> date | None:
    """Parse the embargo/release date from a news-release page."""
    for tag in soup.find_all(["p", "span", "div"], limit=50):
        text = tag.get_text(" ", strip=True)
        em = _EMBARGO_RE.search(text)
        if em:
            dm = _DATE_RE.search(em.group())
            if dm:
                try:
                    return datetime.strptime(
                        f"{dm.group(1)} {dm.group(2)}, {dm.group(3)}", "%B %d, %Y"
                    ).date()
                except ValueError:
                    continue

    page_text = soup.get_text(" ")
    em = _EMBARGO_RE.search(page_text)
    if em:
        dm = _DATE_RE.search(em.group())
        if dm:
            try:
                return datetime.strptime(
                    f"{dm.group(1)} {dm.group(2)}, {dm.group(3)}", "%B %d, %Y"
                ).date()
            except ValueError:
                pass

    return None


def _extract_ref_period(soup: BeautifulSoup) -> str | None:
    """Extract the reference-period text from a release page title/header."""
    for tag in soup.find_all(["h1", "h2", "h3", "title", "p"], limit=30):
        text = tag.get_text(" ", strip=True)
        if _MONTH_PERIOD_RE.search(text) or _QUARTER_PERIOD_RE.search(text):
            return text

    return None


def _extract_release_links(
    soup: BeautifulSoup, base_url: str
) -> list[tuple[str, str]]:
    """Extract (title, absolute_url) pairs from an archive index page."""
    links: list[tuple[str, str]] = []
    for a_tag in soup.select("li a[href]"):
        href = a_tag.get("href", "")
        text = a_tag.get_text(strip=True)
        if not href or not text:
            continue
        if "(HTML)" in text or href.endswith(".htm") or href.endswith(".html"):
            abs_url = urljoin(base_url, href)
            clean_title = text.replace("(HTML)", "").strip()
            links.append((clean_title, abs_url))
    return links


def _make_client(timeout: float) -> httpx.Client:
    return httpx.Client(
        headers={"User-Agent": USER_AGENT},
        follow_redirects=True,
        timeout=timeout,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def scrape_archive(
    pub: Publication | str,
    *,
    timeout: float = 30.0,
) -> pl.DataFrame:
    """Scrape historical release dates from a BLS publication archive page.

    Follows each release link to extract the embargo date and reference period,
    going back to 2003.

    Returns a DataFrame with columns: ``program``, ``release_date``, ``ref_date``.
    """
    if isinstance(pub, str):
        pub = PUBLICATIONS[pub]

    with _make_client(timeout) as client:
        resp = client.get(pub.archive_url)
        resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    release_links = _extract_release_links(soup, pub.archive_url)

    rows: list[dict] = []
    with _make_client(timeout) as client:
        for title, url in release_links:
            try:
                resp = client.get(url)
                resp.raise_for_status()
            except Exception:
                logger.warning("Failed to fetch %s", url, exc_info=True)
                continue

            page_soup = BeautifulSoup(resp.text, "html.parser")
            release_date = _extract_embargo_date(page_soup)
            if release_date is None:
                logger.warning("No embargo date found on %s", url)
                continue

            ref_text = _extract_ref_period(page_soup)
            ref_date = _parse_ref_date(pub.name, ref_text) if ref_text else None

            rows.append({
                "program": pub.name,
                "release_date": release_date,
                "ref_date": ref_date,
            })

    if not rows:
        return pl.DataFrame(schema=SCHEMA)
    return pl.DataFrame(rows, schema=SCHEMA)


def scrape_schedule(
    pub: Publication | str,
    *,
    timeout: float = 30.0,
) -> pl.DataFrame:
    """Scrape current-year release dates from a BLS schedule page.

    Returns a DataFrame with columns: ``program``, ``release_date``, ``ref_date``.
    """
    if isinstance(pub, str):
        pub = PUBLICATIONS[pub]

    if not pub.schedule_url:
        return pl.DataFrame(schema=SCHEMA)

    with _make_client(timeout) as client:
        resp = client.get(pub.schedule_url)
        resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    rows: list[dict] = []

    for li in soup.select("li"):
        text = li.get_text(" ", strip=True)
        dm = _DATE_RE.search(text)
        if not dm:
            continue

        try:
            release_date = datetime.strptime(
                f"{dm.group(1)} {dm.group(2)}, {dm.group(3)}", "%B %d, %Y"
            ).date()
        except ValueError:
            continue

        ref_date = _parse_ref_date(pub.name, text)
        rows.append({
            "program": pub.name,
            "release_date": release_date,
            "ref_date": ref_date,
        })

    if not rows:
        return pl.DataFrame(schema=SCHEMA)
    return pl.DataFrame(rows, schema=SCHEMA)


def scrape_lapse(*, timeout: float = 30.0) -> pl.DataFrame:
    """Scrape the funding-lapse revised release dates page.

    Returns a DataFrame with columns: ``program``, ``release_date``, ``revised_date``.
    """
    schema = {"program": pl.Utf8, "release_date": pl.Date, "revised_date": pl.Date}

    with _make_client(timeout) as client:
        resp = client.get(LAPSE_URL)
        resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    rows: list[dict] = []

    program_keywords = {
        "employment situation": "ces",
        "empsit": "ces",
        "state and area": "sae",
        "laus": "sae",
        "job openings": "jolts",
        "jolts": "jolts",
        "quarterly census": "qcew",
        "cewqtr": "qcew",
        "business employment dynamics": "bed",
        "cewbd": "bed",
    }

    for row in soup.select("tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) < 3:
            continue

        text = cells[0].get_text(" ", strip=True).lower()
        program = None
        for keyword, prog in program_keywords.items():
            if keyword in text:
                program = prog
                break
        if program is None:
            continue

        orig_match = _DATE_RE.search(cells[1].get_text(" ", strip=True))
        revised_match = _DATE_RE.search(cells[2].get_text(" ", strip=True))

        if not orig_match or not revised_match:
            continue

        try:
            orig_date = datetime.strptime(
                f"{orig_match.group(1)} {orig_match.group(2)}, {orig_match.group(3)}",
                "%B %d, %Y",
            ).date()
            revised_date = datetime.strptime(
                f"{revised_match.group(1)} {revised_match.group(2)}, {revised_match.group(3)}",
                "%B %d, %Y",
            ).date()
        except ValueError:
            continue

        rows.append({
            "program": program,
            "release_date": orig_date,
            "revised_date": revised_date,
        })

    if not rows:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(rows, schema=schema)


def scrape_all(*, timeout: float = 30.0) -> pl.DataFrame:
    """Scrape all programs (archive + schedule) and merge lapse revisions.

    Returns a DataFrame with columns:
    ``program``, ``release_date``, ``ref_date``, ``revised_date``.
    """
    frames: list[pl.DataFrame] = []

    for pub in PUBLICATIONS.values():
        try:
            archive_df = scrape_archive(pub, timeout=timeout)
            if len(archive_df) > 0:
                frames.append(archive_df)
        except Exception:
            logger.error("Failed to scrape archive for %s", pub.name, exc_info=True)

        try:
            schedule_df = scrape_schedule(pub, timeout=timeout)
            if len(schedule_df) > 0:
                frames.append(schedule_df)
        except Exception:
            logger.error("Failed to scrape schedule for %s", pub.name, exc_info=True)

    if not frames:
        return pl.DataFrame(schema={**SCHEMA, "revised_date": pl.Date})

    combined = pl.concat(frames, how="diagonal_relaxed").unique(
        subset=["program", "release_date"], keep="first"
    ).sort(["program", "release_date"])

    try:
        lapse_df = scrape_lapse(timeout=timeout)
        if len(lapse_df) > 0:
            combined = combined.join(
                lapse_df, on=["program", "release_date"], how="left"
            )
        else:
            combined = combined.with_columns(pl.lit(None, dtype=pl.Date).alias("revised_date"))
    except Exception:
        logger.error("Failed to scrape lapse page", exc_info=True)
        combined = combined.with_columns(pl.lit(None, dtype=pl.Date).alias("revised_date"))

    return combined
