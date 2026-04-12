"""Generic BLS archive scraper for publication release dates.

Fetches the archive index page for a publication, follows links to
individual release pages, and extracts the release date from the page header.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from bls_stats.release_dates.config import PUBLICATIONS, Publication

logger = logging.getLogger(__name__)

# Matches dates like "March 11, 2025" or "February 7, 2024"
_DATE_RE = re.compile(
    r"(?:January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+\d{1,2},\s+\d{4}"
)


@dataclass
class ReleaseDate:
    publication: str
    title: str
    release_date: date
    url: str


def scrape_archive(
    pub: Publication | str,
    *,
    timeout: float = 30.0,
    max_releases: int | None = None,
) -> list[ReleaseDate]:
    """Scrape release dates from a BLS publication archive page.

    Args:
        pub: A Publication instance or the name of a registered publication.
        timeout: HTTP request timeout in seconds.
        max_releases: If set, stop after scraping this many release links.
    """
    if isinstance(pub, str):
        pub = PUBLICATIONS[pub]

    with httpx.Client(timeout=timeout) as client:
        resp = client.get(pub.archive_url)
        resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    release_links = _extract_release_links(soup, pub.archive_url)

    if max_releases is not None:
        release_links = release_links[:max_releases]

    results: list[ReleaseDate] = []
    with httpx.Client(timeout=timeout) as client:
        for title, url in release_links:
            release_date = _scrape_release_date(client, url)
            if release_date is not None:
                results.append(
                    ReleaseDate(
                        publication=pub.name,
                        title=title,
                        release_date=release_date,
                        url=url,
                    )
                )
    return results


def scrape_all(
    *,
    timeout: float = 30.0,
    max_per_pub: int | None = None,
) -> list[ReleaseDate]:
    """Scrape release dates for all registered publications."""
    all_dates: list[ReleaseDate] = []
    for pub in PUBLICATIONS.values():
        try:
            dates = scrape_archive(pub, timeout=timeout, max_releases=max_per_pub)
            all_dates.extend(dates)
        except Exception:
            logger.error("Failed to scrape %s", pub.name, exc_info=True)
    return all_dates


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
        # Only follow links that look like release pages (HTML format)
        if "(HTML)" in text or href.endswith(".htm") or href.endswith(".html"):
            abs_url = urljoin(base_url, href)
            clean_title = text.replace("(HTML)", "").strip()
            links.append((clean_title, abs_url))
    return links


def _scrape_release_date(client: httpx.Client, url: str) -> date | None:
    """Fetch a single release page and extract the release date from its header."""
    try:
        resp = client.get(url)
        resp.raise_for_status()
    except Exception:
        logger.warning("Failed to fetch release page: %s", url, exc_info=True)
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # BLS release pages typically have the date in a <p class="sub"> or
    # near the top of the document
    for tag in soup.find_all(["p", "span", "div"], limit=30):
        text = tag.get_text(strip=True)
        match = _DATE_RE.search(text)
        if match:
            try:
                return datetime.strptime(match.group(), "%B %d, %Y").date()
            except ValueError:
                continue

    # Fallback: search the entire page text
    page_text = soup.get_text()
    match = _DATE_RE.search(page_text)
    if match:
        try:
            return datetime.strptime(match.group(), "%B %d, %Y").date()
        except ValueError:
            pass

    logger.warning("Could not extract release date from %s", url)
    return None
