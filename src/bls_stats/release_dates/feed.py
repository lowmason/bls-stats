"""Atom feed poller for BLS publication release dates.

BLS publishes per-program Atom feeds (with ``.rss`` extension) at
``https://www.bls.gov/feed/{series}.rss``.  Each feed has ~12 entries
covering roughly one year for monthly programs and three years for
quarterly programs.

The ``poll_feed`` / ``poll_all`` functions parse these feeds and return
the same DataFrame schema as the HTML scrapers in ``scraper.py``.
"""

from __future__ import annotations

import logging
from datetime import date, datetime

import feedparser
import httpx
import polars as pl

from bls_stats.download.fetch import USER_AGENT
from bls_stats.release_dates.config import PUBLICATIONS, Publication
from bls_stats.release_dates.scraper import SCHEMA, _parse_ref_date

logger = logging.getLogger(__name__)


def poll_feed(
    pub: Publication | str,
    *,
    timeout: float = 30.0,
) -> pl.DataFrame:
    """Fetch and parse a single program's Atom feed.

    Returns a DataFrame with columns: ``program``, ``release_date``, ``ref_date``.
    """
    if isinstance(pub, str):
        pub = PUBLICATIONS[pub]

    with httpx.Client(
        headers={"User-Agent": USER_AGENT},
        follow_redirects=True,
        timeout=timeout,
    ) as client:
        resp = client.get(pub.feed_url)
        resp.raise_for_status()

    feed = feedparser.parse(resp.text)
    rows: list[dict] = []

    for entry in feed.entries:
        published = getattr(entry, "published", None)
        if not published:
            continue

        try:
            release_date = datetime.fromisoformat(
                published.replace("Z", "+00:00")
            ).date()
        except (ValueError, TypeError):
            pub_parsed = entry.get("published_parsed")
            if pub_parsed:
                release_date = date(pub_parsed.tm_year, pub_parsed.tm_mon, pub_parsed.tm_mday)
            else:
                logger.warning("Could not parse date from entry: %s", published)
                continue

        title = getattr(entry, "title", "")
        ref_date = _parse_ref_date(pub.name, title, release_date)

        rows.append({
            "program": pub.name,
            "release_date": release_date,
            "ref_date": ref_date,
        })

    if not rows:
        return pl.DataFrame(schema=SCHEMA)
    return pl.DataFrame(rows, schema=SCHEMA)


def poll_all(*, timeout: float = 30.0) -> pl.DataFrame:
    """Poll Atom feeds for all registered publications.

    Returns a DataFrame with columns: ``program``, ``release_date``, ``ref_date``.
    """
    frames: list[pl.DataFrame] = []
    for pub in PUBLICATIONS.values():
        try:
            df = poll_feed(pub, timeout=timeout)
            if len(df) > 0:
                frames.append(df)
        except Exception:
            logger.error("Failed to poll feed for %s", pub.name, exc_info=True)

    if not frames:
        return pl.DataFrame(schema=SCHEMA)
    return pl.concat(frames, how="diagonal_relaxed").sort(["program", "release_date"])
