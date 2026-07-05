"""Atom feed → typed Release events (ARCH §5.1–§5.2)."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date
from xml.etree import ElementTree

import httpx

from bls_stats.registry import REGISTRY, Frequency

log = logging.getLogger(__name__)

ATOM = "{http://www.w3.org/2005/Atom}"
_LINK_DATE = re.compile(r"_(\d{2})(\d{2})(\d{4})\.htm")
_MONTHS = {
    m: i
    for i, m in enumerate(
        [
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ],
        start=1,
    )
}
_MONTH_RE = re.compile(r"\b(" + "|".join(_MONTHS) + r")\b")
_QUARTER_RE = re.compile(r"\b(First|Second|Third|Fourth) Quarter (\d{4})\b")
_QUARTERS = {"First": 1, "Second": 2, "Third": 3, "Fourth": 4}
_ANNUAL_RE = re.compile(r"\bMay (\d{4})\b")


class FeedParseError(ValueError):
    pass


@dataclass(frozen=True)
class Release:
    program: str
    release_date: date
    ref_year: int
    ref_period: int
    is_benchmark: bool


def _infer_year(month: int, published: date) -> int:
    """Latest occurrence of `month` strictly before the release date (ARCH §5.2)."""
    year = published.year
    if month >= published.month:
        year -= 1
    return year


def _ref_period(text: str, freq: Frequency, release_date: date) -> tuple[int, int] | None:
    if freq == Frequency.MONTHLY:
        m = _MONTH_RE.search(text)
        if not m:
            return None
        month = _MONTHS[m.group(1)]
        return _infer_year(month, release_date), month
    if freq == Frequency.QUARTERLY:
        m = _QUARTER_RE.search(text)
        return (int(m.group(2)), _QUARTERS[m.group(1)]) if m else None
    m = _ANNUAL_RE.search(text)  # annual (oews): "May YYYY"
    return (int(m.group(1)), 1) if m else None


def _is_benchmark(rule: str | None, ref_period: int) -> bool:
    return rule in ("jan_data", "q1_data") and ref_period == 1


def parse_feed(xml_bytes: bytes, program: str) -> list[Release]:
    spec = REGISTRY[program]
    root = ElementTree.fromstring(xml_bytes)
    releases: list[Release] = []
    for entry in root.iter(f"{ATOM}entry"):
        link = entry.find(f"{ATOM}link")
        href = link.get("href", "") if link is not None else ""
        m = _LINK_DATE.search(href)
        if not m:
            log.warning("%s: entry without parseable archive link (%r) — skipped", program, href)
            continue
        release_date = date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        title = (
            (entry.findtext(f"{ATOM}title") or "") + " " + (entry.findtext(f"{ATOM}content") or "")
        )
        parsed = _ref_period(title, spec.frequency, release_date)
        if parsed is None:
            log.warning("%s: no reference period in entry %r — skipped", program, title[:80])
            continue
        ref_year, ref_period = parsed
        releases.append(
            Release(
                program,
                release_date,
                ref_year,
                ref_period,
                _is_benchmark(spec.profile.benchmark_rule, ref_period),
            )
        )
    releases.sort(key=lambda r: r.release_date)
    return releases


def poll(client: httpx.Client, programs: list[str]) -> list[Release]:
    """Fetch each distinct feed once; fan out shared feeds BEFORE any ledger logic (ARCH §5.2)."""
    from bls_stats.core.http import get

    by_feed: dict[str, list[str]] = {}
    for p in programs:
        url = REGISTRY[p].feed_url
        if url is None:  # ep — ARCH §5.2 exception
            continue
        by_feed.setdefault(url, []).append(p)
    out: list[Release] = []
    for url, progs in by_feed.items():
        try:
            body = get(client, url).content
        except httpx.HTTPError as exc:
            log.warning("feed %s failed (%s) — programs %s skipped this run", url, exc, progs)
            continue
        for p in progs:
            out.extend(parse_feed(body, p))
    out.sort(key=lambda r: r.release_date)
    return out
