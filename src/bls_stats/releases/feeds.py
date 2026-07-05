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
    """Reserved for feed-level parse failures; entry-level issues are logged and skipped
    instead (see `parse_feed`), so this is not currently raised."""


@dataclass(frozen=True)
class Release:
    """One detected BLS release event for a single program (ARCH §5.1).

    Produced by `parse_feed`/`poll` from an Atom feed entry. Shared feeds (CES/CPS via
    `empsit`) fan out to one `Release` per program before any ledger anti-join (ARCH §5.2),
    so each program's event carries its own `is_benchmark` determination.

    Attributes:
        program: Registry key, e.g. `"ces"`.
        release_date: The date this release was (or will be) published, parsed from the
            archive-link href — the only stable identity key (ARCH §5.2); never from feed
            timestamps.
        ref_year: Reference year of the data this release covers.
        ref_period: Reference period within `ref_year` — month (1-12), quarter (1-4), or 1
            for annual programs — inferred from title text (year is never present, ARCH §5.2).
        is_benchmark: Whether this release is a benchmark event, determined structurally from
            `ref_period` via the program's `benchmark_rule` (ARCH §5.2) — never from feed text.
    """

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
    """Extract `(ref_year, ref_period)` from entry text. Only monthly titles omit the year
    (ARCH §5.2) and need `_infer_year`; quarterly/annual titles spell the year out directly."""
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
    """Parse one program's Atom feed body into `Release` events, oldest first (ARCH §5.1-§5.2).

    Feeds are Atom 1.0 despite the `.rss` extension. Each entry's release date comes from the
    archive-link href (`..._MMDDYYYY.htm`) — the only stable identity key; the Atom `id` is not
    stable (observed edited in place on `cewbd`) and entry timestamps are unreliable (embargo
    quirks, mislabeled UTC on some feeds). An entry without a parseable link or reference
    period is logged at WARNING and skipped rather than raising — calendar gaps and malformed
    entries are expected, not fatal (ARCH §5.2).

    Args:
        xml_bytes: Raw Atom feed body.
        program: Registry key whose `frequency` and `benchmark_rule` drive parsing — pass the
            program explicitly even for a shared feed (e.g. call once each for `"ces"` and
            `"cps"` against the same `empsit` bytes) so each gets its own `is_benchmark` rule.

    Returns:
        `Release` events sorted by `release_date` ascending. Empty if no entry parsed.
    """
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
    """Fetch each distinct feed once and parse it for every program that shares it.

    Groups `programs` by their `feed_url` so a shared feed (CES/CPS via `empsit`) is
    downloaded a single time, then calls `parse_feed` once per program against those same
    bytes — the fan-out happens here, before any ledger anti-join (ARCH §5.2), so each
    program's events and failure isolation stay independent downstream. EP is skipped (no
    feed, ARCH §5.2 exception). A feed that fails to fetch logs a warning and is skipped for
    this run; the programs it would have covered continue via the ledger's next poll.

    Args:
        client: The shared `httpx.Client`.
        programs: Registry keys to poll, e.g. `["ces", "cps", "jolts"]`.

    Returns:
        `Release` events across all requested programs and their feeds, sorted by
        `release_date` ascending.
    """
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
