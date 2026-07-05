"""Release-date calendar: archive/schedule scrape, lapse overlay, gaps,
filter_published (ARCH §5.4)."""

from __future__ import annotations

import logging
import re
from datetime import date

import httpx
import polars as pl
from bs4 import BeautifulSoup

from bls_stats.core.periods import Period, ref_date, shift
from bls_stats.registry import REGISTRY, Frequency

log = logging.getLogger(__name__)

CALENDAR_SCHEMA: dict[str, pl.DataType] = {
    "program": pl.Utf8,
    "ref_date": pl.Date,
    "release_date": pl.Date,
    "original_release": pl.Date,
    "is_benchmark": pl.Boolean,
}
"""Column schema of `state/release_calendar` (ARCH §4.5). `release_date` null means the release
was cancelled by the lapse overlay; `original_release` is null unless the lapse overlay
rescheduled or cancelled the row, in which case it holds the pre-revision date."""

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
_MONTH_YEAR = re.compile(r"\b(" + "|".join(_MONTHS) + r")\s+(\d{4})\b")
_QUARTER_YEAR = re.compile(r"\b(First|Second|Third|Fourth) Quarter (\d{4})\b")
_QUARTERS = {"First": 1, "Second": 2, "Third": 3, "Fourth": 4}
_ABBR_DATE = re.compile(r"\b([A-Z][a-z]{2})\.?\s+(\d{1,2}),\s+(\d{4})\b")
_ABBR = {m[:3]: i for m, i in _MONTHS.items()}


def parse_ref_from_text(text: str, program: str) -> tuple[int, int] | None:
    """Extract `(ref_year, ref_period)` from archive/schedule row text, unlike feed titles
    (`releases.feeds._ref_period`), calendar page text always spells out the year.

    Args:
        text: Row or link text scraped from an archive or schedule page.
        program: Registry key; selects the month/quarter/annual pattern via `frequency`.

    Returns:
        `(ref_year, ref_period)`, or `None` if no matching pattern is found.
    """
    freq = REGISTRY[program].frequency
    if freq == Frequency.MONTHLY:
        m = _MONTH_YEAR.search(text)
        return (int(m.group(2)), _MONTHS[m.group(1)]) if m else None
    if freq == Frequency.QUARTERLY:
        m = _QUARTER_YEAR.search(text)
        return (int(m.group(2)), _QUARTERS[m.group(1)]) if m else None
    m = _MONTH_YEAR.search(text)  # annual: "May 2025"
    return (int(m.group(2)), 1) if m else None


def parse_abbr_date(text: str) -> date | None:
    """Parse a `"Mon. D, YYYY"`-style abbreviated date (schedule/lapse page format).

    Args:
        text: Text to search for the pattern, e.g. `"Jul. 2, 2026"`.

    Returns:
        The parsed date, or `None` if no match, or the month abbreviation is unrecognized.
    """
    m = _ABBR_DATE.search(text)
    if not m or m.group(1) not in _ABBR:
        return None
    return date(int(m.group(3)), _ABBR[m.group(1)], int(m.group(2)))


def _row(
    program: str, ry: int, rp: int, release: date | None, original: date | None = None
) -> dict:
    rule = REGISTRY[program].profile.benchmark_rule
    return {
        "program": program,
        "ref_date": ref_date(program, ry, rp),
        "release_date": release,
        "original_release": original,
        "is_benchmark": rule in ("jan_data", "q1_data") and rp == 1,
    }


def _frame(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=CALENDAR_SCHEMA)


def scrape_archive(html: bytes, program: str) -> pl.DataFrame:
    """Scrape a program's archive page into calendar rows covering historical releases.

    Finds every link matching the `..._MMDDYYYY.htm` archive-date pattern, pairs its embedded
    release date with the reference period parsed from the link's text, and looks up
    `is_benchmark` structurally from the program's `benchmark_rule`. Links without a
    parseable reference period are skipped.

    Args:
        html: Raw archive page HTML.
        program: Registry key.

    Returns:
        A `CALENDAR_SCHEMA` frame with `original_release` always null (archive pages carry no
        revision history — that comes from `apply_lapse_overlay`).
    """
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for a in soup.find_all("a", href=_LINK_DATE):
        m = _LINK_DATE.search(a["href"])
        parsed = parse_ref_from_text(a.get_text(" ", strip=True), program)
        if parsed is None:
            continue
        rows.append(_row(program, *parsed, date(int(m.group(3)), int(m.group(1)), int(m.group(2)))))
    return _frame(rows)


def scrape_schedule(html: bytes, program: str) -> pl.DataFrame:
    """Scrape a program's schedule page into calendar rows covering upcoming releases.

    Each table row must yield both a reference period and an abbreviated release date to be
    kept — rows missing either are silently dropped.

    Args:
        html: Raw schedule page HTML.
        program: Registry key.

    Returns:
        A `CALENDAR_SCHEMA` frame with `original_release` always null.
    """
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for tr in soup.find_all("tr"):
        text = tr.get_text(" ", strip=True)
        parsed = parse_ref_from_text(text, program)
        released = parse_abbr_date(text)
        if parsed and released:
            rows.append(_row(program, *parsed, released))
    return _frame(rows)


def apply_lapse_overlay(cal: pl.DataFrame, html: bytes) -> pl.DataFrame:
    """Apply a government-lapse revised-release-dates table on top of a scraped calendar.

    Each overlay row names an original release date and either a revised date or a
    cancellation. A matching calendar row (by `release_date == original` and a program whose
    release-name text parses against the row's period) is updated: on reschedule, the
    original `release_date` moves to `original_release` and the new date becomes
    `release_date`; on cancellation, `release_date` becomes null and `original_release` still
    records what was originally scheduled. Rows with an unparseable original date, or neither
    a parseable revised date nor a cancellation marker, are skipped.

    Args:
        cal: A calendar frame (`CALENDAR_SCHEMA`) to overlay, e.g. from `scrape_archive`.
        html: Raw HTML of a lapse-revision table (see `LAPSE_URLS`).

    Returns:
        `cal` with matching rows rewritten in place (a new frame; `cal` itself is untouched).
    """
    soup = BeautifulSoup(html, "lxml")
    for tr in soup.find_all("tr"):
        cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        if len(cells) < 3:
            continue
        name, original_txt, revised_txt = cells[0], cells[1], cells[2]
        original = parse_abbr_date(original_txt)
        if original is None:
            continue
        cancelled = "cancel" in revised_txt.lower()
        revised = None if cancelled else parse_abbr_date(revised_txt)
        if not cancelled and revised is None:
            continue
        match = pl.col("release_date") == original
        # Restrict to programs whose release-name text matches this row where possible:
        for program in cal["program"].unique().to_list():
            parsed = parse_ref_from_text(name, program)
            if parsed is None:
                continue
            rd = ref_date(program, *parsed)
            row_match = match & (pl.col("program") == program) & (pl.col("ref_date") == rd)
            cal = cal.with_columns(
                pl.when(row_match)
                .then(pl.col("release_date"))
                .otherwise(pl.col("original_release"))
                .alias("original_release"),
                pl.when(row_match)
                .then(pl.lit(revised, dtype=pl.Date))
                .otherwise(pl.col("release_date"))
                .alias("release_date"),
            )
    return cal


LAPSE_URLS = (
    "https://www.bls.gov/bls/2025-lapse-revised-release-dates.htm",
    "https://www.bls.gov/bls/updated_release_schedule.htm",
)
"""Government-lapse revised-release-dates pages applied on top of every scrape (ARCH §5.4)."""


def build(client: httpx.Client, programs: list[str]) -> pl.DataFrame:
    """Scrape a full release calendar for `programs`: archive + schedule + lapse overlay.

    Per-program tolerance for missing sources (ARCH §5.4): a program with no `archive_url` /
    `schedule_url` configured, or whose page fetch fails, is logged at WARNING and skipped
    rather than aborting the whole build (e.g. QCEW's schedule page 404s). Requests are
    throttled at a fixed 2-second interval across all pages, including the lapse overlay.

    Args:
        client: The shared `httpx.Client`.
        programs: Registry keys to scrape.

    Returns:
        A `CALENDAR_SCHEMA` frame, deduped on `(program, ref_date, release_date)` (keeping the
        first occurrence) and sorted by `(program, ref_date)`.
    """
    from bls_stats.core.http import Throttle, get

    throttle = Throttle(2.0)
    frames: list[pl.DataFrame] = []
    for program in programs:
        spec = REGISTRY[program]
        for kind, url, scraper in (
            ("archive", spec.archive_url, scrape_archive),
            ("schedule", spec.schedule_url, scrape_schedule),
        ):
            if url is None:
                log.warning("%s: no %s page configured — skipped", program, kind)
                continue
            throttle.wait()
            try:
                frames.append(scraper(get(client, url).content, program))
            except httpx.HTTPError as exc:
                log.warning("%s: %s page failed (%s) — skipped", program, kind, exc)
    cal = pl.concat(frames) if frames else _frame([])
    for url in LAPSE_URLS:
        throttle.wait()
        try:
            cal = apply_lapse_overlay(cal, get(client, url).content)
        except httpx.HTTPError as exc:
            log.warning("lapse overlay %s failed (%s) — skipped", url, exc)
    return cal.unique(
        subset=["program", "ref_date", "release_date"], keep="first", maintain_order=True
    ).sort(["program", "ref_date"])


def find_gaps(cal: pl.DataFrame) -> pl.DataFrame:
    """Find reference periods missing from the calendar within each program's observed span.

    For each periodic program (monthly/quarterly; annual and non-periodic programs are
    skipped), walks every period from its earliest to latest observed `ref_date` and reports
    any that has no calendar row at all — a scrape/coverage gap, distinct from a release the
    calendar explicitly marks cancelled (null `release_date`, still present as a row).

    This is a distinct scrape-coverage check and is no longer wired into the `gaps` CLI
    command, which now audits expected-vs-ledger (calendar-expected releases vs. what the
    pipeline actually recorded).

    Args:
        cal: A calendar frame (`CALENDAR_SCHEMA`), e.g. from `build`.

    Returns:
        A frame with `program` (`Utf8`) and `ref_date` (`Date`) columns, one row per missing
        period.
    """
    out: list[dict] = []
    for program in cal["program"].unique().sort().to_list():
        freq = REGISTRY[program].frequency
        if freq not in (Frequency.MONTHLY, Frequency.QUARTERLY):
            continue
        have = set(cal.filter(pl.col("program") == program)["ref_date"].drop_nulls().to_list())
        if not have:
            continue
        lo, hi = min(have), max(have)
        n = 12 if freq == Frequency.MONTHLY else 4
        y, p = lo.year, (lo.month if n == 12 else (lo.month + 2) // 3)
        while ref_date(program, y, p) <= hi:
            rd = ref_date(program, y, p)
            if rd not in have:
                out.append({"program": program, "ref_date": rd})
            y, p = shift(program, y, p, 1)
    return pl.DataFrame(out, schema={"program": pl.Utf8, "ref_date": pl.Date})


def filter_published(program: str, periods: list[Period], cal: pl.DataFrame) -> list[Period]:
    """Restrict a candidate period list to those the calendar confirms were published.

    ARCH §5.4 pinned semantics: drop only (a) periods later than the program's latest
    published `ref_date` and (b) periods explicitly cancelled (a calendar row with a null
    `release_date`). Periods predating calendar coverage **pass through unfiltered** — archive
    scrapes don't reach as far back as the oldest flat-file history (CES starts 1939; no
    archive goes back that far), and a period's presence in the bulk file is itself proof of
    publication. Strict membership against the calendar would silently discard decades of
    history, so this is a deny-list, not an allow-list.

    Args:
        program: Registry key.
        periods: Candidate `(year, period)` pairs to filter, typically from
            `core.periods.reference_periods`.
        cal: A calendar frame (`CALENDAR_SCHEMA`) containing at least this program's rows.

    Returns:
        The subset of `periods` not excluded by either rule, in input order.

    Raises:
        ValueError: The calendar has no published (non-null `release_date`) row for
            `program` at all — bootstrap order violation; run `calendar build` first.
    """
    mine = cal.filter(pl.col("program") == program)
    published = mine.filter(pl.col("release_date").is_not_null())
    if published.is_empty():
        raise ValueError(f"{program}: release calendar is empty — run `calendar build` first")
    max_ref = published["ref_date"].max()
    published_refs = set(published["ref_date"].to_list())
    # A period cancelled in one row but published in another (a rescheduled release) is retained;
    # only periods cancelled and never republished are dropped (C-25, ARCH §5.4).
    cancelled = (
        set(mine.filter(pl.col("release_date").is_null())["ref_date"].to_list()) - published_refs
    )
    kept = []
    for year, period in periods:
        rd = ref_date(program, year, period)
        if rd is None or (rd <= max_ref and rd not in cancelled):
            kept.append((year, period))
    return kept
