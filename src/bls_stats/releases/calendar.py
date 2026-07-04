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


def build(client: httpx.Client, programs: list[str]) -> pl.DataFrame:
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
    """ARCH §5.4 pinned semantics: drop only future-of-latest-published and cancelled periods."""
    mine = cal.filter(pl.col("program") == program)
    published = mine.filter(pl.col("release_date").is_not_null())
    if published.is_empty():
        raise ValueError(f"{program}: release calendar is empty — run `calendar build` first")
    max_ref = published["ref_date"].max()
    cancelled = set(mine.filter(pl.col("release_date").is_null())["ref_date"].to_list())
    kept = []
    for year, period in periods:
        rd = ref_date(program, year, period)
        if rd is None or (rd <= max_ref and rd not in cancelled):
            kept.append((year, period))
    return kept
