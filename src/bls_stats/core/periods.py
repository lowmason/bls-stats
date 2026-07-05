"""Reference-period generation and canonical ref_date rules (BEH §3, §4)."""

from __future__ import annotations

import re
from datetime import date, timedelta

from bls_stats.registry import REGISTRY, Frequency, RefDateRule

Period = tuple[int, int]

_QUARTER_END_MONTH = {1: 3, 2: 6, 3: 9, 4: 12}


class PeriodError(ValueError):
    pass


def _spec(program: str):
    try:
        return REGISTRY[program]
    except KeyError:
        raise PeriodError(f"unknown program: {program!r}") from None


def _parse(program: str, text: str) -> Period:
    freq = _spec(program).frequency
    if freq == Frequency.MONTHLY:
        m = re.fullmatch(r"(\d{4})/(\d{1,2})", text)
        if not m or not 1 <= int(m.group(2)) <= 12:
            raise PeriodError(f"{program}: expected YYYY/MM (01-12), got {text!r}")
        return int(m.group(1)), int(m.group(2))
    if freq == Frequency.QUARTERLY:
        m = re.fullmatch(r"(\d{4})/0?([1-4])", text)
        if not m:
            raise PeriodError(f"{program}: expected YYYY/Q (1-4), got {text!r}")
        return int(m.group(1)), int(m.group(2))
    m = re.fullmatch(r"\d{4}", text)  # ANNUAL and NONE take plain years
    if not m:
        raise PeriodError(f"{program}: expected YYYY, got {text!r}")
    return int(text), 1


def _per_year(program: str) -> int:
    return {Frequency.MONTHLY: 12, Frequency.QUARTERLY: 4}.get(_spec(program).frequency, 1)


def _to_index(period: Period, n: int) -> int:
    return period[0] * n + (period[1] - 1)


def _from_index(idx: int, n: int) -> Period:
    return idx // n, idx % n + 1


def reference_periods(program: str, start: str, end: str) -> list[Period]:
    lo, hi = _parse(program, start), _parse(program, end)
    n = _per_year(program)
    a, b = _to_index(lo, n), _to_index(hi, n)
    if a > b:
        raise PeriodError(f"start {start!r} is after end {end!r}")
    return [_from_index(i, n) for i in range(a, b + 1)]


def shift(program: str, year: int, period: int, by: int) -> Period:
    n = _per_year(program)
    return _from_index(_to_index((year, period), n) + by, n)


def last_business_day(year: int, month: int) -> date:
    nxt = date(year + (month == 12), month % 12 + 1, 1)
    d = nxt - timedelta(days=1)
    while d.weekday() >= 5:  # Sat/Sun
        d -= timedelta(days=1)
    return d


def ref_date(program: str, year: int, period: int) -> date | None:
    rule = _spec(program).ref_date_rule
    if rule == RefDateRule.DAY_12:
        return date(year, period, 12)
    if rule == RefDateRule.LAST_BUSINESS_DAY:
        return last_business_day(year, period)
    if rule == RefDateRule.QUARTER_END_12:
        return date(year, _QUARTER_END_MONTH[period], 12)
    if rule == RefDateRule.MAY_12:
        return date(year, 5, 12)
    return None  # ep (ARCH §4.3)
