"""Reference-period generation and canonical ref_date rules (BEH §3, §4)."""

from __future__ import annotations

import re
from datetime import date, timedelta

from bls_stats.registry import REGISTRY, Frequency, RefDateRule

Period = tuple[int, int]

_QUARTER_END_MONTH = {1: 3, 2: 6, 3: 9, 4: 12}


class PeriodError(ValueError):
    """An invalid period string, out-of-range component, or unknown program name."""


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
    """Enumerate every `(year, period)` pair in `[start, end]`, inclusive of both ends.

    The period grammar is program-frequency-dependent (BEH §3): `"YYYY/MM"` (01-12) for
    monthly programs, `"YYYY/Q"` (1-4) for quarterly programs, and plain `"YYYY"` for
    annual/non-periodic programs (the period component of the returned tuples is always `1`
    in that case).

    Args:
        program: Registry key, e.g. `"ces"` or `"qcew"`. Selects the period grammar.
        start: Inclusive range start, in the program's period grammar.
        end: Inclusive range end, in the program's period grammar.

    Returns:
        `(year, period)` tuples in ascending order, one per period in the range.

    Raises:
        PeriodError: `start`/`end` don't match the program's grammar, a component is
            out of range, `start` is after `end`, or `program` is unknown.
    """
    lo, hi = _parse(program, start), _parse(program, end)
    n = _per_year(program)
    a, b = _to_index(lo, n), _to_index(hi, n)
    if a > b:
        raise PeriodError(f"start {start!r} is after end {end!r}")
    return [_from_index(i, n) for i in range(a, b + 1)]


def shift(program: str, year: int, period: int, by: int) -> Period:
    """Return the period `by` steps from `(year, period)`, wrapping across year boundaries.

    A step is one month for monthly programs, one quarter for quarterly programs, and one
    year for annual/non-periodic programs. `by` may be negative.

    Args:
        program: Registry key; determines the step size via the program's frequency.
        year: Starting year.
        period: Starting period (month 1-12, quarter 1-4, or 1 for annual programs).
        by: Number of steps to shift, positive or negative.

    Returns:
        The resulting `(year, period)` pair.
    """
    n = _per_year(program)
    return _from_index(_to_index((year, period), n) + by, n)


def last_business_day(year: int, month: int) -> date:
    """Return the last weekday (Mon-Fri) of the given month — JOLTS' `ref_date` anchor (BEH §4)."""
    nxt = date(year + (month == 12), month % 12 + 1, 1)
    d = nxt - timedelta(days=1)
    while d.weekday() >= 5:  # Sat/Sun
        d -= timedelta(days=1)
    return d


def ref_date(program: str, year: int, period: int) -> date | None:
    """Map a `(year, period)` pair to its canonical `ref_date` per the program's rule (BEH §4).

    The mapping is dictated by `ProgramSpec.ref_date_rule`: the 12th of the month for
    day-12 programs, the last business day of the month for JOLTS, the 12th of the
    quarter-ending month for quarterly programs, or May 12th for OEWS.

    Args:
        program: Registry key.
        year: Reference year.
        period: Reference period (month, quarter, or 1 for annual programs).

    Returns:
        The canonical `ref_date`, or `None` for EP — which is not periodic and carries no
        `ref_date` at all (ARCH §4.3).
    """
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
