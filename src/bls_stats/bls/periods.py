"""Reference-period generation by BLS program.

Monthly programs (CES, SAE, JOLTS) use ``YYYY/MM`` periods.
Quarterly programs (QCEW, BED) use ``YYYY/Q`` periods.

Both *start_period* and *end_period* are **inclusive**.
"""

from __future__ import annotations

import re

MONTHLY_PROGRAMS = frozenset({"ces", "sae", "jolts"})
QUARTERLY_PROGRAMS = frozenset({"qcew", "bed"})
ALL_PROGRAMS = MONTHLY_PROGRAMS | QUARTERLY_PROGRAMS

_MONTHLY_RE = re.compile(r"^(\d{4})/(0[1-9]|1[0-2])$")
_QUARTERLY_RE = re.compile(r"^(\d{4})/([1-4])$")


def _parse_monthly(period: str) -> tuple[int, int]:
    m = _MONTHLY_RE.match(period)
    if not m:
        raise ValueError(
            f"Invalid monthly period {period!r}; expected YYYY/MM (e.g. 2024/03)"
        )
    return int(m.group(1)), int(m.group(2))


def _parse_quarterly(period: str) -> tuple[int, int]:
    m = _QUARTERLY_RE.match(period)
    if not m:
        raise ValueError(
            f"Invalid quarterly period {period!r}; expected YYYY/Q (e.g. 2024/1)"
        )
    return int(m.group(1)), int(m.group(2))


def _monthly_range(
    start: tuple[int, int], end: tuple[int, int]
) -> list[tuple[int, int]]:
    sy, sm = start
    ey, em = end
    periods: list[tuple[int, int]] = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        periods.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return periods


def _quarterly_range(
    start: tuple[int, int], end: tuple[int, int]
) -> list[tuple[int, int]]:
    sy, sq = start
    ey, eq = end
    periods: list[tuple[int, int]] = []
    y, q = sy, sq
    while (y, q) <= (ey, eq):
        periods.append((y, q))
        q += 1
        if q > 4:
            q = 1
            y += 1
    return periods


def reference_periods(
    program: str,
    start_period: str,
    end_period: str,
) -> list[tuple[int, int]]:
    """Return every reference period between *start_period* and *end_period* (inclusive).

    Parameters
    ----------
    program:
        BLS program name (case-insensitive): ``ces``, ``sae``, ``jolts``,
        ``qcew``, or ``bed``.
    start_period:
        First period.  ``YYYY/MM`` for monthly programs, ``YYYY/Q`` for
        quarterly programs.
    end_period:
        Last period (inclusive).  Same format as *start_period*.

    Returns
    -------
    list[tuple[int, int]]
        Ordered ``(year, month)`` tuples for monthly programs, or
        ``(year, quarter)`` tuples for quarterly programs.

    Raises
    ------
    ValueError
        If the program is unknown, the period format is wrong, or
        *start_period* is after *end_period*.
    """
    prog = program.lower()
    if prog not in ALL_PROGRAMS:
        raise ValueError(
            f"Unknown program {program!r}; choose from {sorted(ALL_PROGRAMS)}"
        )

    if prog in MONTHLY_PROGRAMS:
        start = _parse_monthly(start_period)
        end = _parse_monthly(end_period)
        if start > end:
            raise ValueError(
                f"start_period {start_period!r} is after end_period {end_period!r}"
            )
        return _monthly_range(start, end)

    start = _parse_quarterly(start_period)
    end = _parse_quarterly(end_period)
    if start > end:
        raise ValueError(
            f"start_period {start_period!r} is after end_period {end_period!r}"
        )
    return _quarterly_range(start, end)
