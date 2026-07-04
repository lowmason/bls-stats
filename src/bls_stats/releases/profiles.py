"""Release → fetch-plan expansion (ARCH §2.2, §5.3). Pure: ledger context is injected."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date

from bls_stats.core.periods import Period, ref_date, shift
from bls_stats.registry import REGISTRY, Frequency
from bls_stats.releases.feeds import Release


@dataclass(frozen=True)
class Slot:
    ref_date: date
    revision: int
    benchmark: int
    kind: str  # "routine" | "benchmark_window"


def _routine_periods(release: Release) -> list[tuple[Period, int]]:
    """[(period, revision)] the release structurally carries (ARCH §2.1)."""
    spec = REGISTRY[release.program]
    newest = (release.ref_year, release.ref_period)
    if spec.profile.routine_rule == "year_to_date":  # QCEW: all quarters of ref year so far
        return [(shift(release.program, *newest, -i), i) for i in range(release.ref_period)]
    return [(shift(release.program, *newest, -i), i) for i in range(spec.profile.routine_slots)]


def _terminal_revision(program: str, period: Period, release: Release) -> int:
    spec = REGISTRY[program]
    if spec.profile.routine_rule == "year_to_date":
        return 4 - period[1]  # quarter q of the completed prior year: prints at q..Q4 releases
    return spec.profile.routine_slots - 1


def _window_periods(release: Release) -> list[Period]:
    """ARCH §2.2: January/Q1 of (newest_year - window_years) through newest, inclusive."""
    spec = REGISTRY[release.program]
    years = spec.profile.benchmark_window_years or 0
    if spec.profile.routine_rule == "year_to_date":  # QCEW: the prior calendar year(s)
        out: list[Period] = []
        for y in range(release.ref_year - years, release.ref_year):
            out.extend((y, q) for q in range(1, 5))
        return out
    n = 12 if spec.frequency == Frequency.MONTHLY else 4
    start = (release.ref_year - years, 1)
    out = []
    cur = start
    while cur <= (release.ref_year, release.ref_period):
        out.append(cur)
        cur = shift(release.program, *cur, 1)
        if len(out) > years * n + n:  # safety bound
            break
    return out


def expand(release: Release, prior_benchmark: Callable[[date], int]) -> list[Slot]:
    program = release.program
    slots: dict[date, Slot] = {}
    for period, revision in _routine_periods(release):
        rd = ref_date(program, *period)
        assert rd is not None
        slots[rd] = Slot(rd, revision, prior_benchmark(rd), "routine")
    if release.is_benchmark:
        for period in _window_periods(release):
            rd = ref_date(program, *period)
            assert rd is not None
            if rd in slots:  # one row per ref_date per release (ARCH §2.2)
                continue
            slots[rd] = Slot(
                rd,
                _terminal_revision(program, period, release),
                prior_benchmark(rd) + 1,
                "benchmark_window",
            )
    return sorted(slots.values(), key=lambda s: s.ref_date, reverse=True)
