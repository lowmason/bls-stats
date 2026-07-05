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
    """One `(ref_date, revision, benchmark)` fetch-plan entry from expanding a `Release`.

    `expand()` guarantees at most one `Slot` per `ref_date` per release (ARCH §2.2): a
    benchmark release's window is deduped against its routine slots before `Slot`s are built.

    Attributes:
        ref_date: The period this slot covers.
        revision: Structural print number (ARCH §2.1) — the release's routine slot index for
            `kind="routine"`, or the terminal revision (`routine_slots - 1`, or `4 - quarter`
            for QCEW) for `kind="benchmark_window"`.
        benchmark: Benchmark counter: unchanged from the prior count for `kind="routine"`
            slots, or prior count `+ 1` for `kind="benchmark_window"` slots (null prior counts
            as 0, ARCH §2.1).
        kind: `"routine"` (a structurally-carried print) or `"benchmark_window"` (a
            re-snapshot slot only present because this release is a benchmark event).
    """

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
    """Revision number for a window-only (non-routine) slot in a benchmark release (ARCH §2.2).

    For QCEW (`routine_rule="year_to_date"`), a prior-year quarter q was routinely printed at
    each release from q's own through Q4's — 4-q routine releases — so its terminal revision
    is `4 - q`, e.g. Q1 reaches revision 3 by the time Q4 of the same year is released. Every
    other program uses a fixed slot count, so the terminal revision is simply the last routine
    slot index, `routine_slots - 1`.
    """
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
    """Expand one `Release` event into its fetch-plan `Slot`s (ARCH §2.2, §5.3).

    Every release contributes its routine slots — the prints it structurally carries, per
    `RevisionProfile.routine_slots` (or QCEW's `year_to_date` rule: every quarter of the
    reference year so far). A benchmark release additionally contributes a windowed snapshot
    from January (or Q1) of `ref_year - benchmark_window_years` through the newest period;
    `ref_date`s already covered by a routine slot are deduped out of the window rather than
    duplicated, guaranteeing exactly one `Slot` per `ref_date`. Window-only slots get the
    terminal revision (`_terminal_revision`) and `benchmark = prior_benchmark(ref_date) + 1`;
    routine slots keep `benchmark = prior_benchmark(ref_date)` unchanged.

    Args:
        release: The detected release event to expand.
        prior_benchmark: Callback returning the benchmark counter already on record for a
            given `ref_date` (a null/absent prior counts as 0, ARCH §2.1) — injected so this
            function stays pure and ledger-agnostic; callers typically close over the ledger.

    Returns:
        `Slot`s sorted by `ref_date` descending (newest first).
    """
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
