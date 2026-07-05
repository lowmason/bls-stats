from datetime import date

from bls_stats.releases.feeds import Release
from bls_stats.releases.profiles import expand


def no_priors(_ref: date) -> int:
    return 0


def test_ces_routine_release_three_slots() -> None:
    slots = expand(Release("ces", date(2026, 7, 2), 2026, 6, False), no_priors)
    assert [(s.ref_date, s.revision, s.benchmark, s.kind) for s in slots] == [
        (date(2026, 6, 12), 0, 0, "routine"),
        (date(2026, 5, 12), 1, 0, "routine"),
        (date(2026, 4, 12), 2, 0, "routine"),
    ]


def test_ces_benchmark_release_one_row_per_ref_date() -> None:  # ARCH §2.2 blocker fix
    slots = expand(Release("ces", date(2026, 2, 11), 2026, 1, True), no_priors)
    by_ref = {s.ref_date: s for s in slots}
    assert len(slots) == len(by_ref)  # no ref_date appears twice
    # routine slots keep (slot, prior_count):
    assert (by_ref[date(2026, 1, 12)].revision, by_ref[date(2026, 1, 12)].benchmark) == (0, 0)
    assert (by_ref[date(2025, 12, 12)].revision, by_ref[date(2025, 12, 12)].benchmark) == (1, 0)
    assert (by_ref[date(2025, 11, 12)].revision, by_ref[date(2025, 11, 12)].benchmark) == (2, 0)
    # window-only slots get terminal revision + benchmark increment:
    oct_2025 = by_ref[date(2025, 10, 12)]
    assert (oct_2025.revision, oct_2025.benchmark, oct_2025.kind) == (2, 1, "benchmark_window")
    # window start: January of (2026 - 5):
    assert min(by_ref) == date(2021, 1, 12)


def test_benchmark_counter_uses_prior_counts() -> None:
    def priors(ref: date) -> int:
        return 3 if ref == date(2025, 6, 12) else 0

    slots = expand(Release("ces", date(2026, 2, 11), 2026, 1, True), priors)
    jun25 = next(s for s in slots if s.ref_date == date(2025, 6, 12))
    assert jun25.benchmark == 4  # prior 3 + 1


def test_jolts_ref_dates_use_last_business_day() -> None:
    slots = expand(Release("jolts", date(2026, 6, 30), 2026, 5, False), no_priors)
    assert slots[0].ref_date == date(2026, 5, 29)  # May 31 2026 is a Sunday


def test_qcew_year_to_date_routine() -> None:  # ARCH §6.2 touched set
    slots = expand(Release("qcew", date(2026, 6, 4), 2025, 4, False), no_priors)
    assert [(s.ref_date, s.revision) for s in slots] == [
        (date(2025, 12, 12), 0),
        (date(2025, 9, 12), 1),
        (date(2025, 6, 12), 2),
        (date(2025, 3, 12), 3),
    ]


def test_qcew_q1_benchmark_pulls_prior_year() -> None:
    slots = expand(Release("qcew", date(2026, 9, 3), 2026, 1, True), no_priors)
    by_ref = {s.ref_date: s for s in slots}
    assert (by_ref[date(2026, 3, 12)].revision, by_ref[date(2026, 3, 12)].benchmark) == (0, 0)
    # prior-year quarters: terminal revision 4-q, benchmark+1 (spec lifecycle (3,0)→(3,1)):
    assert (by_ref[date(2025, 3, 12)].revision, by_ref[date(2025, 3, 12)].benchmark) == (3, 1)
    assert (by_ref[date(2025, 12, 12)].revision, by_ref[date(2025, 12, 12)].benchmark) == (0, 1)


def test_cps_single_slot() -> None:
    slots = expand(Release("cps", date(2026, 7, 2), 2026, 6, False), no_priors)
    assert len(slots) == 1 and slots[0].revision == 0
