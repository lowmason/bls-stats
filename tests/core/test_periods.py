from datetime import date

import pytest

from bls_stats.core.periods import PeriodError, ref_date, reference_periods, shift


def test_monthly_inclusive_range() -> None:
    assert reference_periods("ces", "2025/11", "2026/02") == [
        (2025, 11),
        (2025, 12),
        (2026, 1),
        (2026, 2),
    ]


def test_quarterly_and_annual_parsing() -> None:
    assert reference_periods("qcew", "2024/03", "2025/01") == [(2024, 3), (2024, 4), (2025, 1)]
    assert reference_periods("oews", "2022", "2024") == [(2022, 1), (2023, 1), (2024, 1)]


@pytest.mark.parametrize(
    ("program", "start", "end"),
    [
        ("ces", "2026/13", "2026/13"),
        ("qcew", "2026/5", "2026/5"),
        ("ces", "2026/03", "2026/01"),
        ("nope", "2026/01", "2026/01"),
    ],
)
def test_invalid_inputs_raise(program: str, start: str, end: str) -> None:
    with pytest.raises(PeriodError):
        reference_periods(program, start, end)


def test_ref_date_rules() -> None:  # BEH §4
    assert ref_date("ces", 2026, 6) == date(2026, 6, 12)
    assert ref_date("jolts", 2026, 2) == date(2026, 2, 27)  # Feb 28 2026 is a Saturday
    assert ref_date("jolts", 2026, 5) == date(2026, 5, 29)  # May 31 2026 is a Sunday
    assert ref_date("qcew", 2026, 1) == date(2026, 3, 12)
    assert ref_date("bed", 2025, 4) == date(2025, 12, 12)
    assert ref_date("oews", 2025, 1) == date(2025, 5, 12)
    assert ref_date("ep", 2026, 1) is None


def test_shift_monthly_across_year() -> None:
    assert shift("ces", 2026, 1, -2) == (2025, 11)
    assert shift("qcew", 2026, 1, -1) == (2025, 4)
