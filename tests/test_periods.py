"""Tests for reference-period generation."""

import pytest

from bls_stats.bls.periods import reference_periods


class TestMonthlyPeriods:
    def test_single_month(self):
        assert reference_periods("ces", "2024/06", "2024/06") == [(2024, 6)]

    def test_full_year(self):
        result = reference_periods("sae", "2024/01", "2024/12")
        assert len(result) == 12
        assert result[0] == (2024, 1)
        assert result[-1] == (2024, 12)

    def test_cross_year_boundary(self):
        result = reference_periods("jolts", "2023/11", "2024/02")
        assert result == [(2023, 11), (2023, 12), (2024, 1), (2024, 2)]

    def test_start_and_end_inclusive(self):
        result = reference_periods("ces", "2024/03", "2024/05")
        assert (2024, 3) in result
        assert (2024, 5) in result
        assert len(result) == 3

    def test_case_insensitive(self):
        assert reference_periods("CES", "2024/01", "2024/03") == [
            (2024, 1),
            (2024, 2),
            (2024, 3),
        ]


class TestQuarterlyPeriods:
    def test_single_quarter(self):
        assert reference_periods("qcew", "2024/2", "2024/2") == [(2024, 2)]

    def test_full_year(self):
        result = reference_periods("bed", "2024/1", "2024/4")
        assert result == [(2024, 1), (2024, 2), (2024, 3), (2024, 4)]

    def test_cross_year_boundary(self):
        result = reference_periods("qcew", "2023/3", "2024/2")
        assert result == [(2023, 3), (2023, 4), (2024, 1), (2024, 2)]

    def test_start_and_end_inclusive(self):
        result = reference_periods("bed", "2024/1", "2024/3")
        assert (2024, 1) in result
        assert (2024, 3) in result
        assert len(result) == 3


class TestValidation:
    def test_unknown_program(self):
        with pytest.raises(ValueError, match="Unknown program"):
            reference_periods("fake", "2024/01", "2024/03")

    def test_bad_monthly_format(self):
        with pytest.raises(ValueError, match="Invalid monthly period"):
            reference_periods("ces", "2024/1", "2024/3")

    def test_bad_quarterly_format(self):
        with pytest.raises(ValueError, match="Invalid quarterly period"):
            reference_periods("qcew", "2024/01", "2024/03")

    def test_start_after_end_monthly(self):
        with pytest.raises(ValueError, match="after"):
            reference_periods("ces", "2024/06", "2024/01")

    def test_start_after_end_quarterly(self):
        with pytest.raises(ValueError, match="after"):
            reference_periods("qcew", "2024/3", "2024/1")
