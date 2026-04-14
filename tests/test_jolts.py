"""Tests for JOLTS downloader."""

from datetime import date
from unittest.mock import patch

import polars as pl

from bls_stats.download.jolts import (
    _period_to_month,
    _last_business_day,
    _filter_to_periods,
)


class TestPeriodToMonth:
    def test_m01(self):
        assert _period_to_month("M01") == 1

    def test_m12(self):
        assert _period_to_month("M12") == 12

    def test_m13_annual_avg(self):
        assert _period_to_month("M13") is None

    def test_quarter_code(self):
        assert _period_to_month("Q01") is None

    def test_invalid(self):
        assert _period_to_month("X99") is None

    def test_non_numeric(self):
        assert _period_to_month("MAB") is None


class TestLastBusinessDay:
    def test_month_ending_on_friday(self):
        # Jan 2024 ends on Wednesday
        assert _last_business_day(2024, 1) == date(2024, 1, 31)

    def test_month_ending_on_saturday(self):
        # Mar 2024 ends on Sunday -> last business day is Friday 29th
        assert _last_business_day(2024, 3) == date(2024, 3, 29)

    def test_month_ending_on_sunday(self):
        # Jun 2024 ends on Sunday -> last business day is Friday 28th
        assert _last_business_day(2024, 6) == date(2024, 6, 28)

    def test_feb_leap_year(self):
        # Feb 2024 (leap) ends on Thursday
        assert _last_business_day(2024, 2) == date(2024, 2, 29)


class TestFilterToPeriods:
    def test_keeps_matching_periods(self):
        df = pl.DataFrame({
            "year": [2024, 2024, 2024],
            "period": ["M01", "M06", "M12"],
            "value": [10, 20, 30],
        })
        result = _filter_to_periods(df, {(2024, 1), (2024, 6)})
        assert len(result) == 2
        assert result["value"].to_list() == [10, 20]

    def test_excludes_m13(self):
        df = pl.DataFrame({
            "year": [2024, 2024],
            "period": ["M01", "M13"],
            "value": [10, 20],
        })
        result = _filter_to_periods(df, {(2024, 1)})
        assert len(result) == 1

    def test_ref_date_is_last_business_day(self):
        df = pl.DataFrame({
            "year": [2024],
            "period": ["M03"],
            "value": [10],
        })
        result = _filter_to_periods(df, {(2024, 3)})
        assert result["ref_date"][0] == date(2024, 3, 29)

    def test_month_not_in_output(self):
        df = pl.DataFrame({
            "year": [2024],
            "period": ["M01"],
            "value": [10],
        })
        result = _filter_to_periods(df, {(2024, 1)})
        assert "month" not in result.columns
        assert "_month" not in result.columns


class TestDownloadJOLTS:
    @patch("bls_stats.download.jolts.read_tsv")
    def test_end_to_end(self, mock_read_tsv, tmp_path):
        mock_read_tsv.return_value = pl.DataFrame({
            "series_id": ["JTS000000000000000JOL"],
            "year": [2024],
            "period": ["M01"],
            "value": ["8765.0"],
            "footnote_codes": [""],
        })

        from bls_stats.download.jolts import download_jolts

        df = download_jolts([(2024, 1)], out_dir=tmp_path)

        assert len(df) == 1
        assert set(df.columns) == {
            "series_id", "year", "period", "value", "footnote_codes",
            "ref_date", "downloaded",
        }
        assert df["ref_date"][0] == date(2024, 1, 31)
        assert (tmp_path / "jolts_estimates.parquet").exists()
