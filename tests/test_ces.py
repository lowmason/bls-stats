"""Tests for CES downloader."""

from datetime import date
from unittest.mock import patch

import polars as pl

from bls_stats.download.ces import _period_to_month, _filter_to_periods


class TestPeriodToMonth:
    def test_m01(self):
        assert _period_to_month("M01") == 1

    def test_m12(self):
        assert _period_to_month("M12") == 12

    def test_m13_annual_avg(self):
        assert _period_to_month("M13") is None

    def test_quarter_code(self):
        assert _period_to_month("Q01") is None

    def test_non_numeric(self):
        assert _period_to_month("MAB") is None


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

    def test_excludes_annual_avg(self):
        df = pl.DataFrame({
            "year": [2024, 2024],
            "period": ["M01", "M13"],
            "value": [10, 20],
        })
        result = _filter_to_periods(df, {(2024, 1), (2024, 13)})
        assert len(result) == 1

    def test_ref_date_day_is_12(self):
        df = pl.DataFrame({
            "year": [2024],
            "period": ["M03"],
            "value": [10],
        })
        result = _filter_to_periods(df, {(2024, 3)})
        assert result["ref_date"][0] == date(2024, 3, 12)


class TestDownloadCES:
    @patch("bls_stats.download.ces.read_tsv")
    def test_end_to_end(self, mock_read_tsv, tmp_path):
        mock_read_tsv.return_value = pl.DataFrame({
            "series_id": ["CES0000000001"],
            "year": [2024],
            "period": ["M01"],
            "value": ["157000"],
            "footnote_codes": [""],
        })

        from bls_stats.download.ces import download_ces

        df = download_ces([(2024, 1)], out_dir=tmp_path)

        assert len(df) == 1
        assert set(df.columns) == {
            "series_id", "year", "period", "value", "footnote_codes",
            "month", "ref_date", "downloaded",
        }
        assert df["ref_date"][0] == date(2024, 1, 12)
        assert (tmp_path / "ces_estimates.parquet").exists()
