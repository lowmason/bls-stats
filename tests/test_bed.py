"""Tests for BED downloader."""

from datetime import date
from unittest.mock import patch

import polars as pl

from bls_stats.download.bed import _period_to_quarter, _filter_to_periods


class TestPeriodToQuarter:
    def test_q01(self):
        assert _period_to_quarter("Q01") == 1

    def test_q04(self):
        assert _period_to_quarter("Q04") == 4

    def test_invalid_period(self):
        assert _period_to_quarter("M01") is None

    def test_out_of_range(self):
        assert _period_to_quarter("Q05") is None

    def test_non_numeric(self):
        assert _period_to_quarter("QAB") is None


class TestFilterToPeriods:
    def test_keeps_matching_quarters(self):
        df = pl.DataFrame({
            "year": [2024, 2024, 2024, 2024],
            "period": ["Q01", "Q02", "Q03", "Q04"],
            "value": [10, 20, 30, 40],
        })
        result = _filter_to_periods(df, {(2024, 2), (2024, 3)})
        assert len(result) == 2
        assert result["value"].to_list() == [20, 30]

    def test_excludes_invalid_periods(self):
        df = pl.DataFrame({
            "year": [2024, 2024],
            "period": ["Q01", "M01"],
            "value": [10, 20],
        })
        result = _filter_to_periods(df, {(2024, 1)})
        assert len(result) == 1

    def test_ref_date_uses_last_month_day_12(self):
        df = pl.DataFrame({
            "year": [2024, 2024],
            "period": ["Q01", "Q03"],
            "value": [10, 30],
        })
        result = _filter_to_periods(df, {(2024, 1), (2024, 3)})
        assert result["ref_date"][0] == date(2024, 3, 12)
        assert result["ref_date"][1] == date(2024, 9, 12)


class TestDownloadBED:
    @patch("bls_stats.download.bed.read_tsv")
    def test_end_to_end(self, mock_read_tsv, tmp_path):
        mock_read_tsv.return_value = pl.DataFrame({
            "series_id": ["BDS0000000000000000010101LQ5"],
            "year": [2024],
            "period": ["Q01"],
            "value": ["1234.5"],
            "footnote_codes": [""],
        })

        from bls_stats.download.bed import download_bed

        df = download_bed([(2024, 1)], out_dir=tmp_path)

        assert len(df) == 1
        assert set(df.columns) == {
            "series_id", "year", "period", "value", "footnote_codes",
            "ref_date", "downloaded",
        }
        assert df["ref_date"][0] == date(2024, 3, 12)
        assert (tmp_path / "bed_estimates.parquet").exists()
