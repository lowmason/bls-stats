"""Tests for QCEW downloader."""

from datetime import date
from unittest.mock import patch

import polars as pl

from bls_stats.download.qcew import _filter_to_periods, _ref_date_from_quarter


class TestFilterToPeriods:
    def test_keeps_matching_quarters(self):
        df = pl.DataFrame({
            "year": [2024, 2024, 2024, 2024],
            "qtr": [1, 2, 3, 4],
            "value": [10, 20, 30, 40],
        })
        result = _filter_to_periods(df, {(2024, 2), (2024, 3)})
        assert len(result) == 2
        assert result["qtr"].to_list() == [2, 3]

    def test_cross_year(self):
        df = pl.DataFrame({
            "year": [2023, 2023, 2024, 2024],
            "qtr": [3, 4, 1, 2],
            "value": [10, 20, 30, 40],
        })
        result = _filter_to_periods(df, {(2023, 4), (2024, 1)})
        assert len(result) == 2
        assert result["year"].to_list() == [2023, 2024]


class TestRefDateFromQuarter:
    def test_q1(self):
        assert _ref_date_from_quarter(2024, 1) == date(2024, 3, 12)

    def test_q2(self):
        assert _ref_date_from_quarter(2024, 2) == date(2024, 6, 12)

    def test_q3(self):
        assert _ref_date_from_quarter(2024, 3) == date(2024, 9, 12)

    def test_q4(self):
        assert _ref_date_from_quarter(2024, 4) == date(2024, 12, 12)


class TestDownloadQCEW:
    @patch("bls_stats.download.qcew.read_zip_csvs")
    def test_end_to_end(self, mock_read_zip, tmp_path):
        mock_read_zip.return_value = pl.DataFrame({
            "area_fips": ["US000", "06000", "06037"],
            "industry_code": ["10", "10", "10"],
            "own_code": ["0", "0", "0"],
            "size_code": ["0", "0", "0"],
            "year": [2024, 2024, 2024],
            "qtr": [1, 1, 1],
            "disclosure_code": ["", "", ""],
            "qtrly_estabs": [100, 50, 25],
            "month1_emplvl": [150000, 75000, 30000],
        })

        from bls_stats.download.qcew import download_qcew

        df = download_qcew([(2024, 1)], out_dir=tmp_path)

        assert len(df) == 3
        assert "ref_date" in df.columns
        assert "downloaded" in df.columns
        assert df["ref_date"][0] == date(2024, 3, 12)
        assert (tmp_path / "qcew_estimates.parquet").exists()
