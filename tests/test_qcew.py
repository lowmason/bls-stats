"""Tests for QCEW downloader."""

from datetime import date
from unittest.mock import patch

import polars as pl

from bls_stats.download.qcew import _classify_area, _quarter_for_month, _filter_to_range


class TestClassifyArea:
    def test_national(self):
        assert _classify_area("US000") == ("national", "US")

    def test_state(self):
        assert _classify_area("06000") == ("state", "06")

    def test_county(self):
        assert _classify_area("06037") == ("county", "06037")

    def test_msa(self):
        assert _classify_area("C3108") == ("msa", "C3108")

    def test_whitespace_stripped(self):
        assert _classify_area("  US000  ") == ("national", "US")


class TestQuarterForMonth:
    def test_jan(self):
        assert _quarter_for_month(1) == 1

    def test_apr(self):
        assert _quarter_for_month(4) == 2

    def test_jul(self):
        assert _quarter_for_month(7) == 3

    def test_oct(self):
        assert _quarter_for_month(10) == 4

    def test_dec(self):
        assert _quarter_for_month(12) == 4


class TestFilterToRange:
    def test_filters_by_quarter(self):
        df = pl.DataFrame({
            "year": [2024, 2024, 2024, 2024],
            "qtr": [1, 2, 3, 4],
            "value": [10, 20, 30, 40],
        })
        result = _filter_to_range(df, date(2024, 4, 1), date(2024, 9, 30))
        assert len(result) == 2
        assert result["qtr"].to_list() == [2, 3]

    def test_cross_year(self):
        df = pl.DataFrame({
            "year": [2023, 2023, 2024, 2024],
            "qtr": [3, 4, 1, 2],
            "value": [10, 20, 30, 40],
        })
        result = _filter_to_range(df, date(2023, 10, 1), date(2024, 3, 31))
        assert len(result) == 2
        assert result["year"].to_list() == [2023, 2024]


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

        df = download_qcew(date(2024, 1, 1), date(2024, 3, 31), out_dir=tmp_path)

        assert len(df) == 3
        assert "downloaded" in df.columns
        assert (tmp_path / "qcew_estimates.parquet").exists()
