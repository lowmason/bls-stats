"""Tests for QCEW downloader."""

from datetime import date
from unittest.mock import MagicMock, patch

import polars as pl

from bls_stats.download.qcew import _classify_area, _ref_date_to_qtr, download_qcew


class TestClassifyArea:
    def test_national(self):
        assert _classify_area("US000") == ("national", "US")

    def test_state(self):
        assert _classify_area("06000") == ("state", "06")

    def test_county(self):
        assert _classify_area("06037") == ("county", "06037")

    def test_msa(self):
        assert _classify_area("C3108") == ("msa", "C3108")


class TestRefDateToQtr:
    def test_jan(self):
        assert _ref_date_to_qtr(date(2024, 1, 12)) == (2024, 1)

    def test_apr(self):
        assert _ref_date_to_qtr(date(2024, 4, 12)) == (2024, 2)

    def test_oct(self):
        assert _ref_date_to_qtr(date(2024, 10, 12)) == (2024, 4)


class TestDownloadQCEW:
    @patch("bls_stats.download.qcew.QCEWClient")
    def test_industry_only(self, mock_client_cls, tmp_path):
        mock_csv = pl.DataFrame(
            {
                "area_fips": ["US000"],
                "industry_code": ["10"],
                "year": [2024],
                "qtr": [1],
                "month1_emplvl": [150000],
            }
        )
        mock_instance = mock_client_cls.return_value
        mock_instance.get_industry.return_value = mock_csv

        df = download_qcew(date(2024, 1, 12), slices=["industry"], out_dir=tmp_path)

        assert "source" in df.columns
        assert "employment" in df.columns
        assert len(df) > 0
