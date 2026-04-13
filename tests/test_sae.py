"""Tests for SAE downloader."""

from datetime import date
from unittest.mock import patch

import polars as pl

from bls_stats.download.sae import _period_to_month, _classify_geo, _add_parsed_fields, _filter_to_range


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


class TestClassifyGeo:
    def test_national(self):
        assert _classify_geo("00", "00000") == ("national", "US")

    def test_state(self):
        assert _classify_geo("06", "00000") == ("state", "06")

    def test_area(self):
        assert _classify_geo("06", "31080") == ("area", "0631080")


class TestAddParsedFields:
    def test_parses_series_id(self):
        # SMU0600000000000001 -> 20 chars: prefix(2) + seasonal(1) + state(2) + area(5) + supersector(2) + industry(6) + data_type(2)
        df = pl.DataFrame({
            "series_id": ["SMU0600000000000001"],
        })
        result = _add_parsed_fields(df)
        assert result["seasonal"][0] == "U"
        assert result["state"][0] == "06"
        assert result["area"][0] == "00000"


class TestFilterToRange:
    def test_filters_months(self):
        df = pl.DataFrame({
            "year": [2024, 2024, 2024],
            "period": ["M01", "M06", "M12"],
            "value": [10, 20, 30],
        })
        result = _filter_to_range(df, date(2024, 1, 1), date(2024, 6, 30))
        assert len(result) == 2

    def test_excludes_annual_avg(self):
        df = pl.DataFrame({
            "year": [2024, 2024],
            "period": ["M01", "M13"],
            "value": [10, 20],
        })
        result = _filter_to_range(df, date(2024, 1, 1), date(2024, 12, 31))
        assert len(result) == 1


class TestDownloadSAE:
    @patch("bls_stats.download.sae.read_tsv")
    def test_end_to_end(self, mock_read_tsv, tmp_path):
        mock_read_tsv.return_value = pl.DataFrame({
            "series_id": ["SMS0600000000000001"],
            "year": [2024],
            "period": ["M01"],
            "value": ["75000"],
            "footnote_codes": [""],
        })

        from bls_stats.download.sae import download_sae

        df = download_sae(date(2024, 1, 1), date(2024, 3, 31), out_dir=tmp_path)

        assert len(df) == 1
        assert df["source"][0] == "sae"
        assert (tmp_path / "sae_estimates.parquet").exists()
