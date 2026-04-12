"""Tests for JOLTS downloader."""

from datetime import date
from unittest.mock import patch

from bls_stats.bls.client import SeriesResult
from bls_stats.download.jolts import _build_series_ids, _period_to_date, download_jolts


class TestPeriodToDate:
    def test_m01(self):
        assert _period_to_date(2024, "M01") == date(2024, 1, 12)

    def test_m12(self):
        assert _period_to_date(2024, "M12") == date(2024, 12, 12)

    def test_invalid_quarter(self):
        assert _period_to_date(2024, "Q01") is None

    def test_m13(self):
        assert _period_to_date(2024, "M13") is None


class TestBuildSeriesIDs:
    def test_returns_nonempty(self):
        meta = _build_series_ids()
        assert len(meta) > 0

    def test_all_21_chars(self):
        meta = _build_series_ids()
        for m in meta:
            assert len(m["series_id"]) == 21, f"Bad length for {m['series_id']}"

    def test_has_required_keys(self):
        meta = _build_series_ids()
        required = {
            "series_id", "seasonally_adjusted", "geographic_type",
            "geographic_code", "industry_type", "industry_code", "measure",
        }
        for m in meta:
            assert required.issubset(m.keys())

    def test_includes_all_geo_types(self):
        meta = _build_series_ids()
        geo_types = {m["geographic_type"] for m in meta}
        assert "national" in geo_types
        assert "state" in geo_types
        assert "region" in geo_types


class TestDownloadJOLTS:
    @patch("bls_stats.download.jolts.BLSClient")
    def test_output_schema(self, mock_client_cls, tmp_path):
        meta = _build_series_ids()
        real_sid = meta[0]["series_id"]

        mock_instance = mock_client_cls.return_value
        mock_instance.get_series.return_value = [
            SeriesResult(
                series_id=real_sid,
                data=[{"year": 2024, "period": "M01", "value": "8765.0", "footnotes": []}],
            )
        ]

        df = download_jolts(date(2024, 1, 12), "fake-key", out_dir=tmp_path)

        expected_cols = {
            "source", "series_id", "seasonally_adjusted", "geographic_type",
            "geographic_code", "industry_type", "industry_code", "measure",
            "ref_date", "value",
        }
        assert set(df.columns) == expected_cols
        assert len(df) == 1
