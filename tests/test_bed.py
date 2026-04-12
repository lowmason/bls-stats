"""Tests for BED downloader."""

from datetime import date
from unittest.mock import patch

from bls_stats.bls.client import SeriesResult
from bls_stats.download.bed import _build_series_ids, _period_to_date, download_bed


class TestPeriodToDate:
    def test_q01(self):
        assert _period_to_date(2024, "Q01") == date(2024, 1, 12)

    def test_q04(self):
        assert _period_to_date(2024, "Q04") == date(2024, 10, 12)

    def test_invalid(self):
        assert _period_to_date(2024, "M01") is None

    def test_unknown_quarter(self):
        assert _period_to_date(2024, "Q05") is None


class TestBuildSeriesIDs:
    def test_returns_nonempty(self):
        meta = _build_series_ids()
        assert len(meta) > 0

    def test_all_28_chars(self):
        meta = _build_series_ids()
        for m in meta:
            assert len(m["series_id"]) == 28, f"Bad length for {m['series_id']}"

    def test_has_required_keys(self):
        meta = _build_series_ids()
        required = {
            "series_id", "seasonally_adjusted", "geographic_type",
            "geographic_code", "industry_type", "industry_code", "measure",
        }
        for m in meta:
            assert required.issubset(m.keys())

    def test_includes_national_and_state(self):
        meta = _build_series_ids()
        geo_types = {m["geographic_type"] for m in meta}
        assert "national" in geo_types
        assert "state" in geo_types


class TestDownloadBED:
    @patch("bls_stats.download.bed.BLSClient")
    def test_output_schema(self, mock_client_cls, tmp_path):
        meta = _build_series_ids()
        real_sid = meta[0]["series_id"]

        mock_instance = mock_client_cls.return_value
        mock_instance.get_series.return_value = [
            SeriesResult(
                series_id=real_sid,
                data=[{"year": 2024, "period": "Q01", "value": "1234.5", "footnotes": []}],
            )
        ]

        df = download_bed(date(2024, 1, 12), "fake-key", out_dir=tmp_path)

        expected_cols = {
            "source", "series_id", "seasonally_adjusted", "geographic_type",
            "geographic_code", "industry_type", "industry_code", "measure",
            "ref_date", "value",
        }
        assert set(df.columns) == expected_cols
        assert len(df) == 1
