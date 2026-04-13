"""Tests for BED downloader."""

from datetime import date
from unittest.mock import patch

import polars as pl

from bls_stats.download.bed import (
    _period_to_quarter_date,
    _classify_geo,
    _add_parsed_fields,
    _filter_to_range,
)


class TestPeriodToQuarterDate:
    def test_q01(self):
        assert _period_to_quarter_date(2024, "Q01") == date(2024, 1, 1)

    def test_q02(self):
        assert _period_to_quarter_date(2024, "Q02") == date(2024, 4, 1)

    def test_q03(self):
        assert _period_to_quarter_date(2024, "Q03") == date(2024, 7, 1)

    def test_q04(self):
        assert _period_to_quarter_date(2024, "Q04") == date(2024, 10, 1)

    def test_invalid_period(self):
        assert _period_to_quarter_date(2024, "M01") is None

    def test_unknown_quarter(self):
        assert _period_to_quarter_date(2024, "Q05") is None


class TestClassifyGeo:
    def test_national(self):
        assert _classify_geo("0000000000") == ("national", "US")

    def test_state(self):
        assert _classify_geo("0100000000") == ("state", "01")

    def test_area(self):
        assert _classify_geo("0100100000") == ("area", "0100100000")


class TestAddParsedFields:
    def test_parses_series_id(self):
        # BDS0000000000000000010101LQ5 -> 28 chars
        df = pl.DataFrame({
            "series_id": ["BDS0000000000000000010101LQ5"],
        })
        result = _add_parsed_fields(df)
        assert result["seasonal"][0] == "S"
        assert result["area_code"][0] == "0000000000"
        assert result["industry_code"][0] == "000000"
        assert result["ownership"][0] == "5"


class TestFilterToRange:
    def test_filters_quarters(self):
        df = pl.DataFrame({
            "year": [2024, 2024, 2024, 2024],
            "period": ["Q01", "Q02", "Q03", "Q04"],
            "value": [10, 20, 30, 40],
        })
        result = _filter_to_range(df, date(2024, 4, 1), date(2024, 9, 30))
        assert len(result) == 2

    def test_excludes_invalid_periods(self):
        df = pl.DataFrame({
            "year": [2024, 2024],
            "period": ["Q01", "M01"],
            "value": [10, 20],
        })
        result = _filter_to_range(df, date(2024, 1, 1), date(2024, 12, 31))
        assert len(result) == 1


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

        df = download_bed(date(2024, 1, 1), date(2024, 3, 31), out_dir=tmp_path)

        assert len(df) == 1
        assert "source" in df.columns
        assert "geographic_type" in df.columns
        assert "seasonally_adjusted" in df.columns
        assert df["source"][0] == "bed"
        assert df["geographic_type"][0] == "national"
        assert (tmp_path / "bed_estimates.parquet").exists()
