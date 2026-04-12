"""Tests for series ID builder functions."""

from bls_stats.series.builder import bd_series_id, ce_series_id, jt_series_id, sm_series_id


class TestBDSeriesID:
    def test_default_length(self):
        sid = bd_series_id()
        assert len(sid) == 28

    def test_starts_with_prefix(self):
        sid = bd_series_id()
        assert sid[:2] == "BD"

    def test_known_example(self):
        sid = bd_series_id(
            area_code="0000000000",
            industry_code="000000",
            data_element="1",
            seasonal="S",
            unit_analysis="0",
            size_class="01",
            data_class="01",
            rate_level="L",
            record_type="Q",
            ownership="5",
        )
        assert sid == "BDS0000000000000000010101LQ5"

    def test_seasonal_unseasonal(self):
        sa = bd_series_id(seasonal="S")
        nsa = bd_series_id(seasonal="U")
        assert sa[2] == "S"
        assert nsa[2] == "U"

    def test_state_area_code(self):
        sid = bd_series_id(area_code="0100000000")
        assert sid[3:13] == "0100000000"


class TestJTSeriesID:
    def test_default_length(self):
        sid = jt_series_id()
        assert len(sid) == 21

    def test_starts_with_prefix(self):
        sid = jt_series_id()
        assert sid[:2] == "JT"

    def test_national_job_openings_level(self):
        sid = jt_series_id(
            industry_code="000000",
            state_code="00",
            area_code="00000",
            data_element="JO",
            rate_level="L",
            seasonal="S",
            size_class="00",
        )
        assert sid == "JTS000000000000000JOL"

    def test_state_series(self):
        sid = jt_series_id(
            industry_code="000000",
            state_code="06",
            area_code="00000",
            data_element="HI",
            rate_level="R",
        )
        assert sid[9:11] == "06"
        assert sid[-3:-1] == "HI"
        assert sid[-1] == "R"


class TestCESeriesID:
    def test_length(self):
        sid = ce_series_id(supersector="00", industry="000000", data_type="01")
        assert len(sid) == 13

    def test_prefix(self):
        sid = ce_series_id(supersector="00", industry="000000", data_type="01")
        assert sid[:2] == "CE"


class TestSMSeriesID:
    def test_length(self):
        sid = sm_series_id(
            state="06", area="00000", supersector="00",
            industry="000000", data_type="01",
        )
        assert len(sid) == 20

    def test_prefix(self):
        sid = sm_series_id(
            state="06", area="00000", supersector="00",
            industry="000000", data_type="01",
        )
        assert sid[:2] == "SM"
