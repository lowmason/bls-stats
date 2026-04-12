"""Tests for BLS programs registry."""

from bls_stats.bls.programs import BD, EN, JT, SM, PROGRAMS


class TestBDProgram:
    def test_registered(self):
        assert "BD" in PROGRAMS
        assert PROGRAMS["BD"] is BD

    def test_series_id_length(self):
        assert BD.series_id_length == 28

    def test_field_count(self):
        assert len(BD.fields) == 11

    def test_prefix(self):
        assert BD.prefix == "BD"


class TestJTProgram:
    def test_registered(self):
        assert "JT" in PROGRAMS
        assert PROGRAMS["JT"] is JT

    def test_series_id_length(self):
        assert JT.series_id_length == 21

    def test_field_count(self):
        assert len(JT.fields) == 8

    def test_prefix(self):
        assert JT.prefix == "JT"


class TestExistingPrograms:
    def test_ces_registered(self):
        assert "CE" in PROGRAMS
        assert PROGRAMS["CE"] is EN

    def test_sae_registered(self):
        assert "SM" in PROGRAMS
        assert PROGRAMS["SM"] is SM
