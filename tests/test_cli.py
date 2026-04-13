"""Tests for CLI argument parsing and dispatch."""

from click.testing import CliRunner

from bls_stats.__main__ import cli, _parse_month, _parse_year_range


class TestParseMonth:
    def test_valid(self):
        assert _parse_month("2024-01").month == 1
        assert _parse_month("2024-01").year == 2024

    def test_invalid_raises(self):
        import click
        import pytest

        with pytest.raises(click.BadParameter):
            _parse_month("not-a-date")


class TestParseYearRange:
    def test_single_year(self):
        sd, ed = _parse_year_range("2024")
        assert sd.year == 2024 and sd.month == 1
        assert ed.year == 2024 and ed.month == 12

    def test_range(self):
        sd, ed = _parse_year_range("2020-2024")
        assert sd.year == 2020
        assert ed.year == 2024

    def test_invalid_raises(self):
        import click
        import pytest

        with pytest.raises(click.BadParameter):
            _parse_year_range("2020-2022-2024")


class TestDownloadCommand:
    def test_requires_program(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["download", "--start-date", "2024-01", "--end-date", "2024-03"])
        assert result.exit_code != 0

    def test_requires_date(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["download", "--program", "qcew"])
        assert result.exit_code != 0

    def test_invalid_program(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["download", "--program", "invalid", "--year", "2024"])
        assert result.exit_code != 0

    def test_cannot_use_year_and_date(self):
        runner = CliRunner()
        result = runner.invoke(
            cli, ["download", "--program", "qcew", "--year", "2024", "--start-date", "2024-01"]
        )
        assert result.exit_code != 0


class TestReleaseDatesCommand:
    def test_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["release-dates", "--help"])
        assert result.exit_code == 0
        assert "Scrape" in result.output
