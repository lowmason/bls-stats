"""Tests for CLI argument parsing and dispatch."""

from click.testing import CliRunner

from bls_stats.__main__ import cli


class TestDownloadCommand:
    def test_requires_program(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["download", "--ref-date", "2024-01"])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "required" in result.output.lower()

    def test_requires_ref_date(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["download", "--program", "qcew"])
        assert result.exit_code != 0

    def test_invalid_program(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["download", "--program", "invalid", "--ref-date", "2024-01"])
        assert result.exit_code != 0

    def test_invalid_ref_date(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["download", "--program", "qcew", "--ref-date", "not-a-date"])
        assert result.exit_code != 0

    def test_api_key_required_for_v2(self):
        runner = CliRunner(env={"BLS_API_KEY": ""})
        result = runner.invoke(cli, ["download", "--program", "ces", "--ref-date", "2024-01"])
        assert result.exit_code != 0
        assert "API key" in result.output


class TestReleaseDatesCommand:
    def test_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["release-dates", "--help"])
        assert result.exit_code == 0
        assert "Scrape" in result.output
