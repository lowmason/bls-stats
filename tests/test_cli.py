from typer.testing import CliRunner

from bls_stats.cli import app

runner = CliRunner()


def test_help_lists_all_commands() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for cmd in ("backfill", "ingest", "calendar", "gaps", "store", "metadata", "doctor"):
        assert cmd in result.output


def test_ingest_dry_run_smoke(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("BLS_STORE_URI", str(tmp_path / "store"))
    monkeypatch.setattr("bls_stats.pipeline.run_ingest", lambda *a, **k: 0)
    result = runner.invoke(app, ["ingest", "--dry-run"])
    assert result.exit_code == 0


def test_ingest_exit_code_propagates(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("BLS_STORE_URI", str(tmp_path / "store"))
    monkeypatch.setattr("bls_stats.pipeline.run_ingest", lambda *a, **k: 1)
    result = runner.invoke(app, ["ingest"])
    assert result.exit_code == 1


def test_backfill_requires_program_and_range() -> None:
    result = runner.invoke(app, ["backfill"])
    assert result.exit_code != 0
