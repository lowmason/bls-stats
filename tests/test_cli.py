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


def test_backfill_qcew_malformed_range_exits_two(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("BLS_STORE_URI", str(tmp_path / "store"))
    result = runner.invoke(
        app, ["backfill", "--program", "qcew", "--start", "2020-13", "--end", "2020/1"]
    )
    assert result.exit_code == 2


def test_bad_log_level_falls_back_to_info(monkeypatch, tmp_path) -> None:
    import logging

    from bls_stats.cli import _setup

    monkeypatch.setenv("BLS_STORE_URI", str(tmp_path / "store"))
    monkeypatch.setenv("BLS_LOG_LEVEL", "verbose")
    logging.getLogger().handlers.clear()
    settings, _ = _setup()  # must not raise (falls back to INFO)
    assert settings.log_level == "verbose"


def _seed_store_for_gaps(tmp_path):
    """A store whose calendar expects a CES release that the ledger never recorded."""
    from datetime import UTC, date, datetime

    import polars as pl

    from bls_stats.releases.calendar import CALENDAR_SCHEMA
    from bls_stats.storage.delta import VintageStore
    from bls_stats.vintage.ledger import Ledger, SlotRecord

    store = VintageStore(str(tmp_path / "store"))
    store.append_state(
        "release_calendar",
        pl.DataFrame(
            [
                {
                    "program": "ces",
                    "ref_date": date(2026, 3, 12),
                    "release_date": date(2026, 4, 3),
                    "original_release": None,
                    "is_benchmark": False,
                },
            ],
            schema=CALENDAR_SCHEMA,
        ),
    )
    # jolts has a missed slot; ces has no ledger row at all.
    Ledger(store).record(
        [
            SlotRecord(
                "jolts",
                date(2026, 3, 31),
                date(2026, 5, 6),
                0,
                0,
                "increment",
                0,
                "missed",
                datetime(2026, 5, 6, tzinfo=UTC),
            ),
        ]
    )
    return store


def test_gaps_flags_expected_release_missing_from_ledger(monkeypatch, tmp_path) -> None:  # C-1
    _seed_store_for_gaps(tmp_path)
    monkeypatch.setenv("BLS_STORE_URI", str(tmp_path / "store"))
    result = runner.invoke(app, ["gaps", "--program", "ces"])
    assert result.exit_code == 1  # the expected CES release has no ledger row
    assert "unexplained: 1" in result.output


def test_gaps_program_scopes_strict_missed(monkeypatch, tmp_path) -> None:  # C-1
    _seed_store_for_gaps(tmp_path)
    monkeypatch.setenv("BLS_STORE_URI", str(tmp_path / "store"))
    # jolts has no calendar row, so no unexplained gap; its missed slot is acknowledged and,
    # under --strict, fails — but that is jolts' own slot, not leaked from ces:
    result = runner.invoke(app, ["gaps", "--program", "jolts", "--strict"])
    assert result.exit_code == 1
    assert "acknowledged: 1" in result.output


def test_ingest_unknown_program_exits_two(monkeypatch, tmp_path) -> None:  # C-4
    monkeypatch.setenv("BLS_STORE_URI", str(tmp_path / "store"))
    result = runner.invoke(app, ["ingest", "--program", "CES"])  # uppercase typo
    assert result.exit_code == 2
    assert "unknown program" in result.output


def test_programs_matches_registry() -> None:  # C-9
    from bls_stats.cli import PROGRAMS
    from bls_stats.registry import REGISTRY

    assert PROGRAMS == list(REGISTRY)
