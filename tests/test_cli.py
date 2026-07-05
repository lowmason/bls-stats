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


def test_bad_log_level_falls_back_to_info(monkeypatch, tmp_path) -> None:  # C-23
    import logging

    from bls_stats.cli import _setup

    monkeypatch.setenv("BLS_STORE_URI", str(tmp_path / "store"))
    monkeypatch.setenv("BLS_LOG_LEVEL", "verbose")  # not a real level
    logging.getLogger().handlers.clear()
    logging.getLogger().setLevel(logging.WARNING)  # a distinct starting level
    _setup()
    assert logging.getLogger().level == logging.INFO  # fell back to INFO, not left at WARNING


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


def test_gaps_bad_as_of_date_exits_two(monkeypatch, tmp_path) -> None:  # C-8 (gaps --as-of-date)
    _seed_store_for_gaps(tmp_path)
    monkeypatch.setenv("BLS_STORE_URI", str(tmp_path / "store"))
    result = runner.invoke(app, ["gaps", "--program", "ces", "--as-of-date", "2026-3-12"])
    assert result.exit_code == 2
    assert "invalid date" in result.output


def test_ingest_unknown_program_exits_two(monkeypatch, tmp_path) -> None:  # C-4
    monkeypatch.setenv("BLS_STORE_URI", str(tmp_path / "store"))
    result = runner.invoke(app, ["ingest", "--program", "CES"])  # uppercase typo
    assert result.exit_code == 2
    assert "unknown program" in result.output


def test_programs_matches_registry() -> None:  # C-9
    from bls_stats.cli import PROGRAMS
    from bls_stats.registry import REGISTRY

    assert PROGRAMS == list(REGISTRY)


def test_store_query_bad_ref_date_exits_two(monkeypatch, tmp_path) -> None:  # C-8
    from datetime import UTC, date, datetime

    import polars as pl

    from bls_stats.storage.delta import VintageStore

    store = VintageStore(str(tmp_path / "store"))
    store.append_observations(
        "ces",
        pl.DataFrame(
            {
                "series_id": ["CES0000000001"],
                "value": [1.0],
                "footnote_codes": [""],
                "ref_date": [date(2026, 3, 12)],
                "release_date": [date(2026, 4, 3)],
                "revision": [0],
                "benchmark": [0],
                "source": ["increment"],
                "downloaded": [datetime(2026, 4, 3, tzinfo=UTC)],
            },
            schema={
                "series_id": pl.Utf8,
                "value": pl.Float64,
                "footnote_codes": pl.Utf8,
                "ref_date": pl.Date,
                "release_date": pl.Date,
                "revision": pl.Int16,
                "benchmark": pl.Int16,
                "source": pl.Utf8,
                "downloaded": pl.Datetime("us", "UTC"),
            },
        ),
    )
    monkeypatch.setenv("BLS_STORE_URI", str(tmp_path / "store"))
    result = runner.invoke(app, ["store", "query", "--program", "ces", "--ref-date", "2026-3-12"])
    assert result.exit_code == 2
    assert "invalid date" in result.output


def test_doctor_exits_zero_on_warnings_only(monkeypatch, tmp_path) -> None:  # C-6
    from bls_stats.storage.doctor import CheckResult

    monkeypatch.setenv("BLS_STORE_URI", str(tmp_path / "store"))  # local: warn, not fail
    monkeypatch.setattr(
        "bls_stats.storage.doctor.check_conditional_put",
        lambda s: CheckResult("conditional_put", True, "skipped: local store"),
    )
    monkeypatch.setattr(
        "bls_stats.storage.doctor.check_bls",
        lambda s: CheckResult("bls", True, "HTTP 200"),
    )
    result = runner.invoke(app, ["doctor"])
    assert result.exit_code == 0  # only warnings, no hard failures


def test_metadata_cache_dir_from_settings(monkeypatch, tmp_path) -> None:  # C-7
    from bls_stats.core.config import load_settings

    monkeypatch.setenv("BLS_METADATA_CACHE", str(tmp_path / "meta"))
    assert load_settings().metadata_cache_dir == str(tmp_path / "meta")
