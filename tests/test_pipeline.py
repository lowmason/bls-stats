from datetime import UTC, date, datetime

import polars as pl
import pytest

from bls_stats.core.config import Settings
from bls_stats.pipeline import run_backfill, run_ingest, stamp
from bls_stats.releases.feeds import Release
from bls_stats.storage.delta import VintageStore
from bls_stats.vintage.ledger import Ledger

NOW = datetime(2026, 7, 2, 13, 0, tzinfo=UTC)
CLOCK = lambda: NOW  # noqa: E731
# A later wall-clock for a subsequent ingest run. The ledger resolves latest-status-wins
# by strictly-increasing ingested_at (see tests/vintage/test_ledger.py's ts(9)/ts(11)),
# so multi-run tests must advance the injected clock or the two runs' rows collide at NOW.
LATER = datetime(2026, 8, 7, 13, 0, tzinfo=UTC)
LATER_CLOCK = lambda: LATER  # noqa: E731
JUNE_RELEASE = Release("ces", date(2026, 7, 2), 2026, 6, False)


def fake_fetch(refs: list[date] | None = None, rows_per_ref: int = 3):
    def _fetch(client, program, slots, dest_dir, downloaded) -> pl.DataFrame:
        wanted = refs if refs is not None else [s.ref_date for s in slots]
        return pl.DataFrame(
            {
                "series_id": [f"CES{i:010d}" for r in wanted for i in range(rows_per_ref)],
                "value": [1.0] * rows_per_ref * len(wanted),
                "footnote_codes": [""] * rows_per_ref * len(wanted),
                "ref_date": [r for r in wanted for _ in range(rows_per_ref)],
            },
            schema={
                "series_id": pl.Utf8,
                "value": pl.Float64,
                "footnote_codes": pl.Utf8,
                "ref_date": pl.Date,
            },
        )

    return _fetch


@pytest.fixture()
def store(tmp_path) -> VintageStore:
    return VintageStore(str(tmp_path / "store"))


def _ingest(store, **kw):
    defaults = dict(
        programs=["ces"],
        clock=CLOCK,
        poll_fn=lambda client, programs: [JUNE_RELEASE],
        fetch_fn=fake_fetch(),
        fresh_fn=lambda client, program, rd: True,
    )
    return run_ingest(Settings(), store, **(defaults | kw))


def test_happy_path_commits_three_slots_and_records(store) -> None:
    assert _ingest(store) == 0
    obs = store.scan_observations("ces").collect()
    assert obs.height == 9  # 3 slots × 3 rows
    assert set(zip(obs["revision"].to_list(), obs["benchmark"].to_list(), strict=True)) == {
        (0, 0),
        (1, 0),
        (2, 0),
    }
    led = Ledger(store).resolved()
    assert led.height == 3 and (led["status"] == "ingested").all()


def test_rerun_is_noop(store) -> None:
    _ingest(store)
    _ingest(store)
    assert store.scan_observations("ces").collect().height == 9  # no duplicates


def test_crash_between_commit_and_record_repairs(store, monkeypatch) -> None:
    calls = {"n": 0}
    original = Ledger.record

    def crashing_record(self, records):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("simulated crash after data commit")
        return original(self, records)

    monkeypatch.setattr(Ledger, "record", crashing_record)
    assert _ingest(store) == 1  # first run: event failed after commit
    assert _ingest(store) == 0  # rerun: presence check repairs, no re-append
    assert store.scan_observations("ces").collect().height == 9
    assert (Ledger(store).resolved()["status"] == "ingested").all()


def test_stale_file_defers_and_exits_zero(store) -> None:
    assert _ingest(store, fresh_fn=lambda client, program, rd: False) == 0  # ARCH §7.4
    led = Ledger(store).resolved()
    assert (led["status"] == "deferred").all()
    assert store.scan_observations("ces") is None  # nothing committed


def test_deferred_event_retried_next_run(store) -> None:
    _ingest(store, fresh_fn=lambda client, program, rd: False)
    assert _ingest(store, clock=LATER_CLOCK) == 0  # file now fresh (later run)
    assert (Ledger(store).resolved()["status"] == "ingested").all()


def test_superseded_deferred_becomes_missed(store) -> None:  # ARCH §5.3 transition
    _ingest(store, fresh_fn=lambda client, program, rd: False)  # June deferred
    july = Release("ces", date(2026, 8, 7), 2026, 7, False)
    _ingest(
        store, clock=LATER_CLOCK, poll_fn=lambda client, programs: [july]
    )  # newer release ingests (later run)
    led = Ledger(store).resolved()
    june = led.filter(pl.col("release_date") == date(2026, 7, 2))
    assert june.height == 3 and (june["status"] == "missed").all()


def test_empty_slice_defers(store) -> None:
    assert _ingest(store, fetch_fn=fake_fetch(refs=[])) == 0
    assert (Ledger(store).resolved()["status"] == "deferred").all()


def test_fetch_error_isolates_and_exits_two_when_all_fail(store) -> None:
    def boom(client, program, slots, dest_dir, downloaded):
        raise RuntimeError("download failed")

    assert _ingest(store, fetch_fn=boom) == 2


def test_dry_run_commits_nothing(store) -> None:
    assert _ingest(store, dry_run=True) == 0
    assert store.scan_observations("ces") is None
    assert store.read_state("ledger") is None


def test_stamp_types() -> None:
    df = stamp(
        pl.DataFrame(
            {
                "series_id": ["x"],
                "value": [1.0],
                "footnote_codes": [""],
                "ref_date": [date(2026, 6, 12)],
            }
        ),
        date(2026, 6, 12),
        date(2026, 7, 2),
        0,
        0,
        "increment",
        NOW,
    )
    assert df.schema["revision"] == pl.Int16 and df.schema["source"] == pl.Utf8


def _seed_calendar(store) -> None:
    store.append_state(
        "release_calendar",
        pl.DataFrame(
            {
                "program": ["ces"],
                "ref_date": [date(2026, 6, 12)],
                "release_date": [date(2026, 7, 2)],
                "original_release": pl.Series([None], dtype=pl.Date),
                "is_benchmark": [False],
            }
        ),
    )


def test_backfill_without_calendar_exits_two(store) -> None:
    assert run_backfill(Settings(), store, "ces", "2020/01", "2020/12", clock=CLOCK) == 2


def test_backfill_program_missing_from_calendar_exits_two(store) -> None:
    _seed_calendar(store)
    assert run_backfill(Settings(), store, "jolts", "2020/01", "2020/12", clock=CLOCK) == 2


def test_backfill_malformed_range_exits_two(store) -> None:
    _seed_calendar(store)
    assert run_backfill(Settings(), store, "ces", "2020-01", "2020-12", clock=CLOCK) == 2
