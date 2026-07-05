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


def test_benchmark_release_rerun_is_noop(store) -> None:  # C-13
    """Re-polling the same benchmark release must not re-append its window (ARCH §4.3/§7.2)."""
    from datetime import timedelta

    bench = Release("ces", date(2027, 2, 5), 2027, 1, True)  # CES Feb benchmark, Jan-2027 data
    run1 = datetime(2027, 2, 6, 13, 0, tzinfo=UTC)
    run2 = run1 + timedelta(days=1)  # next daily cron; benchmark still in the feed
    common = dict(
        programs=["ces"],
        poll_fn=lambda client, programs: [bench],
        fetch_fn=fake_fetch(),
        fresh_fn=lambda client, program, rd: True,
    )
    run_ingest(Settings(), store, clock=lambda: run1, **common)
    after_run1 = store.scan_observations("ces").collect().height
    run_ingest(Settings(), store, clock=lambda: run2, **common)  # re-poll same release
    obs = store.scan_observations("ces").collect()
    assert obs.height == after_run1  # no duplicate window rows
    key = ["series_id", "ref_date", "release_date"]
    assert obs.unique(subset=key).height == obs.height  # candidate key still unique
    assert obs.filter(pl.col("benchmark") == 2).height == 0  # no fabricated benchmark=2


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


def test_backdated_release_denied_not_fabricated(store) -> None:  # C-14
    """Catch-up ingest must not stamp the current file as an older release's live print."""
    older = Release("ces", date(2026, 6, 5), 2026, 5, False)
    newer = Release("ces", date(2026, 7, 2), 2026, 6, False)
    run_ingest(
        Settings(), store, programs=["ces"], clock=CLOCK,
        poll_fn=lambda client, programs: [older, newer],  # outage catch-up: two at once
        fetch_fn=fake_fetch(), fresh_fn=lambda client, program, rd: True,
    )
    obs = store.scan_observations("ces").collect()
    # the older release's date must NOT appear as a committed vintage:
    assert obs.filter(pl.col("release_date") == date(2026, 6, 5)).height == 0
    # the newer release committed normally:
    assert obs.filter(pl.col("release_date") == date(2026, 7, 2)).height > 0
    # the older release's slots are recorded missed (visible to `gaps`), not silently dropped:
    led = Ledger(store).resolved()
    older_rows = led.filter(pl.col("release_date") == date(2026, 6, 5))
    assert older_rows.height > 0 and (older_rows["status"] == "missed").all()


def test_backdated_repoll_does_not_downgrade_ingested(store) -> None:  # C-14 fix (Important #1)
    """A back-dated re-poll must not flip an already-ingested slot to missed (ARCH §5.3)."""
    june = Release("ces", date(2026, 7, 2), 2026, 6, False)   # June data, published Jul 2
    july = Release("ces", date(2026, 8, 6), 2026, 7, False)   # July data, published Aug 6 (newer)
    _ingest(store, poll_fn=lambda client, programs: [june], clock=CLOCK)  # run 1: June ingests
    # run 2: rolling feed rolls forward, June re-appears alongside newer July
    _ingest(store, poll_fn=lambda client, programs: [june, july], clock=LATER_CLOCK)
    led = Ledger(store).resolved()
    june_rows = led.filter(pl.col("release_date") == date(2026, 7, 2))
    assert june_rows.height > 0 and (june_rows["status"] == "ingested").all()  # NOT downgraded
    obs = store.scan_observations("ces").collect()
    assert obs.filter(pl.col("release_date") == date(2026, 7, 2)).height > 0     # June obs intact
    assert obs.filter(pl.col("release_date") == date(2026, 8, 6)).height > 0     # July committed


def test_backdated_via_ledger_branch_recorded_missed(store) -> None:  # C-14 coverage (Important #2)
    """The 'already-ingested in ledger' back-dated branch: a lone older re-poll denied + missed."""
    june = Release("ces", date(2026, 7, 2), 2026, 6, False)
    july = Release("ces", date(2026, 8, 6), 2026, 7, False)
    _ingest(store, poll_fn=lambda client, programs: [july], clock=CLOCK)  # run 1: July ingests
    # run 2: lone older re-poll (June only), already back-dated by the ingested July in the ledger
    _ingest(store, poll_fn=lambda client, programs: [june], clock=LATER_CLOCK)
    obs = store.scan_observations("ces").collect()
    assert obs.filter(pl.col("release_date") == date(2026, 7, 2)).height == 0  # June not fabricated
    led = Ledger(store).resolved()
    june_rows = led.filter(pl.col("release_date") == date(2026, 7, 2))
    assert june_rows.height > 0 and (june_rows["status"] == "missed").all()      # recorded missed
    assert (led.filter(pl.col("release_date") == date(2026, 8, 6))["status"] == "ingested").all()


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


def test_ingest_ep_only_exits_two(store) -> None:
    assert run_ingest(Settings(), store, ["ep"], clock=CLOCK) == 2


def test_backfill_ep_exits_two(store) -> None:
    assert run_backfill(Settings(), store, "ep", "2024", "2026", clock=CLOCK) == 2
