"""Replay a synthetic CES release sequence and assert the §2.1 lifecycle end-to-end."""

from datetime import UTC, date, datetime

import polars as pl

from bls_stats.core.config import Settings
from bls_stats.pipeline import run_ingest
from bls_stats.releases.feeds import Release
from bls_stats.storage.delta import VintageStore
from bls_stats.storage.reads import as_of, latest

NOW = datetime(2026, 7, 2, 13, 0, tzinfo=UTC)

# CES publishing March..June 2026, then the Feb-2027 benchmark (January-2027 data):
SEQUENCE = [
    Release("ces", date(2026, 4, 3), 2026, 3, False),
    Release("ces", date(2026, 5, 8), 2026, 4, False),
    Release("ces", date(2026, 6, 5), 2026, 5, False),
    Release("ces", date(2026, 7, 2), 2026, 6, False),
    Release("ces", date(2027, 2, 5), 2027, 1, True),
]


def fetch_everything(client, program, slots, dest_dir, downloaded) -> pl.DataFrame:
    refs = [s.ref_date for s in slots]
    return pl.DataFrame(
        {
            "series_id": ["CES0000000001"] * len(refs),
            "value": [float(r.toordinal()) for r in refs],  # value encodes the ref_date
            "footnote_codes": [""] * len(refs),
            "ref_date": refs,
        }
    )


def replay(store: VintageStore) -> None:
    for release in SEQUENCE:
        run_ingest(
            Settings(),
            store,
            ["ces"],
            clock=lambda: NOW,
            poll_fn=lambda client, programs, r=release: [r],
            fetch_fn=fetch_everything,
            fresh_fn=lambda client, program, rd: True,
        )


def test_march_2026_lifecycle_matches_spec(tmp_path) -> None:  # ARCH §2.1 table
    store = VintageStore(str(tmp_path / "store"))
    replay(store)
    march = (
        store.scan_observations("ces")
        .filter(pl.col("ref_date") == date(2026, 3, 12))
        .sort("release_date")
        .collect()
    )
    lifecycle = list(zip(march["revision"].to_list(), march["benchmark"].to_list(), strict=True))
    assert lifecycle == [(0, 0), (1, 0), (2, 0), (2, 1)]  # the user's founding example


def test_benchmark_day_one_row_per_ref_date(tmp_path) -> None:  # ARCH §2.2 blocker fix
    store = VintageStore(str(tmp_path / "store"))
    replay(store)
    bench_day = (
        store.scan_observations("ces").filter(pl.col("release_date") == date(2027, 2, 5)).collect()
    )
    assert bench_day["ref_date"].n_unique() == bench_day.height


def test_candidate_key_unique(tmp_path) -> None:  # ARCH §4.3
    store = VintageStore(str(tmp_path / "store"))
    replay(store)
    obs = store.scan_observations("ces").collect()
    key = ["series_id", "ref_date", "release_date"]
    assert obs.unique(subset=key).height == obs.height


def test_as_of_no_future_leakage_across_replay(tmp_path) -> None:  # ARCH §9 crown jewel
    store = VintageStore(str(tmp_path / "store"))
    replay(store)
    lf = store.scan_observations("ces")
    for when in [date(2026, 4, 30), date(2026, 7, 2), date(2027, 12, 31)]:
        out = as_of(lf, ["series_id"], when).collect()
        assert (out["release_date"] <= when).all()
    # and the latest view reflects the benchmark for March 2026:
    march_latest = (
        latest(lf, ["series_id"]).filter(pl.col("ref_date") == date(2026, 3, 12)).collect()
    )
    assert march_latest["benchmark"][0] == 1
