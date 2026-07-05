from datetime import UTC, date, datetime

import polars as pl

from bls_stats.storage.reads import as_of, latest, prints

TS = datetime(2026, 7, 2, tzinfo=UTC)


def frame(rows: list[dict]) -> pl.LazyFrame:
    return pl.DataFrame(
        [
            {
                "series_id": r.get("sid", "S1"),
                "value": r["v"],
                "ref_date": r["ref"],
                "release_date": r["rel"],
                "revision": r.get("rev"),
                "benchmark": r.get("bm"),
                "source": r.get("src", "increment"),
                "downloaded": TS,
            }
            for r in rows
        ],
        schema_overrides={"revision": pl.Int16, "benchmark": pl.Int16},
    ).lazy()


VINTAGES = frame(
    [
        {"ref": date(2026, 4, 12), "rel": date(2026, 5, 1), "rev": 0, "bm": 0, "v": 1.0},
        {"ref": date(2026, 4, 12), "rel": date(2026, 6, 1), "rev": 1, "bm": 0, "v": 2.0},
        {"ref": date(2026, 4, 12), "rel": date(2026, 7, 2), "rev": 2, "bm": 0, "v": 3.0},
    ]
)


def test_latest_picks_max_release_date() -> None:
    out = latest(VINTAGES, ["series_id"]).collect()
    assert out.height == 1 and out["value"][0] == 3.0


def test_as_of_never_leaks_future() -> None:  # the ARCH §9 crown-jewel invariant
    out = as_of(VINTAGES, ["series_id"], date(2026, 6, 15)).collect()
    assert out["value"][0] == 2.0
    assert (out["release_date"] <= date(2026, 6, 15)).all()


def test_as_of_inclusive_of_release_day() -> None:
    out = as_of(VINTAGES, ["series_id"], date(2026, 6, 1)).collect()
    assert out["value"][0] == 2.0


def test_tiebreak_prefers_increment_then_counters() -> None:  # ARCH §4.4
    lf = frame(
        [
            {
                "ref": date(2026, 4, 12),
                "rel": date(2026, 7, 1),
                "rev": None,
                "bm": None,
                "src": "backfill",
                "v": 10.0,
            },
            {"ref": date(2026, 4, 12), "rel": date(2026, 7, 1), "rev": 2, "bm": 1, "v": 20.0},
        ]
    )
    out = latest(lf, ["series_id"]).collect()
    assert out.height == 1 and out["value"][0] == 20.0


def test_prints_filters_on_counters() -> None:
    out = prints(VINTAGES, revision=1).collect()
    assert out.height == 1 and out["value"][0] == 2.0
