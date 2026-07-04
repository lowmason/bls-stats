import os
from datetime import UTC, date, datetime

import polars as pl
import pytest

from bls_stats.storage.delta import VintageStore


def obs_frame(
    ref: date,
    rel: date,
    revision: int | None,
    benchmark: int | None,
    source: str = "increment",
    value: float = 1.0,
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "series_id": ["CES0000000001"],
            "value": [value],
            "footnote_codes": [""],
            "ref_date": [ref],
            "release_date": [rel],
            "revision": pl.Series([revision], dtype=pl.Int16),
            "benchmark": pl.Series([benchmark], dtype=pl.Int16),
            "source": [source],
            "downloaded": [datetime(2026, 7, 2, 13, 0, tzinfo=UTC)],
        }
    )


@pytest.fixture()
def store(tmp_path) -> VintageStore:
    return VintageStore(str(tmp_path / "store"))


def test_append_and_scan_roundtrip(store: VintageStore) -> None:
    store.append_observations("ces", obs_frame(date(2026, 6, 12), date(2026, 7, 2), 0, 0))
    lf = store.scan_observations("ces")
    assert lf is not None
    out = lf.collect()
    assert out.height == 1
    assert out.schema["ref_date"] == pl.Date
    assert out.schema["revision"] == pl.Int16


def test_scan_missing_table_returns_none(store: VintageStore) -> None:
    assert store.scan_observations("jolts") is None


def test_append_rejects_missing_vintage_columns(store: VintageStore) -> None:
    with pytest.raises(ValueError, match="release_date"):
        store.append_observations("ces", pl.DataFrame({"series_id": ["x"]}))


def test_slot_exists_null_safe(store: VintageStore) -> None:
    store.append_observations(
        "ces", obs_frame(date(2020, 1, 12), date(2026, 7, 1), None, None, source="backfill")
    )
    assert store.slot_exists("ces", date(2020, 1, 12), date(2026, 7, 1), None, None) is True
    assert store.slot_exists("ces", date(2020, 1, 12), date(2026, 7, 1), 0, None) is False
    assert store.slot_exists("ces", date(2020, 2, 12), date(2026, 7, 1), None, None) is False


def test_state_roundtrip_and_append(store: VintageStore) -> None:
    row = pl.DataFrame({"program": ["ces"], "note": ["a"]})
    store.append_state("ledger", row)
    store.append_state("ledger", row.with_columns(pl.lit("b").alias("note")))
    out = store.read_state("ledger")
    assert out is not None and out.height == 2
    assert store.read_state("nope") is None


@pytest.mark.real_store
def test_minio_roundtrip() -> None:
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    if not endpoint:
        pytest.skip("no AWS_ENDPOINT_URL configured")
    from bls_stats.core.config import Settings
    from bls_stats.core.config import storage_options as so

    store = VintageStore(
        "s3://bls-stats/test-store",
        so(Settings(store_uri="s3://bls-stats/test-store", aws_endpoint_url=endpoint)),
    )
    store.append_observations("ces", obs_frame(date(2026, 6, 12), date(2026, 7, 2), 0, 0))
    assert store.slot_exists("ces", date(2026, 6, 12), date(2026, 7, 2), 0, 0)
