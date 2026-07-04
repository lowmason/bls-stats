from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import polars as pl
import pytest

from bls_stats.engines.labstat import is_fresh, parse_flat_file

FIXTURES = Path(__file__).parent.parent / "fixtures" / "labstat"
TS = datetime(2026, 7, 2, 13, 0, tzinfo=UTC)


def test_parse_drops_m13_and_attaches_ref_date() -> None:
    df = parse_flat_file(FIXTURES / "ce.data.sample.txt", "ces", downloaded=TS)
    assert df.height == 4  # M13 row gone
    assert set(df.columns) == {"series_id", "value", "footnote_codes", "ref_date", "downloaded"}
    assert df.schema["series_id"] == pl.Utf8
    assert df.schema["footnote_codes"] == pl.Utf8
    assert df.schema["ref_date"] == pl.Date
    assert date(2026, 6, 12) in df["ref_date"].to_list()


def test_parse_filters_to_requested_periods() -> None:
    df = parse_flat_file(FIXTURES / "ce.data.sample.txt", "ces", [(2026, 5)], downloaded=TS)
    assert df["ref_date"].unique().to_list() == [date(2026, 5, 12)]
    assert df.height == 2  # two series carry May


def test_values_are_trimmed_floats() -> None:
    df = parse_flat_file(FIXTURES / "ce.data.sample.txt", "ces", [(2026, 5)], downloaded=TS)
    assert df.schema["value"] == pl.Float64
    assert 35.42 in df["value"].to_list()


def test_sm_m13_dropped() -> None:
    df = parse_flat_file(FIXTURES / "sm.data.sample.txt", "sae", downloaded=TS)
    assert df.height == 2


def test_is_fresh_compares_to_embargo() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={"Last-Modified": "Thu, 02 Jul 2026 12:30:00 GMT"})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    assert is_fresh(client, "ces", date(2026, 7, 2)) is True  # 08:30 ET == 12:30 UTC (EDT)
    assert is_fresh(client, "ces", date(2026, 7, 3)) is False  # file older than next release


@pytest.mark.network
def test_live_headers_and_freshness_probe() -> None:
    from bls_stats.core.config import load_settings
    from bls_stats.core.http import build_client, head_last_modified
    from bls_stats.registry import REGISTRY

    client = build_client(load_settings(), timeout=60.0)
    lm = head_last_modified(client, REGISTRY["jolts"].increment_url)
    assert lm is not None  # Last-Modified present — the stale guard's precondition
