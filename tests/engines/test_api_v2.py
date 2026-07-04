import json

import httpx
import polars as pl
import pytest

from bls_stats.core.config import Settings
from bls_stats.core.http import Throttle
from bls_stats.engines.api_v2 import BlsApiError, fetch_series

SETTINGS = Settings(api_key="test-key")
NO_THROTTLE = Throttle(0, clock=lambda: 0.0, sleep=lambda _s: None)


def _payload(series: list[dict], messages: list[str] | None = None) -> dict:
    return {"status": "REQUEST_SUCCEEDED", "message": messages or [],
            "Results": {"series": series}}


def _series(sid: str) -> dict:
    return {"seriesID": sid, "data": [
        {"year": "2026", "period": "M05", "value": "159180",
         "footnotes": [{"code": "P", "text": "Preliminary."}], "latest": "true"},
        {"year": "2026", "period": "M04", "value": "159123", "footnotes": [{}]},
    ]}


def test_fetch_parses_rows_and_key_in_payload() -> None:
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        seen.append(body)
        return httpx.Response(200, json=_payload([_series(s) for s in body["seriesid"]]))

    client = httpx.Client(transport=httpx.MockTransport(handler))
    df = fetch_series(client, SETTINGS, ["CES0000000001"], 2026, 2026, throttle=NO_THROTTLE)
    assert seen[0]["registrationkey"] == "test-key"
    assert df.height == 2
    assert df.schema["value"] == pl.Float64
    assert df["footnote_codes"].to_list() == ["P", ""]


def test_batches_of_fifty() -> None:
    batches: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        ids = json.loads(request.content)["seriesid"]
        batches.append(len(ids))
        return httpx.Response(200, json=_payload([_series(s) for s in ids]))

    client = httpx.Client(transport=httpx.MockTransport(handler))
    fetch_series(client, SETTINGS, [f"S{i:03d}" for i in range(120)], 2026, 2026,
                 throttle=NO_THROTTLE)
    assert batches == [50, 50, 20]


def test_hidden_error_in_message_array_raises() -> None:  # ARCH §6.1 quirk
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_payload([], ["Series does not exist for Series XXX"]))

    client = httpx.Client(transport=httpx.MockTransport(handler))
    with pytest.raises(BlsApiError, match="does not exist"):
        fetch_series(client, SETTINGS, ["XXX"], 2026, 2026, throttle=NO_THROTTLE)


def test_missing_api_key_raises() -> None:
    with pytest.raises(BlsApiError, match="BLS_API_KEY"):
        fetch_series(httpx.Client(), Settings(), ["X"], 2026, 2026, throttle=NO_THROTTLE)
