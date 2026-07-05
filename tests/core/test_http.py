import httpx
import pytest

from bls_stats.core.config import Settings
from bls_stats.core.http import Throttle, build_client, get, head_last_modified


def _client_with(handler) -> httpx.Client:
    c = build_client(Settings(contact_email="me@example.org"))
    c._transport = httpx.MockTransport(handler)
    return c


def test_user_agent_includes_contact() -> None:
    c = build_client(Settings(contact_email="me@example.org"))
    assert "me@example.org" in c.headers["User-Agent"]
    assert c.headers["User-Agent"].startswith("bls-stats/")


def test_5xx_retries_then_succeeds() -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(500 if len(calls) < 3 else 200, text="ok")

    resp = get(_client_with(handler), "https://example.com/x", sleep=lambda _s: None)
    assert resp.status_code == 200
    assert len(calls) == 3


def test_4xx_fails_fast() -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(404)

    with pytest.raises(httpx.HTTPStatusError):
        get(_client_with(handler), "https://example.com/x", sleep=lambda _s: None)
    assert len(calls) == 1


def test_5xx_exhausts_retries() -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(503)

    with pytest.raises(httpx.HTTPStatusError):
        get(_client_with(handler), "https://example.com/x", retries=2, sleep=lambda _s: None)
    assert len(calls) == 3  # retries=2 → 3 total attempts


def test_head_last_modified_parses_to_utc() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "HEAD"
        return httpx.Response(200, headers={"Last-Modified": "Thu, 02 Jul 2026 12:30:00 GMT"})

    lm = head_last_modified(_client_with(handler), "https://example.com/x")
    assert lm is not None and lm.isoformat() == "2026-07-02T12:30:00+00:00"


def test_download_retries_5xx_then_succeeds(tmp_path) -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(500 if len(calls) < 2 else 200, text="payload")

    from bls_stats.core.http import download

    dest = download(
        _client_with(handler), "https://example.com/f", tmp_path / "f.txt", sleep=lambda _s: None
    )
    assert dest.read_text() == "payload"
    assert len(calls) == 2


def test_throttle_waits_only_when_needed() -> None:
    now = {"t": 0.0}
    slept: list[float] = []
    th = Throttle(2.0, clock=lambda: now["t"], sleep=lambda s: slept.append(s))
    th.wait()  # first call: no sleep
    now["t"] = 0.5
    th.wait()  # 1.5s remaining
    assert slept == [1.5]
