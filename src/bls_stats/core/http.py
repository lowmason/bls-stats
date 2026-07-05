"""The one HTTP client (ARCH §10): descriptive UA, 4xx fast-fail, 5xx backoff, throttle."""

from __future__ import annotations

import logging
import time as _time
from collections.abc import Callable
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

import httpx

import bls_stats
from bls_stats.core.config import Settings

log = logging.getLogger(__name__)


def build_client(settings: Settings, timeout: float = 300.0) -> httpx.Client:
    ua = f"bls-stats/{bls_stats.__version__} ({settings.contact_email})"
    return httpx.Client(headers={"User-Agent": ua}, timeout=timeout, follow_redirects=True)


def get(
    client: httpx.Client,
    url: str,
    *,
    retries: int = 3,
    backoff: float = 2.0,
    method: str = "GET",
    sleep: Callable[[float], None] = _time.sleep,
) -> httpx.Response:
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            resp = client.request(method, url)
            resp.raise_for_status()
            return resp
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code < 500:
                log.error("HTTP %s for %s — failing fast", exc.response.status_code, url)
                raise
            last_exc = exc
        except httpx.TransportError as exc:
            last_exc = exc
        if attempt < retries:
            delay = backoff * 2**attempt
            log.warning(
                "retry %d/%d for %s in %.1fs (%s)",
                attempt + 1,
                retries,
                url,
                delay,
                last_exc,
            )
            sleep(delay)
    assert last_exc is not None
    raise last_exc


def download(
    client: httpx.Client,
    url: str,
    dest: Path,
    *,
    retries: int = 3,
    backoff: float = 2.0,
    sleep: Callable[[float], None] = _time.sleep,
) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            with client.stream("GET", url) as resp:
                resp.raise_for_status()
                with dest.open("wb") as fh:  # "wb" truncates any partial previous attempt
                    for chunk in resp.iter_bytes():
                        fh.write(chunk)
            return dest
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code < 500:
                log.error("HTTP %s for %s — failing fast", exc.response.status_code, url)
                raise
            last_exc = exc
        except httpx.TransportError as exc:
            last_exc = exc
        if attempt < retries:
            delay = backoff * 2**attempt
            log.warning(
                "download retry %d/%d for %s in %.1fs (%s)",
                attempt + 1,
                retries,
                url,
                delay,
                last_exc,
            )
            sleep(delay)
    assert last_exc is not None
    raise last_exc


def head_last_modified(client: httpx.Client, url: str) -> datetime | None:
    resp = get(client, url, method="HEAD")
    value = resp.headers.get("Last-Modified")
    if value is None:
        return None
    return parsedate_to_datetime(value).astimezone(UTC)


class Throttle:
    def __init__(
        self,
        seconds: float,
        clock: Callable[[], float] = _time.monotonic,
        sleep: Callable[[float], None] = _time.sleep,
    ) -> None:
        self.seconds, self._clock, self._sleep = seconds, clock, sleep
        self._last: float | None = None

    def wait(self) -> None:
        if self._last is not None:
            remaining = self.seconds - (self._clock() - self._last)
            if remaining > 0:
                self._sleep(remaining)
        self._last = self._clock()
