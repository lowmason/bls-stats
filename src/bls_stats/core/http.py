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
    """Construct the one shared `httpx.Client` used across the package (ARCH §10).

    Sends a descriptive User-Agent (`bls-stats/<version> (<contact email>)`) as required by
    BLS's terms, and follows redirects by default.

    Args:
        settings: Resolved settings; `contact_email` is embedded in the User-Agent.
        timeout: Per-request timeout in seconds. Defaults generously (300s) to accommodate
            the 300+ MB flat-file downloads (ARCH §10).

    Returns:
        A configured `httpx.Client` ready for `get`/`download`/`head_last_modified`.
    """
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
    """Issue one request with the package-wide retry policy (ARCH §7.4): 4xx fails fast,
    5xx/transport errors retry with exponential backoff.

    Args:
        client: The shared `httpx.Client` (see `build_client`).
        url: Request URL.
        retries: Maximum retry attempts after the first try (so `retries=3` allows up to 4
            total attempts).
        backoff: Base delay in seconds; actual delay is `backoff * 2**attempt`.
        method: HTTP method, e.g. `"GET"` or `"HEAD"`.
        sleep: Injected sleep function (real `time.sleep` by default) — swap in a no-op or
            recorder in tests for determinism (ARCH §9).

    Returns:
        The successful `httpx.Response` (status < 400, or a 5xx/transport error that
        eventually succeeded within `retries`).

    Raises:
        httpx.HTTPStatusError: A 4xx response (raised immediately, no retry) or a 5xx
            response that persisted through all retries.
        httpx.TransportError: A connection-level failure that persisted through all retries.
    """
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
    """Stream a URL to disk, applying the same 4xx-fail-fast / 5xx-retry policy as `get`.

    Streams in chunks rather than buffering in memory (ARCH §10 — RSS target < 8 GB for
    300+ MB flat files). Each retry truncates and rewrites `dest` from scratch (`"wb"` mode),
    so a partial prior attempt never leaves stale bytes on disk.

    Args:
        client: The shared `httpx.Client`.
        url: File URL to download.
        dest: Destination path; parent directories are created if missing.
        retries: Maximum retry attempts after the first try.
        backoff: Base delay in seconds; actual delay is `backoff * 2**attempt`.
        sleep: Injected sleep function, overridable for deterministic tests.

    Returns:
        `dest`, once the file has been fully written.

    Raises:
        httpx.HTTPStatusError: A 4xx response, or a 5xx response that persisted through
            all retries.
        httpx.TransportError: A connection-level failure that persisted through all retries.
    """
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
    """HEAD a URL and return its `Last-Modified` header, normalized to UTC.

    Used as the stale-file guard for increment fetches (ARCH §6.3): verifies a flat file has
    actually flipped to the new vintage before trusting its contents, rather than relying on
    feed timestamps (unreliable per ARCH §5.2).

    Args:
        client: The shared `httpx.Client`.
        url: File URL to check.

    Returns:
        The parsed `Last-Modified` timestamp as a timezone-aware UTC `datetime`, or `None` if
        the header is absent.
    """
    resp = get(client, url, method="HEAD")
    value = resp.headers.get("Last-Modified")
    if value is None:
        return None
    return parsedate_to_datetime(value).astimezone(UTC)


class Throttle:
    """A simple wall-clock rate limiter: `wait()` sleeps just enough to enforce a minimum
    interval between calls.

    Used to pace scrapes against BLS pages (ARCH §5.4) and could apply equally to the API v2
    engine's request cap (ARCH §6.1). Clock and sleep are injected for deterministic tests
    (ARCH §9).
    """

    def __init__(
        self,
        seconds: float,
        clock: Callable[[], float] = _time.monotonic,
        sleep: Callable[[float], None] = _time.sleep,
    ) -> None:
        """Initialize the throttle; the interval starts unarmed (first `wait()` never sleeps).

        Args:
            seconds: Minimum interval enforced between successive `wait()` calls.
            clock: Injected monotonic clock function.
            sleep: Injected sleep function.
        """
        self.seconds, self._clock, self._sleep = seconds, clock, sleep
        self._last: float | None = None

    def wait(self) -> None:
        """Block until at least `seconds` have elapsed since the previous `wait()` call.

        Returns immediately (no sleep) on the first call and whenever the interval has
        already elapsed.
        """
        if self._last is not None:
            remaining = self.seconds - (self._clock() - self._last)
            if remaining > 0:
                self._sleep(remaining)
        self._last = self._clock()
