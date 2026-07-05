"""BLS API v2 utility engine (ARCH §6.1): targeted fetches and spot checks only.

The API cannot carry full-universe daily increments on one registered key (500 queries/day,
50 series/query cap) — see ARCH §6.1 for the series-count math that rules it out as the
primary fetch path. It survives as a **utility**: targeted series pulls, `latest=true` probes,
spot-check validation of ingested values, and catalog lookups. Flat files remain the primary
increment source (`bls_stats.engines.labstat`, `.qcew`, `.oews`).

Two BLS API behaviors this module works around:

- **Hidden errors:** the API can return HTTP 200 with `"status": "REQUEST_SUCCEEDED"` while the
  actual failure (e.g. an unknown series ID) is buried in the `message[]` array — a plain
  `raise_for_status()` would miss it. `fetch_series` inspects `message[]` explicitly.
- **No POST retries:** unlike `core.http.get`/`download` (which retry 5xx/transport errors with
  backoff), this module's POST to the timeseries endpoint is deliberately not retried — series
  queries are cheap to fail and re-run at the call site, and retrying a POST risks duplicate
  billing against the daily quota if the first attempt actually succeeded server-side.
"""

from __future__ import annotations

import logging

import httpx
import polars as pl

from bls_stats.core.config import Settings
from bls_stats.core.http import Throttle

log = logging.getLogger(__name__)

ENDPOINT = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BATCH = 50  # registered-key limit per query


class BlsApiError(RuntimeError):
    """Raised for a missing API key, a non-`REQUEST_SUCCEEDED` status, or a hidden `message[]`
    error (ARCH §6.1) — a request that returned HTTP 200 but failed logically."""


def fetch_series(
    client: httpx.Client,
    settings: Settings,
    series_ids: list[str],
    start_year: int,
    end_year: int,
    *,
    throttle: Throttle | None = None,
) -> pl.DataFrame:
    """Fetch series observations from the BLS API v2 timeseries endpoint, batched and throttled.

    Splits `series_ids` into batches of `BATCH` (50, the registered-key per-query limit),
    throttles between requests (default 0.25s spacing — the 50-req/10s cap), and checks each
    response for the hidden-error quirk described in the module docstring before parsing rows.

    Args:
        client: Shared `httpx.Client`.
        settings: Runtime settings; `settings.api_key` must be set (`BLS_API_KEY`).
        series_ids: Series IDs to fetch, in any order; batching preserves input order.
        start_year: First year to request, inclusive.
        end_year: Last year to request, inclusive (one API query covers a ≤20-year window).
        throttle: Rate limiter between batch requests; defaults to `Throttle(0.25)`.

    Returns:
        A `pl.DataFrame` with one row per (series, period) observation: `series_id` (`Utf8`),
        `year` (`Int32`), `period` (`Utf8`, e.g. `"M05"`), `value` (`Float64`, null for BLS's
        empty-string or `"-"` sentinels), `footnote_codes` (`Utf8`, comma-joined codes from the
        observation's footnotes), and `latest` (`Boolean`).

    Raises:
        BlsApiError: `settings.api_key` is unset, the response status isn't
            `REQUEST_SUCCEEDED`, or `message[]` contains a "does not exist" / "No Data" error
            despite an HTTP 200 (ARCH §6.1).
    """
    if settings.api_key is None:
        raise BlsApiError("BLS_API_KEY is not configured")
    throttle = throttle if throttle is not None else Throttle(0.25)  # 50 req / 10 s cap
    rows: list[dict] = []
    for i in range(0, len(series_ids), BATCH):
        batch = series_ids[i : i + BATCH]
        throttle.wait()
        resp = client.post(
            ENDPOINT,
            json={
                "seriesid": batch,
                "startyear": str(start_year),
                "endyear": str(end_year),
                "registrationkey": settings.api_key,
            },
        )
        resp.raise_for_status()
        payload = resp.json()
        messages = [m for m in payload.get("message", []) if m]
        if payload.get("status") != "REQUEST_SUCCEEDED":
            raise BlsApiError(f"API status {payload.get('status')}: {messages}")
        errors = [m for m in messages if "does not exist" in m or "No Data" in m]
        if errors:  # HTTP 200 + REQUEST_SUCCEEDED can still be an error (ARCH §6.1)
            raise BlsApiError("; ".join(errors))
        for series in payload["Results"]["series"]:
            for obs in series.get("data", []):
                rows.append(
                    {
                        "series_id": series["seriesID"],
                        "year": int(obs["year"]),
                        "period": obs["period"],
                        "value": float(obs["value"]) if obs["value"] not in ("", "-") else None,
                        "footnote_codes": ",".join(
                            f["code"] for f in obs.get("footnotes", []) if f.get("code")
                        ),
                        "latest": obs.get("latest") == "true",
                    }
                )
    return pl.DataFrame(
        rows,
        schema={
            "series_id": pl.Utf8,
            "year": pl.Int32,
            "period": pl.Utf8,
            "value": pl.Float64,
            "footnote_codes": pl.Utf8,
            "latest": pl.Boolean,
        },
    )
