"""BLS API v2 utility engine (ARCH §6.1): targeted fetches and spot checks only."""

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
    pass


def fetch_series(
    client: httpx.Client,
    settings: Settings,
    series_ids: list[str],
    start_year: int,
    end_year: int,
    *,
    throttle: Throttle | None = None,
) -> pl.DataFrame:
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
