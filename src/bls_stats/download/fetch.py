"""Shared HTTP download utilities for BLS bulk data files.

Provides retry logic, proper User-Agent headers (BLS blocks bot-like agents),
and zip extraction support for QCEW bulk downloads.
"""

from __future__ import annotations

import io
import logging
import time
import zipfile

import httpx
import polars as pl

logger = logging.getLogger(__name__)

USER_AGENT = "bls-stats/0.1.0 (Federal statistics research)"
DEFAULT_TIMEOUT = 300.0
MAX_RETRIES = 3
RETRY_BACKOFF = 2.0


def _make_client(timeout: float = DEFAULT_TIMEOUT) -> httpx.Client:
    return httpx.Client(
        headers={"User-Agent": USER_AGENT},
        follow_redirects=True,
        timeout=timeout,
    )


def download_text(url: str, *, timeout: float = DEFAULT_TIMEOUT) -> str:
    """Download a URL and return its text content with retries."""
    logger.info("Downloading %s", url)
    for attempt in range(MAX_RETRIES):
        try:
            with _make_client(timeout) as client:
                resp = client.get(url)
                resp.raise_for_status()
                return resp.text
        except (httpx.HTTPStatusError, httpx.TransportError) as exc:
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF * (attempt + 1)
                logger.warning(
                    "Attempt %d/%d failed for %s: %s (retrying in %.1fs)",
                    attempt + 1, MAX_RETRIES, url, exc, wait,
                )
                time.sleep(wait)
            else:
                raise
    raise RuntimeError("unreachable")


def download_bytes(url: str, *, timeout: float = DEFAULT_TIMEOUT) -> bytes:
    """Download a URL and return raw bytes with retries."""
    logger.info("Downloading %s", url)
    for attempt in range(MAX_RETRIES):
        try:
            with _make_client(timeout) as client:
                resp = client.get(url)
                resp.raise_for_status()
                return resp.content
        except (httpx.HTTPStatusError, httpx.TransportError) as exc:
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF * (attempt + 1)
                logger.warning(
                    "Attempt %d/%d failed for %s: %s (retrying in %.1fs)",
                    attempt + 1, MAX_RETRIES, url, exc, wait,
                )
                time.sleep(wait)
            else:
                raise
    raise RuntimeError("unreachable")


def read_tsv(url: str, *, timeout: float = DEFAULT_TIMEOUT) -> pl.DataFrame:
    """Download a BLS tab-delimited flat file and parse it into a DataFrame."""
    text = download_text(url, timeout=timeout)
    df = pl.read_csv(
        io.StringIO(text),
        separator="\t",
        infer_schema_length=10_000,
    )
    df = df.rename({c: c.strip() for c in df.columns})
    return df


def read_zip_csvs(
    url: str,
    *,
    timeout: float = DEFAULT_TIMEOUT,
    schema_overrides: dict[str, pl.DataType] | None = None,
) -> pl.DataFrame:
    """Download a zip, read all CSVs inside, and return a single concatenated DataFrame."""
    data = download_bytes(url, timeout=timeout)
    frames: list[pl.DataFrame] = []
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        logger.info("Zip contains %d CSV files", len(csv_names))
        for name in csv_names:
            raw = zf.read(name)
            try:
                frame = pl.read_csv(
                    io.BytesIO(raw),
                    infer_schema_length=10_000,
                    schema_overrides=schema_overrides,
                )
                frame = frame.rename({c: c.strip() for c in frame.columns})
                frames.append(frame)
            except Exception:
                logger.warning("Failed to parse CSV %s from zip", name, exc_info=True)
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal_relaxed")
