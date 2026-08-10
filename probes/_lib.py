# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx[http2]>=0.27"]
# ///
"""Shared helpers for the Stage-1 ground-truth probes.

Deliberately NOT package code: Stage 2 bootstraps src/bls_stats/ (spec §16).
These scripts are the recorded *method* behind specs/bls-stats-stage1-findings.md.
"""
from __future__ import annotations

import datetime as dt
import json
import time
from pathlib import Path

import httpx

# §7.1/R12: a descriptive, contactable UA is mandatory from the first request.
CONTACT_UA = "bls-stats-probe/0.1 (data-archival research; contact: mason.lowell@mac.com)"

# §7.2 mitigation 2: browser-shaped headers for the html profile.
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Upgrade-Insecure-Requests": "1",
}

# §7.3: the flatfile profile pins Accept-Encoding for reproducible archived bytes.
CONTACT_HEADERS = {
    "User-Agent": CONTACT_UA,
    "Accept": "*/*",
    "Accept-Encoding": "gzip",
}

POLITE_DELAY_S = 2.0
RESULTS_DIR = Path(__file__).parent / "results"


def make_client(*, browser_shaped: bool = False, timeout: float = 60.0) -> httpx.Client:
    return httpx.Client(
        http2=True,
        headers=BROWSER_HEADERS if browser_shaped else CONTACT_HEADERS,
        timeout=timeout,
        follow_redirects=True,
    )


def probe(client: httpx.Client, method: str, url: str, *, keep_body: bool = False, **kw) -> dict:
    """One recorded request. Sleeps POLITE_DELAY_S afterward, success or failure."""
    rec: dict = {
        "probed_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
        "method": method,
        "url": url,
    }
    started = time.monotonic()
    try:
        r = client.request(method, url, **kw)
        rec |= {
            "status": r.status_code,
            "http_version": r.http_version,
            "final_url": str(r.url),
            "redirects": [(h.status_code, str(h.url)) for h in r.history],
            "headers": dict(r.headers),
        }
        if method == "GET" or keep_body:
            rec["body_bytes"] = len(r.content)
            rec["body_head"] = r.content[:500].decode("utf-8", "replace")
            if keep_body:
                rec["body_text"] = r.text
    except httpx.HTTPError as e:
        rec |= {"status": None, "error": f"{type(e).__name__}: {e}"}
    rec["elapsed_ms"] = round((time.monotonic() - started) * 1000)
    time.sleep(POLITE_DELAY_S)
    return rec


def write_results(script: str, records: list[dict]) -> Path:
    RESULTS_DIR.mkdir(exist_ok=True)
    out = RESULTS_DIR / f"{script}-{dt.date.today().isoformat()}.jsonl"
    with out.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, default=str) + "\n")
    return out


if __name__ == "__main__":
    # Offline self-check: builds both client shapes, round-trips a result file.
    # No network, no BLS request.
    c = make_client()
    assert c.headers["user-agent"].startswith("bls-stats-probe/"), "contact UA missing"
    assert c.headers["accept-encoding"] == "gzip", "Accept-Encoding not pinned"
    c.close()
    b = make_client(browser_shaped=True)
    assert b.headers["user-agent"].startswith("Mozilla/5.0"), "browser UA missing"
    b.close()
    p = write_results("selfcheck", [{"ok": True}])
    assert p.read_text(encoding="utf-8") == '{"ok": true}\n'
    p.unlink()
    print("_lib self-check OK")
