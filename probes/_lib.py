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
import os
import time
from pathlib import Path

import httpx


def _load_project_env(path: Path) -> None:
    """Minimal .env loader (KEY=VALUE lines; comments and blanks skipped).
    Existing environment always wins — exported vars are never overridden.
    Dependency-free on purpose: uv resolves deps from the *entry* script's
    PEP 723 block, so a python-dotenv import here would force the dep into
    every probe's header."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


_load_project_env(Path(__file__).parent.parent / ".project.env")  # repo root; absent in worktrees

# §7.1/R12: a descriptive, contactable UA is mandatory from the first request.
# Contact address comes from .project.env (BLS_CONTACT_EMAIL); the fallback keeps
# the probes compliant if the env file is absent (e.g. an isolated worktree).
CONTACT_UA = (
    "bls-stats-probe/0.1 (data-archival research; contact: "
    f"{os.environ.get('BLS_CONTACT_EMAIL', 'mason.lowell@mac.com')})"
)

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
    import tempfile
    with tempfile.NamedTemporaryFile("w", suffix=".env", delete=False) as tf:
        tf.write("# comment\nFOO_PROBE=bar\n")
    os.environ["FOO_PROBE"] = "kept"
    _load_project_env(Path(tf.name))
    assert os.environ["FOO_PROBE"] == "kept", "existing env must win over the file"
    del os.environ["FOO_PROBE"]
    _load_project_env(Path(tf.name))
    assert os.environ.pop("FOO_PROBE") == "bar", "file value not loaded"
    Path(tf.name).unlink()
    print("_lib self-check OK")
