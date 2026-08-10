# Stage 1: Ground-Truth Probes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: implement this plan task-by-task via
> subagent-driven-development (the default) — or executing-plans when your human partner chose
> inline execution at the handoff. Steps use checkbox (`- [ ]`) syntax for tracking.

> Roadmap: specs/bls-stats-spec-roadmap.md, Stage 1 — on plan completion, tick the stage and
> re-validate later stages against what shipped.

**Goal:** Settle the probe-resolvable unknowns that gate irreversible Stage-2 decisions (spec §20
issues 1, 2, 4-sizes, 14, 15-mechanism) and write down the archive-bucket creation parameters
before any bucket exists — the exit artifact is `specs/bls-stats-stage1-findings.md`, a written
finding, not software.

**Architecture:** Standalone PEP 723 probe scripts under `probes/`, run with `uv run`. There is
deliberately **no `pyproject.toml`, no `src/`, no pytest** in this stage — Stage 2 bootstraps the
package (spec §16). The scripts are the recorded *method* behind the findings document; each writes
dated raw output to `probes/results/` (committed). The findings document summarizes every probe
with method + date + interpretation, and ends with the archive-bucket parameter sheet.

**Tech Stack:** Python ≥ 3.12, `uv` (inline script metadata), `httpx[http2]` for all BLS HTTP,
`boto3` for the object-store probe — the same client spec §16 mandates for the real adapter, so
capability findings transfer directly to Stage 2.

## Global Constraints

Copied from the spec; every task's requirements include these.

- Python ≥ 3.12; `uv` (§1.4). Probes are single-file PEP 723 scripts; packaging arrives in Stage 2.
- `httpx[http2]>=0.27` for all BLS HTTP (§1.4, §16); `boto3>=1.35.35` for the S3 probe only.
- **A descriptive, contactable User-Agent is mandatory from the first request** (§7.1, R12). The
  exact string is defined once, in `probes/_lib.py`.
- **NEVER send a bare or default User-Agent to any BLS host — not even to "confirm the 403".**
  R12: BLS "permits real-time blocking of non-compliant robots"; a blocked IP forfeits Stage 2
  capture. The bare-UA 403 is already settled (R12) and needs no re-observation.
- Politeness: strictly sequential requests, ≥ 2.0 s spacing (`_lib.POLITE_DELAY_S`), HEAD where
  sufficient, ranged GET where a body sample is needed. **Exactly one full-file download in the
  whole stage** (the QCEW measurement, Task 5). Do not run the BLS-facing tasks (2–5) in parallel
  with each other — they share the client IP.
- Peak RSS < 8 GB (§1.4): the QCEW measurement streams in 1 MiB chunks and never loads a CSV into
  memory.
- **No real bucket is created in this stage.** §7.3: object-lock immutability is settable only at
  bucket creation, so the real buckets are created in Stage 2 *from the Task-7 parameter sheet*.
  Probe buckets are prefixed `bls-stats-probe-` and deleted before the probe exits.
- No secrets in committed output: results record endpoint hostnames, never credentials;
  `BLS_API_KEY` must never appear in `probes/results/`.
- Every finding is dated (results filenames carry ISO dates; each record carries `probed_at`).
  Service behavior is point-in-time — a result is evidence for its date, not a stable fact.
- Commit messages: plain imperative mood, matching the existing log (no `feat:` prefixes).

## Execution-time inputs (stop and ask your human partner if missing)

0. **Task 6, development endpoint (known — this workstation, verified 2026-08-10):** MinIO
   (Homebrew) runs via the LaunchAgent `~/Library/LaunchAgents/com.lowell.minio.plist`, serving
   `/Users/lowell/S3` at `http://localhost:9000` (console `:9001`), modern single-drive layout
   (`.minio.sys` present). No `bls-stats` bucket exists yet — correct: Stage 2 creates the real
   buckets from Task 7's sheet. For the probe, export `AWS_ENDPOINT_URL=http://localhost:9000`
   and set `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` from the plist's `EnvironmentVariables`
   (`MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` — read with
   `plutil -p ~/Library/LaunchAgents/com.lowell.minio.plist`; never commit the values). This is
   the **root** credential, so expect the delete-deny check to report NOT ENFORCED for it — that
   is the §17.4 finding that Stage 2 needs a distinct non-admin runtime credential, not a probe
   bug. Probe buckets will appear as `/Users/lowell/S3/bls-stats-probe-*` and are deleted;
   existing buckets (`alt-nfp`, `bls-stats-old`) are never touched.
1. **Task 6, deployment endpoint:** `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`,
   `AWS_SECRET_ACCESS_KEY` (and `AWS_REGION` if not `us-east-1`).
2. **Task 6:** shell access to a deployment-side dev container (§20 issue 14 is precisely
   "reachable from the container network?").
3. **Task 3 (optional):** `BLS_API_KEY` (registered v2 key). Without it the probe runs the v1
   unregistered path and records that the registered path is untested.
4. **Task 7:** operator sign-off on two recorded decisions — object-lock mode/duration, and the
   replication mechanism. The task drafts both with recommendations; the sign-off may be batched
   at the completion gate.

## Task dependency order

Task 1 first. Tasks 2–5 depend only on Task 1 and run in any order (but sequentially — shared
client IP). Task 6 is standalone (no `_lib`) and may run any time after Task 1 (it commits into
the same findings file). Task 7 needs Task 6's matrix. Task 8 needs everything.

---

### Task 1: Probe scaffolding and findings skeleton

**Files:**
- Create: `probes/_lib.py`
- Create: `probes/README.md`
- Create: `specs/bls-stats-stage1-findings.md`

**Interfaces:**
- Consumes: nothing (fresh repo, `specs/` only).
- Produces (used by Tasks 2–5):
  - `_lib.CONTACT_UA: str`, `_lib.CONTACT_HEADERS: dict`, `_lib.BROWSER_HEADERS: dict`
  - `_lib.POLITE_DELAY_S: float = 2.0`, `_lib.RESULTS_DIR: Path`
  - `_lib.make_client(*, browser_shaped: bool = False, timeout: float = 60.0) -> httpx.Client`
  - `_lib.probe(client, method: str, url: str, *, keep_body: bool = False, **kw) -> dict` —
    one recorded request; sleeps `POLITE_DELAY_S` after, success or failure; GET records
    `body_head` (first 500 chars) and, iff `keep_body`, full `body_text`.
  - `_lib.write_results(script: str, records: list[dict]) -> Path` — JSONL to
    `probes/results/<script>-<YYYY-MM-DD>.jsonl`.
- Produces (used by Tasks 2–8): the findings skeleton, one numbered section per probe.

- [ ] **Step 1: Write `probes/_lib.py`**

```python
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
        if method == "GET":
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
```

- [ ] **Step 2: Run the self-check**

Run: `uv run probes/_lib.py`
Expected: `_lib self-check OK` (first run also resolves `httpx[http2]` into uv's cache).

- [ ] **Step 3: Write `probes/README.md`**

```markdown
# Stage-1 ground-truth probes

Method record for `specs/bls-stats-stage1-findings.md` (roadmap Stage 1). Not package
code — Stage 2 bootstraps `src/bls_stats/`.

Run any probe from the repo root:

    uv run probes/<script>.py

Raw dated output lands in `probes/results/` and is committed; the findings document
cites it.

Rules (spec §7.1, R12, §1.4):

- Contactable User-Agent on every BLS request — defined once in `_lib.py`.
- **Never** send a bare/default UA to a BLS host, even to "confirm the 403": BLS
  blocks non-compliant robots in real time, and a blocked IP forfeits Stage-2 capture.
- Sequential requests only, ≥ 2 s apart. One full-file download total (`qcew_sizes.py`).
- `objstore_capabilities.py` creates only throwaway `bls-stats-probe-*` buckets and
  deletes them. The real buckets are created in Stage 2 from the findings' parameter
  sheet — never here.
```

- [ ] **Step 4: Write `specs/bls-stats-stage1-findings.md`** (skeleton; later tasks fill their
  sections)

```markdown
# Stage 1 findings — ground-truth probes

**Roadmap:** specs/bls-stats-spec-roadmap.md, Stage 1. **Spec:** specs/bls-stats-spec.md.
**Method:** each finding cites its probe script (`probes/`), raw output
(`probes/results/`), and run date. Service behavior is point-in-time: every result is
evidence for its date, none is assumed stable (re-verify before hard-coding).

## 1. Flat-file ingest channel — §20 issue 2 (Task 2)

What settles it: "confirm the compliant profile passes and lock the ingest channel
before anything else is built."

*(recorded by Task 2)*

## 2. API surface — §7.1 `api` profile (Task 3)

What settles it: structural success checking demonstrated live — HTTP 200 and
`REQUEST_SUCCEEDED` are not success signals.

*(recorded by Task 3)*

## 3. HTML posture — §20 issue 1 (Task 4)

What settles it: "a live probe across all three transport profiles … it determines the
whole HTML ingest posture" — browser-shaped `httpx` sufficient, or headless mandatory.

*(recorded by Task 4)*

## 4. QCEW singlefile sizes — §20 issue 4, sizes half (Task 5)

What settles it: "actual byte sizes … the review's size figures are third-party."
Diff *cost* is measured at Stage 5 (roadmap).

*(recorded by Task 5)*

## 5. Object-store endpoint capability matrix — §20 issue 14, §1.4, §17.4 (Task 6)

What settles it: "confirming the deployment endpoint's address and reachability from
the container network," plus which §17.4 requirements the endpoint can satisfy.

*(recorded by Task 6)*

## 6. Replication options — §20 issue 15, mechanism (Task 7)

What settles it: "the deployment's replication decision" — options recorded here,
`doctor`-confirmed at Stage 7.

*(recorded by Task 7)*

## 7. Archive-bucket creation parameters — §7.3, §17.4 (Task 7)

Written **before any bucket exists**: object-lock retention is settable only at bucket
creation (§7.3) — "the one storage decision that cannot be corrected later."

*(recorded by Task 7)*

## 8. Consequences for later stages (Task 8)

*(recorded by Task 8)*
```

- [ ] **Step 5: Commit**

```bash
git add probes/_lib.py probes/README.md specs/bls-stats-stage1-findings.md
git commit -m "Add Stage-1 probe scaffolding and findings skeleton"
```

---

### Task 2: Flat-file host probe (§20 issue 2)

**Files:**
- Create: `probes/transport_flatfile.py`
- Modify: `specs/bls-stats-stage1-findings.md` (section 1)

**Interfaces:**
- Consumes: `_lib.make_client`, `_lib.probe`, `_lib.write_results`, `_lib.POLITE_DELAY_S` (Task 1
  signatures).
- Produces: `probes/results/transport_flatfile-<date>.jsonl`; findings section 1 with the
  ingest-channel verdict later tasks and Stage 2 rely on.

- [ ] **Step 1: Write `probes/transport_flatfile.py`**

```python
# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx[http2]>=0.27"]
# ///
"""§20 issue 2: does the compliant flatfile profile pass on the flat-file hosts?

HEAD + tiny GET + ranged GET only. The one bulk download lives in qcew_sizes.py.
"""
import datetime as dt
import time

import httpx

from _lib import POLITE_DELAY_S, make_client, probe, write_results

REQUESTS = [
    ("GET", "https://download.bls.gov/robots.txt", "stated robot policy, dated"),
    ("HEAD", "https://download.bls.gov/pub/time.series/ce/ce.data.0.AllCESSeries",
     "large CES file: Last-Modified/ETag/Content-Length surface"),
    ("GET", "https://download.bls.gov/pub/time.series/ce/ce.period",
     "tiny mapping file: GET delivers bytes"),
    ("HEAD", "https://download.bls.gov/pub/time.series/jt/jt.data.1.AllItems",
     "second LABSTAT prefix (JOLTS)"),
    ("HEAD", "https://data.bls.gov/cew/data/files/2024/csv/2024_qtrly_singlefile.zip",
     "QCEW host data.bls.gov + singlefile URL-pattern confirmation"),
]

RANGED_URL = "https://download.bls.gov/pub/time.series/ce/ce.data.0.AllCESSeries"


def ranged_probe(client: httpx.Client, url: str, nbytes: int = 1024) -> dict:
    """Ranged GET that never drains the body if the server ignores Range."""
    rec: dict = {
        "probed_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
        "method": "GET",
        "url": url,
        "range": f"bytes=0-{nbytes - 1}",
    }
    try:
        with client.stream("GET", url, headers={"Range": f"bytes=0-{nbytes - 1}"}) as r:
            first = next(r.iter_bytes(nbytes), b"")
            rec |= {
                "status": r.status_code,
                "http_version": r.http_version,
                "headers": dict(r.headers),
                "first_chunk_bytes": len(first),
            }
    except httpx.HTTPError as e:
        rec |= {"status": None, "error": f"{type(e).__name__}: {e}"}
    time.sleep(POLITE_DELAY_S)
    return rec


def main() -> None:
    records = []
    with make_client() as client:
        for method, url, why in REQUESTS:
            rec = probe(client, method, url) | {"why": why}
            print(f"{rec.get('status')}  {rec.get('http_version', '-'):8} {method:4} {url}")
            records.append(rec)
        rec = ranged_probe(client, RANGED_URL) | {"why": "is resumable streaming supported?"}
        print(f"{rec.get('status')}  ranged GET first_chunk={rec.get('first_chunk_bytes')}")
        records.append(rec)

    out = write_results("transport_flatfile", records)
    statuses = [r.get("status") for r in records]
    ok = all(s == 200 for s in statuses[:5]) and statuses[5] in (200, 206)
    print(f"\nresults: {out}")
    print(f"INGEST CHANNEL {'CONFIRMED' if ok else 'NOT CONFIRMED'}: statuses={statuses}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Syntax smoke check (before touching a rate-limited host)**

Run: `python3 -m py_compile probes/transport_flatfile.py && echo compiles`
Expected: `compiles`

- [ ] **Step 3: Live run**

Run: `uv run probes/transport_flatfile.py`
Expected: six status lines then `INGEST CHANNEL CONFIRMED: statuses=[200, 200, 200, 200, 200,
206]` (a `200` in the last position is acceptable — it means Range is ignored, recorded as such).
If the QCEW HEAD (position 5) is 404, the URL pattern is wrong: check
`https://www.bls.gov/cew/downloadable-data-files.htm` for the current singlefile path, correct the
URL in this script *and* in `probes/qcew_sizes.py` (Task 5), note the correction in findings
section 4, and re-run.
If any `download.bls.gov` request returns 403 or errors: **the compliant profile did not pass.**
Still record the result in findings section 1 (a negative result is a finding), then stop and
raise it with your human partner — the roadmap gates Stage 2 on this channel being locked, and the
next move (retry window, header adjustment, contact with BLS) is a judgment call, not a plan step.

- [ ] **Step 4: Record findings section 1**

Replace section 1's `*(recorded by Task 2)*` with (fill values from the run output and the JSONL):

```markdown
**Probe:** `probes/transport_flatfile.py`, run <YYYY-MM-DD>
(`probes/results/transport_flatfile-<date>.jsonl`).

| Check | Result |
|---|---|
| `download.bls.gov` robots.txt | <status; note any disallow rules relevant to §7.1 paths> |
| HEAD large LABSTAT file (`ce`) | <status; Last-Modified, ETag, Content-Length values> |
| GET tiny mapping file (`ce.period`) | <status; body_bytes> |
| HEAD second prefix (`jt`) | <status> |
| HEAD QCEW singlefile zip (`data.bls.gov`) | <status; Content-Length> |
| Ranged GET (`bytes=0-1023`) | <206 honored / 200 ignored> |
| HTTP version negotiated | <h2 / http/1.1> |
| Content-Encoding under pinned `Accept-Encoding: gzip` | <value observed> |

**Verdict (issue 2):** <e.g. "CONFIRMED — the compliant contactable-UA profile passes on both
flat-file hosts at polite rates; the ingest channel is locked." — or the observed failure>

**Consequences:** Last-Modified and ETag <are/are not> both present → §6.1 change detection has
<both/which> signals. Range <is/is not> honored → resumable streaming <is/is not> available to
Stage 2's transport. HTTP/2 <negotiated/not negotiated> on this host.
```

- [ ] **Step 5: Commit**

```bash
git add probes/transport_flatfile.py probes/results/ specs/bls-stats-stage1-findings.md
git commit -m "Probe flat-file hosts; record ingest-channel verdict"
```

---

### Task 3: API host probe (§7.1 `api` profile)

**Files:**
- Create: `probes/transport_api.py`
- Modify: `specs/bls-stats-stage1-findings.md` (section 2)

**Interfaces:**
- Consumes: `_lib.make_client`, `_lib.probe(..., keep_body=True)`, `_lib.write_results`.
- Produces: `probes/results/transport_api-<date>.jsonl`; findings section 2.

- [ ] **Step 1: Write `probes/transport_api.py`**

```python
# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx[http2]>=0.27"]
# ///
"""§7.1 api profile: structural success checking on api.bls.gov, demonstrated live.

Uses the v1 single-series GET (no key; unregistered quota is 25 queries/day — this
probe spends exactly one). If BLS_API_KEY is set, spends one v2 query too. The key is
read from the environment and never written to results.
"""
import json
import os

from _lib import make_client, probe, write_results

V1_URL = "https://api.bls.gov/publicAPI/v1/timeseries/data/CES0000000001"
V2_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"


def payload_verdict(body_text: str) -> dict:
    """§7.1: never trust HTTP 200 or REQUEST_SUCCEEDED; inspect message[] and data."""
    d = json.loads(body_text)
    series = d.get("Results", {}).get("series", []) if isinstance(d.get("Results"), dict) else []
    n_points = sum(len(s.get("data", [])) for s in series)
    return {
        "api_status": d.get("status"),
        "messages": d.get("message", []),
        "n_series": len(series),
        "n_datapoints": n_points,
        "success_by_payload": bool(series) and n_points > 0,
    }


def main() -> None:
    records = []
    with make_client() as client:
        rec = probe(client, "GET", V1_URL, keep_body=True)
        body = rec.pop("body_text", "")  # keep results lean; verdict captures the substance
        rec |= {"why": "v1 unregistered single-series GET"} | payload_verdict(body)
        print(f"v1: http={rec.get('status')} api_status={rec.get('api_status')} "
              f"datapoints={rec.get('n_datapoints')} messages={rec.get('messages')}")
        records.append(rec)

        key = os.environ.get("BLS_API_KEY")
        if key:
            payload = {"seriesid": ["CES0000000001"], "startyear": "2024",
                       "endyear": "2025", "registrationkey": key}
            rec = probe(client, "POST", V2_URL, keep_body=True, json=payload)
            body = rec.pop("body_text", "")
            rec |= {"why": "v2 registered POST (key redacted)"} | payload_verdict(body)
            print(f"v2: http={rec.get('status')} api_status={rec.get('api_status')} "
                  f"datapoints={rec.get('n_datapoints')}")
            records.append(rec)
        else:
            records.append({"why": "v2 registered POST", "skipped": "BLS_API_KEY not set"})
            print("v2: skipped (BLS_API_KEY not set) — registered path untested")

    out = write_results("transport_api", records)
    print(f"\nresults: {out}")
    v1_ok = records[0].get("success_by_payload")
    print(f"API SURFACE {'CONFIRMED' if v1_ok else 'NOT CONFIRMED'} by payload inspection")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Syntax smoke check**

Run: `python3 -m py_compile probes/transport_api.py && echo compiles`
Expected: `compiles`

- [ ] **Step 3: Live run**

Run: `uv run probes/transport_api.py` (with `BLS_API_KEY` exported if available)
Expected: `v1: http=200 api_status=REQUEST_SUCCEEDED datapoints=<n≥1> messages=[...]` then
`API SURFACE CONFIRMED by payload inspection`. A v1 `message` array that is non-empty on a
successful request is normal (v1 nags about registration) — that is exactly the point being
demonstrated. If `success_by_payload` is false with http=200, record it verbatim: that is §7.1's
trap observed live.

- [ ] **Step 4: Record findings section 2**

Replace section 2's marker with:

```markdown
**Probe:** `probes/transport_api.py`, run <YYYY-MM-DD>
(`probes/results/transport_api-<date>.jsonl`).

| Check | Result |
|---|---|
| v1 unregistered GET, HTTP status | <status> |
| v1 top-level `status` field | <value> |
| v1 `message[]` | <contents — note anything present despite success> |
| v1 per-series datapoints | <n> |
| v2 registered POST | <same fields, or "untested — no key"> |

**Verdict:** the `api` profile reaches api.bls.gov; success is <only/also> determinable by payload
inspection (`message[]` + per-series data), confirming §7.1's rule. Registered-key path
<tested/untested>. Key registration (annual expiry, §7.1 alert requirement) is a Stage-2 setup
item<, not yet done/; key on hand>.
```

- [ ] **Step 5: Commit**

```bash
git add probes/transport_api.py probes/results/ specs/bls-stats-stage1-findings.md
git commit -m "Probe BLS API surface; record structural-success finding"
```

---

### Task 4: HTML host probe (§20 issue 1)

**Files:**
- Create: `probes/transport_html.py`
- Modify: `specs/bls-stats-stage1-findings.md` (section 3)

**Interfaces:**
- Consumes: `_lib.make_client(browser_shaped=True)`, `_lib.probe`, `_lib.write_results`.
- Produces: `probes/results/transport_html-<date>.jsonl`; findings section 3 — the HTML ingest
  posture that decides whether Stage 2 builds transport for a headless `HtmlFetcher` backend.

- [ ] **Step 1: Write `probes/transport_html.py`**

```python
# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx[http2]>=0.27"]
# ///
"""§20 issue 1: is browser-shaped httpx sufficient for www.bls.gov, or is the
headless HtmlFetcher backend (§7.2 mitigation 3) mandatory?

One contact-UA canary first (compliant UA, so R12-safe; dates the known-403
baseline from review §0.3), then a browser-shaped pass over every surface class
the design needs. The verdict is per-surface: §7.2's ladder degrades per surface,
not all-or-nothing.
"""
from _lib import make_client, probe, write_results

# (surface, url, content marker expected in the first 500 bytes)
SURFACES = [
    ("robots_txt", "https://www.bls.gov/robots.txt", "User-agent"),
    ("ics_schedule", "https://www.bls.gov/schedule/news_release/bls.ics", "BEGIN:VCALENDAR"),
    ("atom_feed", "https://www.bls.gov/feed/empsit.rss", "<feed"),
    ("errata_table", "https://www.bls.gov/errata/", "<html"),
    ("newsrels_index", "https://www.bls.gov/bls/newsrels.htm", "<html"),
    ("schedule_index", "https://www.bls.gov/schedule/", "<html"),
]


def main() -> None:
    records = []
    with make_client() as client:  # contact UA — compliant, single request
        rec = probe(client, "GET", "https://www.bls.gov/errata/")
        rec["surface"] = "canary_contact_ua"
        print(f"canary (contact UA): {rec.get('status')} (403 expected per review §0.3)")
        records.append(rec)

    with make_client(browser_shaped=True) as client:
        for name, url, marker in SURFACES:
            rec = probe(client, "GET", url)
            head = (rec.get("body_head") or "").lower()
            rec |= {"surface": name, "marker_found": marker.lower() in head}
            print(f"{rec.get('status')}  marker={str(rec['marker_found']):5}  {name}")
            records.append(rec)

    out = write_results("transport_html", records)
    browser = [r for r in records if r["surface"] != "canary_contact_ua"]
    blocked = [r["surface"] for r in browser
               if r.get("status") != 200 or not r.get("marker_found")]
    print(f"\nresults: {out}")
    if blocked:
        print(f"HEADLESS BACKEND MANDATORY for: {', '.join(blocked)} (§7.2 mitigation 3)")
    else:
        print("BROWSER-SHAPED HTTPX SUFFICIENT on all probed surfaces (§7.2 mitigation 2)")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Syntax smoke check**

Run: `python3 -m py_compile probes/transport_html.py && echo compiles`
Expected: `compiles`

- [ ] **Step 3: Live run**

Run: `uv run probes/transport_html.py`
Expected: the canary line (any status — it is a dated data point, 403 anticipated), six
surface lines, then either verdict line. **Both verdicts are successful task outcomes** — a 403
here is issue 1 resolving to "headless mandatory", not a probe failure. A 200 whose
`marker_found` is False is suspicious (a WAF challenge page can 200): inspect `body_head` in the
JSONL before calling that surface passed, and record what the body actually was.

- [ ] **Step 4: Record findings section 3**

Replace section 3's marker with:

```markdown
**Probe:** `probes/transport_html.py`, run <YYYY-MM-DD>
(`probes/results/transport_html-<date>.jsonl`).

Canary (compliant contact UA, one request): <status> — <matches/updates> the review-§0.3
baseline.

| Surface | Browser-shaped status | Marker found | Posture |
|---|---|---|---|
| robots.txt | | | |
| `.ics` schedule feed | | | |
| Atom release feed (`empsit.rss`) | | | |
| errata table (`/errata/`) | | | |
| news-release index | | | |
| schedule index | | | |

**Verdict (issue 1):** <"browser-shaped httpx sufficient on all probed surfaces — the headless
HtmlFetcher backend stays pluggable and unbuilt until a regression" / "headless backend MANDATORY
for: <surfaces> — Stage 2 must include the §7.2-mitigation-3 worker for those surfaces">. Note:
this is a WAF property observed on <date>; §18.3 forbids treating it as stable — the posture is
re-checked whenever an HTML surface starts failing in operation.
```

- [ ] **Step 5: Commit**

```bash
git add probes/transport_html.py probes/results/ specs/bls-stats-stage1-findings.md
git commit -m "Probe www.bls.gov HTML posture; record issue-1 verdict"
```

---

### Task 5: QCEW singlefile sizes (§20 issue 4, sizes half)

**Files:**
- Create: `probes/qcew_sizes.py`
- Modify: `specs/bls-stats-stage1-findings.md` (section 4)

**Interfaces:**
- Consumes: `_lib.make_client`, `_lib.probe`, `_lib.write_results`.
- Produces: `probes/results/qcew_sizes-<date>.jsonl`; findings section 4 — the measured sizes
  Stage 5 designs the differ's memory envelope against.

- [ ] **Step 1: Write `probes/qcew_sizes.py`**

```python
# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx[http2]>=0.27"]
# ///
"""§20 issue 4 (sizes half): actual QCEW quarterly-singlefile sizes, measured.

HEADs the singlefile zip per year, downloads ONE (the newest available, streaming,
1 MiB chunks), records compressed/decompressed sizes, line counts, and this
process's peak RSS. The download is a measurement, not a capture — no archive
bucket exists yet, by design (§7.3) — and the scratch file is deleted. Diff COST
is measured at Stage 5 (roadmap, issue 4).
"""
import argparse
import resource
import sys
import tempfile
import zipfile
from pathlib import Path

from _lib import make_client, probe, write_results

URL = "https://data.bls.gov/cew/data/files/{year}/csv/{year}_qtrly_singlefile.zip"
YEARS = range(2019, 2027)


def peak_rss_bytes() -> int:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss if sys.platform == "darwin" else rss * 1024  # macOS: bytes; Linux: KiB


def measure_zip(path: Path) -> list[dict]:
    rows = []
    with zipfile.ZipFile(path) as zf:
        for info in zf.infolist():
            n_lines = 0
            with zf.open(info) as member:
                while chunk := member.read(1 << 20):
                    n_lines += chunk.count(b"\n")
            rows.append({
                "member": info.filename,
                "compressed_bytes": info.compress_size,
                "decompressed_bytes": info.file_size,
                "lines": n_lines,
            })
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dest", type=Path, default=Path(tempfile.gettempdir()),
                    help="scratch dir for the one measured download (deleted after)")
    args = ap.parse_args()

    records: list[dict] = []
    newest = None
    with make_client(timeout=120.0) as client:
        for year in YEARS:
            rec = probe(client, "HEAD", URL.format(year=year)) | {"year": year}
            clen = rec.get("headers", {}).get("content-length")
            print(f"{rec.get('status')}  {year}  Content-Length={clen}")
            records.append(rec)
            if rec.get("status") == 200:
                newest = year

        if newest is None:
            write_results("qcew_sizes", records)
            raise SystemExit(
                "No singlefile zip found for any probed year — URL pattern wrong?\n"
                "Check https://www.bls.gov/cew/downloadable-data-files.htm, fix URL, re-run."
            )

        url = URL.format(year=newest)
        dest = args.dest / f"qcew_{newest}_qtrly_singlefile.zip"
        print(f"downloading {url} -> {dest} (streaming, 1 MiB chunks)")
        n = 0
        with client.stream("GET", url) as r, dest.open("wb") as f:
            for chunk in r.iter_bytes(1 << 20):
                f.write(chunk)
                n += len(chunk)
        records.append({"downloaded": url, "bytes_on_disk": n})

    members = measure_zip(dest)
    records.extend({"year": newest} | m for m in members)
    records.append({"peak_rss_bytes": peak_rss_bytes()})
    dest.unlink()

    out = write_results("qcew_sizes", records)
    print(f"\nresults: {out}")
    for m in members:
        print(f"{m['member']}: {m['compressed_bytes']:,} -> "
              f"{m['decompressed_bytes']:,} bytes, {m['lines']:,} lines")
    print(f"peak RSS of this measurement: {peak_rss_bytes():,} bytes")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Syntax smoke check**

Run: `python3 -m py_compile probes/qcew_sizes.py && echo compiles`
Expected: `compiles`

- [ ] **Step 3: Live run** (this is the stage's one bulk download — expect minutes, not seconds)

Run: `uv run probes/qcew_sizes.py`
Expected: one HEAD line per year 2019–2026 (the newest year may 404 before its first quarterly
release — that is data, record it), a download line, per-member size/line output, and a peak-RSS
line well under the 8 GB budget (the measurement itself streams; expect tens of MB). If every HEAD
404s, follow the printed instruction (URL pattern correction — also fix Task 2's QCEW HEAD if not
yet run) and re-run.

- [ ] **Step 4: Record findings section 4**

Replace section 4's marker with:

```markdown
**Probe:** `probes/qcew_sizes.py`, run <YYYY-MM-DD>
(`probes/results/qcew_sizes-<date>.jsonl`). The review's third-party size figures are now
superseded by measurement.

Per-year singlefile zip (HEAD Content-Length):

| Year | Status | Compressed bytes |
|---|---|---|
| 2019 … 2026 | | |

Measured download (<year>): <bytes_on_disk> on the wire; members:

| Member | Compressed | Decompressed | Lines |
|---|---|---|---|
| | | | |

**Envelope arithmetic (§1.4, informing Stage 5):** decompressed size <D> bytes; a naive
two-quarter in-memory diff would hold ~2×<D> plus parse overhead → <fits/does not fit> the 8 GB
peak-RSS target → Stage 5's differ <may/must not> materialize full quarters and <needs/does not
need> streaming joins. Diff cost is measured, not argued, at Stage 5 (roadmap).
```

- [ ] **Step 5: Commit**

```bash
git add probes/qcew_sizes.py probes/results/ specs/bls-stats-stage1-findings.md
git commit -m "Measure QCEW singlefile sizes; record issue-4 sizes"
```

---

### Task 6: Object-store capability matrix + container reachability (§20 issue 14, §1.4, §17.4)

**Files:**
- Create: `probes/objstore_capabilities.py`
- Modify: `specs/bls-stats-stage1-findings.md` (section 5)

**Interfaces:**
- Consumes: nothing from `_lib` (boto3-only; runs standalone so it can be copied alone into a
  container). Env: `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional
  `AWS_REGION`.
- Produces: `probes/results/objstore-<context>-<host>-<date>.json`; findings section 5 — the
  capability matrix Task 7's parameter sheet keys off (object-lock support, delete-deny
  enforcement, replication API), and the issue-14 reachability statement.

**Execution-time inputs:** deployment endpoint credentials and a deployment-side container shell
(see the plan-level list). If either is unavailable, stop and ask your human partner before
starting this task.

- [ ] **Step 1: Write `probes/objstore_capabilities.py`**

```python
# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3>=1.35.35"]
# ///
"""Endpoint capability matrix (§1.4, §17.4; §20 issue 14).

Same checks against whatever AWS_ENDPOINT_URL names, so matrices are comparable
across endpoints; run from a deployment container it settles container→endpoint
reachability. Creates ONLY throwaway bls-stats-probe-* buckets and deletes them.
NEVER creates the real buckets: §7.3 — those are created in Stage 2 from the
findings' parameter sheet.
"""
import argparse
import datetime as dt
import json
import os
import statistics
import time
from pathlib import Path
from urllib.parse import urlparse

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

RESULTS_DIR = Path(__file__).parent / "results"
LOCK_HOLD_S = 90


def require_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise SystemExit(f"error: {name} required (§1.4: the endpoint is configuration)")
    return v


def error_code(e: ClientError) -> str:
    return e.response.get("Error", {}).get("Code", str(e))


def finish(context: str, endpoint: str, region: str, checks: list[dict]) -> None:
    host = urlparse(endpoint).hostname or "unknown"
    matrix = {
        "probed_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
        "context": context,
        "endpoint_host": host,  # hostname only — never credentials
        "region": region,
        "checks": checks,
    }
    RESULTS_DIR.mkdir(exist_ok=True)
    out = RESULTS_DIR / f"objstore-{context}-{host}-{dt.date.today().isoformat()}.json"
    out.write_text(json.dumps(matrix, indent=2) + "\n", encoding="utf-8")
    print(f"\nresults: {out}\n")
    print(json.dumps(matrix, indent=2))  # copy-paste path for container runs


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--context", required=True,
                    help="where this runs: workstation | container | <label>")
    args = ap.parse_args()

    endpoint = require_env("AWS_ENDPOINT_URL")
    require_env("AWS_ACCESS_KEY_ID")
    require_env("AWS_SECRET_ACCESS_KEY")
    region = os.environ.get("AWS_REGION", "us-east-1")
    c = boto3.client(
        "s3", endpoint_url=endpoint, region_name=region,
        config=Config(retries={"max_attempts": 2}, connect_timeout=10,
                      read_timeout=60, s3={"addressing_style": "path"}),
    )

    today = dt.date.today().isoformat()
    plain = f"bls-stats-probe-plain-{today}"
    lock = f"bls-stats-probe-lock-{today}"
    checks: list[dict] = []
    state: dict = {}

    def record(name: str, fn) -> None:
        try:
            checks.append({"check": name, "outcome": "ok", "detail": fn()})
        except ClientError as e:
            checks.append({"check": name, "outcome": "error", "detail": error_code(e)})
        except Exception as e:
            checks.append({"check": name, "outcome": "unreachable",
                           "detail": f"{type(e).__name__}: {e}"})
        print(f"{checks[-1]['outcome']:12} {name}: {checks[-1]['detail']}")

    def create_bucket(name: str, *, object_lock: bool = False) -> str:
        kw: dict = {"Bucket": name}
        if region != "us-east-1":
            kw["CreateBucketConfiguration"] = {"LocationConstraint": region}
        if object_lock:
            kw["ObjectLockEnabledForBucket"] = True
        c.create_bucket(**kw)
        return "created"

    record("reachability.list_buckets",
           lambda: f"{len(c.list_buckets().get('Buckets', []))} buckets visible")
    if checks[-1]["outcome"] != "ok":
        finish(args.context, endpoint, region, checks)  # unreachable IS the issue-14 answer
        return

    record("create_bucket.plain", lambda: create_bucket(plain))

    def conditional_put() -> str:
        c.put_object(Bucket=plain, Key="cond", Body=b"1", IfNoneMatch="*")
        try:
            c.put_object(Bucket=plain, Key="cond", Body=b"2", IfNoneMatch="*")
            return "NOT ENFORCED: second If-None-Match=* PUT succeeded (header ignored)"
        except ClientError as e:
            code = error_code(e)
            ok = code in ("PreconditionFailed", "412")
            return f"enforced ({code})" if ok else f"rejected ({code})"
    record("conditional_put.if_none_match", conditional_put)

    def versioning() -> str:
        c.put_bucket_versioning(Bucket=plain,
                                VersioningConfiguration={"Status": "Enabled"})
        status = c.get_bucket_versioning(Bucket=plain).get("Status")
        c.put_object(Bucket=plain, Key="v", Body=b"1")
        c.put_object(Bucket=plain, Key="v", Body=b"2")
        n = len(c.list_object_versions(Bucket=plain, Prefix="v").get("Versions", []))
        return f"status={status}, versions_after_two_puts={n}"
    record("versioning", versioning)

    def lock_create() -> str:
        create_bucket(lock, object_lock=True)
        conf = c.get_object_lock_configuration(Bucket=lock)
        return str(conf.get("ObjectLockConfiguration", {}).get("ObjectLockEnabled", "absent"))
    record("object_lock.create_enabled", lock_create)

    if checks[-1]["outcome"] == "ok":
        def lock_retention() -> str:
            r = c.put_object(Bucket=lock, Key="locked", Body=b"x")
            state["lock_version"] = r.get("VersionId")
            state["lock_until"] = time.monotonic() + LOCK_HOLD_S
            until = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=LOCK_HOLD_S)
            c.put_object_retention(
                Bucket=lock, Key="locked", VersionId=state["lock_version"],
                Retention={"Mode": "GOVERNANCE", "RetainUntilDate": until})
            return f"GOVERNANCE +{LOCK_HOLD_S}s on version {state['lock_version']}"
        record("object_lock.put_retention", lock_retention)

        def lock_delete_denied() -> str:
            try:
                c.delete_object(Bucket=lock, Key="locked",
                                VersionId=state["lock_version"])
                return "NOT ENFORCED: versioned delete of a locked object succeeded"
            except ClientError as e:
                return f"enforced ({error_code(e)})"
        record("object_lock.delete_denied", lock_delete_denied)

        def lock_default_retention() -> str:
            c.put_object_lock_configuration(
                Bucket=lock,
                ObjectLockConfiguration={
                    "ObjectLockEnabled": "Enabled",
                    "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": 3650}}})
            return "default retention GOVERNANCE/3650d accepted (a *duration* — §17.4)"
        record("object_lock.default_retention_with_duration", lock_default_retention)

    def delete_deny() -> str:
        policy = {"Version": "2012-10-17", "Statement": [{
            "Sid": "DenyDelete", "Effect": "Deny", "Principal": {"AWS": ["*"]},
            "Action": ["s3:DeleteObject", "s3:DeleteObjectVersion"],
            "Resource": [f"arn:aws:s3:::{plain}/*"]}]}
        c.put_bucket_policy(Bucket=plain, Policy=json.dumps(policy))
        try:
            c.delete_object(Bucket=plain, Key="cond")
            detail = ("NOT ENFORCED for this credential (admin/root bypasses bucket "
                      "policy?) — Stage 2 needs a distinct runtime credential")
        except ClientError as e:
            detail = f"enforced ({error_code(e)})"
        c.delete_bucket_policy(Bucket=plain)
        return detail
    record("delete_deny_policy", delete_deny)

    def lifecycle() -> str:
        c.put_bucket_lifecycle_configuration(
            Bucket=plain,
            LifecycleConfiguration={"Rules": [{
                "ID": "probe", "Status": "Enabled",
                "Filter": {"Prefix": "lifecycle-test/"},
                "Expiration": {"Days": 3650}}]})
        n = len(c.get_bucket_lifecycle_configuration(Bucket=plain).get("Rules", []))
        c.delete_bucket_lifecycle(Bucket=plain)
        return f"accepted, {n} rule(s) readable"
    record("lifecycle_api", lifecycle)

    def replication() -> str:
        try:
            c.get_bucket_replication(Bucket=plain)
            return "replication configured (unexpected on a fresh bucket)"
        except ClientError as e:
            code = error_code(e)
            if code in ("ReplicationConfigurationNotFoundError",
                        "NoSuchReplicationConfiguration"):
                return f"API present, none configured ({code})"
            return f"API answered {code} — likely unsupported"
    record("replication_api", replication)

    def timings() -> dict:
        def timed(fn) -> float:
            s = time.monotonic()
            fn()
            return (time.monotonic() - s) * 1000
        out = {}
        ops = [
            ("put", lambda: c.put_object(Bucket=plain, Key="timing", Body=b"x" * 1024)),
            ("head", lambda: c.head_object(Bucket=plain, Key="timing")),
            ("get", lambda: c.get_object(Bucket=plain, Key="timing")["Body"].read()),
            ("list", lambda: c.list_objects_v2(Bucket=plain, Prefix="timing")),
        ]
        for op, fn in ops:
            out[f"{op}_ms_median"] = round(statistics.median(timed(fn) for _ in range(5)), 1)
        return out
    record("timings_1KiB", timings)

    # Cleanup — best effort; a leftover bucket is named in the output for manual removal.
    if state.get("lock_until"):
        wait = state["lock_until"] - time.monotonic() + 5
        if wait > 0:
            print(f"waiting {wait:.0f}s for the probe object's retention to lapse...")
            time.sleep(wait)
    for bucket in (plain, lock):
        try:
            versions = c.list_object_versions(Bucket=bucket)
            for v in versions.get("Versions", []) + versions.get("DeleteMarkers", []):
                c.delete_object(Bucket=bucket, Key=v["Key"], VersionId=v["VersionId"])
            c.delete_bucket(Bucket=bucket)
            print(f"cleaned up {bucket}")
        except ClientError as e:
            print(f"cleanup {bucket}: {error_code(e)} "
                  f"(fine if never created; otherwise remove manually)")

    finish(args.context, endpoint, region, checks)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify the env guard (offline, fast)**

Run: `env -u AWS_ENDPOINT_URL uv run probes/objstore_capabilities.py --context smoke`
Expected: exits with `error: AWS_ENDPOINT_URL required (§1.4: the endpoint is configuration)` —
no network touched.

- [ ] **Step 3: Run against the development endpoint (local MinIO — see execution-time input 0)**

Run, with the env from execution-time input 0 exported (endpoint `http://localhost:9000`,
credentials from the LaunchAgent plist):
`uv run probes/objstore_capabilities.py --context workstation-dev`
Expected: one line per check ending in `results: probes/results/objstore-workstation-dev-...json`
plus the JSON matrix; both probe buckets report `cleaned up` (they materialize under
`/Users/lowell/S3/` while they exist). The ~95 s wait for the lock retention to lapse is normal.
Expected MinIO nuances, all findings rather than failures: the delete-deny check reports NOT
ENFORCED for the root credential (input 0), and versioning/object-lock/conditional-PUT support
depends on the MinIO release — record whatever the matrix says. §1.4: this endpoint is a dev
convenience; the deployment matrix is the load-bearing one.

- [ ] **Step 4: Run against the deployment endpoint, from the workstation**

Run, with the deployment endpoint's env exported:
`uv run probes/objstore_capabilities.py --context workstation-deploy`
Expected: as Step 3. This isolates endpoint capabilities from container networking, so a Step-5
failure is attributable to the network path, not the endpoint.

- [ ] **Step 5: Run from inside a deployment-side dev container (issue 14's actual question)**

In a container shell (uv is expected in the image; else install per
`https://docs.astral.sh/uv/getting-started/installation/`), with the same deployment env exported:

```bash
# copy the single file in (it is standalone by design), then:
uv run objstore_capabilities.py --context container
```

Expected: the same checks; the final JSON matrix prints to stdout — copy it back into
`probes/results/` in the repo verbatim (filename as printed). If `reachability.list_buckets`
reports `unreachable`, that IS the issue-14 finding: record the failure class (DNS / TLS / route /
auth) and stop for a topology decision with your human partner before Stage 2.

- [ ] **Step 6: Record findings section 5**

Replace section 5's marker with (one column per probed endpoint/context):

```markdown
**Probe:** `probes/objstore_capabilities.py`, run <YYYY-MM-DD>
(`probes/results/objstore-*.json`). Contexts probed: <list>.

| Capability (§17.4 / §1.4) | dev endpoint | deployment (workstation) | deployment (container) |
|---|---|---|---|
| Reachable + authenticated | | | |
| Conditional PUT (`If-None-Match`) | <enforced/ignored/rejected> | | |
| Versioning | | | |
| Object lock at bucket creation | | | |
| Retention with a duration (GOVERNANCE/3650d) | | | |
| Locked-version delete denied | | | |
| Delete-deny bucket policy enforced | | | |
| Lifecycle API | | | |
| Replication API | | | |
| PUT/HEAD/GET/LIST medians (1 KiB, ms) | | | |

**Issue 14 verdict:** the deployment endpoint is `<host>` (recorded as configuration, never a
constant); container→endpoint reachability <CONFIRMED/failed: how>. §17.3's writer lease is
contended <only theoretically (single scheduled container)/in practice: why>.

**§1.4 note honored:** capabilities differ between dev and deployment endpoints as anticipated
<in which directions>; nothing in later stages may rely on a capability only one endpoint has —
`doctor` (Stage 7) reports this same matrix live.

**Credential note:** <the probing credential bypassed/respected the delete-deny policy> —
<if bypassed:> Stage 2 must provision a distinct non-admin runtime credential before first capture.
```

- [ ] **Step 7: Commit**

```bash
git add probes/objstore_capabilities.py probes/results/ specs/bls-stats-stage1-findings.md
git commit -m "Probe object-store capabilities from workstation and container"
```

---

### Task 7: Replication options + archive-bucket creation parameter sheet (§20 issue 15; §7.3, §17.4)

**Files:**
- Modify: `specs/bls-stats-stage1-findings.md` (sections 6 and 7)

**Interfaces:**
- Consumes: Task 6's capability matrix (findings section 5) — specifically the `replication_api`,
  `object_lock.*`, and `delete_deny_policy` rows.
- Produces: findings sections 6–7 — the parameter sheet Stage 2's bucket-creation task executes
  verbatim, and the replication options list §20 issue 15 requires. No code.

- [ ] **Step 1: Write findings section 6 (replication options)**

Replace section 6's marker with the following, completing the "Available here?" column from
Task 6's matrix and striking options the matrix rules out:

```markdown
Requirement (§17.4, R7): an independent second copy of `raw/` + `log/fetch/`; any single-copy
period longer than one release cycle is a standing finding (N12, reported by `doctor` from
Stage 7). The mechanism is the deployment's choice — these are the options, recorded with a
recommendation:

| Option | Mechanism | Independence | Available here? | Notes |
|---|---|---|---|---|
| A. Provider-side replication | endpoint's bucket-replication API | independent media, same provider failure domain | <from matrix: replication_api row> | lowest effort if present |
| B. Second endpoint + pull job | scheduled container job: paginated LIST diff + streaming copy to a second, different-provider endpoint | full provider independence | always buildable — it is the §16.1 adapter's `list`/`get`/`put` | strongest; job lands alongside Stage 2's capture loop or later |
| C. Provider backup/snapshot feature | vendor-specific | varies | <checked how / not offered> | record only if A and B both unavailable |

**Recommendation:** <A if the matrix shows a working replication API and the provider failure
domain is acceptable; otherwise B>. **Timing:** N12 starts accruing from first capture — the
second copy should exist by the end of Stage 2, or the single-copy period is a known standing
finding from day one, by choice.

**Decision required (operator):** confirm the mechanism (and, for B, the second provider).
Recorded as pending until the plan-completion gate; `doctor` (Stage 7) verifies whichever lands.
```

- [ ] **Step 2: Write findings section 7 (archive-bucket creation parameter sheet)**

Replace section 7's marker with the following, resolving the two `<from matrix>` fields from
Task 6 and keeping everything else verbatim unless the matrix contradicts it (four-backtick
fence because the sheet itself contains a fenced JSON block):

````markdown
Executed by Stage 2 **before the first capture**. §7.3: "the archive bucket must be created with
immutability configured before the first byte lands … the one storage decision that cannot be
corrected later."

Two buckets (§17.4: whole-bucket policy statements; compaction rewrites kept clear of immutable
objects):

| Parameter | raw bucket `bls-stats-raw` | main bucket `bls-stats-main` |
|---|---|---|
| Contents | `raw/` only | `log/`, `ledger/`, `store/`, `ops/` |
| Created | Stage 2, with every row below applied at creation | Stage 2 |
| Versioning | Enabled | Enabled |
| Object lock | `ObjectLockEnabledForBucket=true`; default retention **GOVERNANCE, 3650 days** (a mode *and* a duration — §17.4) | Off — §17.4 generation-swap GC must delete `gen=<n>/` |
| Endpoint supports lock? | <from matrix: object_lock rows> — if NO: fall back per §17.4 to delete-deny alone and record the gap as a standing finding at Stage 7 | — |
| Delete policy | Deny `s3:DeleteObject` + `s3:DeleteObjectVersion` to `"AWS": "*"` on `arn:aws:s3:::bls-stats-raw/*` (JSON below); enforced for the runtime credential — <from matrix: delete_deny_policy row, incl. whether a distinct non-admin runtime credential is required> | runtime credential MAY delete (generation GC) |
| Lifecycle | **None. `raw/` never expires; no tiering** (§17.4, §1.4) | none initially |
| Region / endpoint | as configured (`AWS_ENDPOINT_URL` — §1.4: never a constant) | same |

Delete-deny policy for the raw bucket, verbatim:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DenyDeleteOnRaw",
      "Effect": "Deny",
      "Principal": {"AWS": ["*"]},
      "Action": ["s3:DeleteObject", "s3:DeleteObjectVersion"],
      "Resource": ["arn:aws:s3:::bls-stats-raw/*"]
    }
  ]
}
```

**Why GOVERNANCE and not COMPLIANCE (recommendation):** COMPLIANCE retention cannot be shortened
by anyone, including the account root — a mis-set duration is permanently unfixable, and the 2025
lesson cuts both ways (irreversibility is the point *and* the risk). GOVERNANCE + the delete-deny
policy gives two independent layers against the runtime credential, with admin
governance-bypass as deliberate break-glass. P1's protection target is the *runtime* path — no
operational code path may be able to delete a vintage.

**Decisions required (operator):**
1. Lock mode + duration: GOVERNANCE/3650d recommended above — confirm or override.
2. Final bucket names: `bls-stats-raw` / `bls-stats-main` are the defaults — confirm or rename
   (names propagate into Stage 2's config, nowhere else yet).
````

- [ ] **Step 3: Consistency check against the matrix**

Re-read findings section 5. Every `<from matrix>` field in sections 6–7 must now hold a concrete
value; if the matrix shows object lock unsupported, section 7's lock row must carry the §17.4
fallback ("delete-deny alone + recorded gap") as the *actual* parameter, not a footnote.
Expected: no unresolved `<from matrix>` markers remain (`grep -n "from matrix" specs/bls-stats-stage1-findings.md` returns nothing).

- [ ] **Step 4: Commit**

```bash
git add specs/bls-stats-stage1-findings.md
git commit -m "Record replication options and archive-bucket creation parameters"
```

---

### Task 8: Findings audit + roadmap tick

**Files:**
- Modify: `specs/bls-stats-stage1-findings.md` (section 8)
- Modify: `specs/bls-stats-spec-roadmap.md` (Stage 1 entry)

**Interfaces:**
- Consumes: findings sections 1–7 complete; the roadmap's Stage 1 exit criteria.
- Produces: the stage's completion evidence; the ticked roadmap entry with its completion stamp.

- [ ] **Step 1: Audit findings against the stage exit criteria**

Check each line; fix gaps in the findings document now (re-running a probe if a result is missing
or undated), not later:

- §20 issue 1 has a dated result and method (section 3) — posture stated per surface.
- §20 issue 2: ingest channel CONFIRMED locked (section 1) — if it is not, Stage 1 cannot exit;
  stop and escalate to your human partner.
- §20 issue 4 sizes are measured, not quoted (section 4).
- §20 issue 14: deployment endpoint identified; container reachability stated (section 5).
- §20 issue 15: options recorded with recommendation (section 6).
- Bucket-creation parameters written, and **no real bucket exists anywhere** (section 7;
  `bls-stats-probe-*` buckets were deleted by the probe runs — verify against the endpoint with
  the operator if any cleanup line reported failure).
- Every section cites script + results file + date.

- [ ] **Step 2: Write findings section 8 (consequences for later stages)**

Replace section 8's marker with a short list derived from the actual results — at minimum, one
line each on:

```markdown
- **Stage 2 (capture):** transport posture per host (sections 1–3): <e.g. headless backend
  in/out of Stage-2 scope; Range support and its effect on streaming GET; Accept-Encoding
  observation>. Bucket creation executes section 7 verbatim; replication decision <status>.
- **Stage 3 (store mechanism):** conditional PUT <absent as assumed / present but unused — §1.4's
  single-writer discipline stands regardless (the design targets the intersection)>.
- **Stage 5 (data plane):** QCEW envelope arithmetic (section 4) → <differ memory-strategy
  implication>.
- **Stage 7 (ops):** `doctor` must report the section-5 matrix live; N11/N12 baselines start
  <dates/state>.
- Roadmap re-validation: <any stage whose Objective/Exit this changes, or "none — findings match
  the roadmap's assumptions">.
```

- [ ] **Step 3: Tick the roadmap**

In `specs/bls-stats-spec-roadmap.md`, change the Stage 1 entry's `- [ ]` to `- [x]` and append,
indented under the stage entry, after its `ROUTING: writing-plans` line:

```markdown
      Stage 1: COMPLETE (<YYYY-MM-DD>) — implemented by plan 1
        (specs/plans/1-bls-stats-stage1-probes.md). Findings:
        specs/bls-stats-stage1-findings.md.
      Next: resume the roadmap.
```

- [ ] **Step 4: Commit**

```bash
git add specs/bls-stats-stage1-findings.md specs/bls-stats-spec-roadmap.md
git commit -m "Complete Stage-1 findings; tick roadmap Stage 1"
```

---

## Completion

After Task 8, run the writing-plans Plan Completion Protocol (resolve-before-defer gate — the two
operator decisions from Task 7 are expected gate items if still pending — then plan markup,
deferred-items update, retirement). Note for the gate: §20 issues 6, 7, and 11 are *not* Stage-1
scope (they resolve by post-`T₀` observation; the roadmap parks them at retirement) — do not
carry them as this plan's leftovers.
