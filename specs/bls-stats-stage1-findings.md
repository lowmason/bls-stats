# Stage 1 findings — ground-truth probes

**Roadmap:** specs/bls-stats-spec-roadmap.md, Stage 1. **Spec:** specs/bls-stats-spec.md.
**Method:** each finding cites its probe script (`probes/`), raw output
(`probes/results/`), and run date. Service behavior is point-in-time: every result is
evidence for its date, none is assumed stable (re-verify before hard-coding).

## 1. Flat-file ingest channel — §20 issue 2 (Task 2)

What settles it: "confirm the compliant profile passes and lock the ingest channel
before anything else is built."

**Probe:** `probes/transport_flatfile.py`, run 2026-08-10
(`probes/results/transport_flatfile-2026-08-10.jsonl`).

| Check | Result |
|---|---|
| `download.bls.gov` robots.txt | 404 (IIS "File or directory not found" page, `content-length: 1245`, no redirects). No robots.txt exists at this path on this host — no Disallow rules are observable from it. Whether a robots policy is published elsewhere (e.g. `www.bls.gov`) is unprobed and out of scope for this task. |
| HEAD large LABSTAT file (`ce`) | 200; `Last-Modified: Fri, 07 Aug 2026 12:30:00 GMT`; `ETag: "094a5766826dd1:0"`; `Content-Length: 350208884` (~334 MiB, uncompressed) |
| GET tiny mapping file (`ce.period`) | 200; `body_bytes` (decoded) = 419; on-wire `Content-Length: 209` (gzip-compressed) |
| HEAD second prefix (`jt`) | 200; `Last-Modified: Tue, 04 Aug 2026 14:00:00 GMT`; `ETag: "0f0e8a1924dd1:0"`; `Content-Length: 34414209` |
| HEAD QCEW singlefile zip (`data.bls.gov`) | 200; `Content-Length: 304826526` (~291 MiB); `Last-Modified: Tue, 02 Sep 2025 11:29:43 GMT`; `ETag: "122b489e-63dcfcdf704e5"`. The 2024 `data.bls.gov/cew/data/files/2024/csv/2024_qtrly_singlefile.zip` URL pattern is confirmed live — no correction needed for Task 5. |
| Ranged GET (`bytes=0-1023`) | 206 — honored. `Content-Range: bytes 0-1023/47300620`. Note: 47300620 is the size of the **gzip-compressed** representation, not the 350208884-byte uncompressed size the plain HEAD (row 2) reported for the same URL — see Consequences. |
| HTTP version negotiated | h2 (HTTP/2) on every one of the six requests, on both `download.bls.gov` and `data.bls.gov` |
| Content-Encoding under pinned `Accept-Encoding: gzip` | `gzip`, observed on every GET response that carries a body (`ce.period`; the ranged slice of `ce.data.0.AllCESSeries`, both with `Vary: Accept-Encoding`). HEAD responses omit `Content-Encoding` (no body is sent). The QCEW `.zip` (already compressed) is served with no additional Content-Encoding — no double compression. |

**Verdict (issue 2):** CONFIRMED — the compliant contactable-UA profile is not blocked on
either flat-file host. All four data-bearing checks (HEAD `ce.data.0.AllCESSeries`, GET
`ce.period`, HEAD `jt.data.1.AllItems`, HEAD the QCEW singlefile zip on `data.bls.gov`)
returned 200 with full Last-Modified/ETag/Content-Length metadata, and the ranged GET
returned 206 with Range honored. Only `robots.txt` at `download.bls.gov/robots.txt`
returned 404 (no document at that path) — this is why the probe script's own console
line printed `INGEST CHANNEL NOT CONFIRMED`: its `ok` check requires all five
non-ranged requests to equal exactly 200, which is a stricter bar than the compliance
question it stands in for. A 404 is not a block: it is not 403 and not an
`httpx.HTTPError`, and none of the other `download.bls.gov` endpoints show any sign of
being throttled or refused. The ingest channel for actual data retrieval is locked; the
robots.txt absence is recorded here as a separate, minor finding, not a channel
failure.

**Consequences:** Last-Modified and ETag are both present on every flat-file response
observed (CES, `ce.period`, JOLTS on `download.bls.gov`; the QCEW zip on
`data.bls.gov`) → §6.1 change detection has both signals available for all four file
families probed. Range is honored (206) → resumable streaming is available to Stage
2's transport, but with a caveat: the Content-Range total (47300620 bytes) reflects the
gzip-compressed representation, not the uncompressed Content-Length a plain HEAD
reports for the same URL (350208884 bytes). Stage 2's range-based downloader must
account for this when the compliant `Accept-Encoding: gzip` header is pinned — either
compute byte offsets against the compressed size actually being ranged over, or omit
gzip `Accept-Encoding` specifically for range-based bulk transfers to get uncompressed
range semantics matching HEAD's reported size. HTTP/2 is negotiated on every request to
both hosts.

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
