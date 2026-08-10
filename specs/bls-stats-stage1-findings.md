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
| `download.bls.gov` robots.txt | 404 (IIS "File or directory not found" page, `content-length: 1245`, no redirects). No robots.txt exists at this path on this host — no Disallow rules are observable from it. Whether a robots policy is published elsewhere (e.g. `www.bls.gov`) is unprobed here — Task 4 probes `www.bls.gov/robots.txt`. |
| HEAD large LABSTAT file (`ce`) | 200; `Last-Modified: Fri, 07 Aug 2026 12:30:00 GMT`; `ETag: "094a5766826dd1:0"`; `Content-Length: 350208884` (~334 MiB, as reported — no `Content-Encoding` or `Vary` on this response) |
| GET tiny mapping file (`ce.period`) | 200; `body_bytes` (decoded) = 419; on-wire `Content-Length: 209` (`Content-Encoding: gzip`, `content-type: application/octet-stream`) |
| HEAD second prefix (`jt`) | 200; `Last-Modified: Tue, 04 Aug 2026 14:00:00 GMT`; `ETag: "0f0e8a1924dd1:0"`; `Content-Length: 34414209` |
| HEAD QCEW singlefile zip (`data.bls.gov`) | 200; `Content-Length: 304826526` (~291 MiB); `Last-Modified: Tue, 02 Sep 2025 11:29:43 GMT`; `ETag: "122b489e-63dcfcdf704e5"`. The 2024 `data.bls.gov/cew/data/files/2024/csv/2024_qtrly_singlefile.zip` URL pattern is confirmed live — no correction needed for Task 5. |
| Ranged GET (`bytes=0-1023`) | 206 — honored. `Content-Range: bytes 0-1023/47300620`; `content-type: text/plain`; `Content-Encoding: gzip`; `Vary: Accept-Encoding`. Same URL, same `ETag: "094a5766826dd1:0"`, and same `Last-Modified` as the plain HEAD above, but that HEAD reported `Content-Length: 350208884` and `content-type: application/octet-stream` — the total and the content-type diverge on an identical validator. See Consequences for what this does and doesn't establish. |
| HTTP version negotiated | h2 (HTTP/2) on every one of the six requests, on both `download.bls.gov` and `data.bls.gov` |
| Content-Encoding under pinned `Accept-Encoding: gzip` | `gzip` on two flat-file GETs: `ce.period` (JSONL line 3) and ranged CES (JSONL line 6), both with `Vary: Accept-Encoding`. Absent on the 404 page (JSONL line 1: 1245 bytes, uncompressed). HEAD responses omit `Content-Encoding` (no body sent). QCEW `.zip` probed only via HEAD (JSONL line 5); HEAD omits body headers by protocol, so no conclusion on transfer encoding. Task 5's full-file download will measure on-wire bytes and settle any double-compression question. |

**Verdict (issue 2):** CONFIRMED for data retrieval — the compliant contactable-UA
profile is not blocked on either flat-file host — **with the robots policy for
`download.bls.gov` unresolved** (no `robots.txt` was served at that path; see below).
All four data-bearing checks (HEAD `ce.data.0.AllCESSeries`, GET `ce.period`, HEAD
`jt.data.1.AllItems`, HEAD the QCEW singlefile zip on `data.bls.gov`) returned 200 with
full Last-Modified/ETag/Content-Length metadata, and the ranged GET returned 206 with
Range honored. Only `robots.txt` at `download.bls.gov/robots.txt` returned 404 (no
document at that path) — this is why the probe script's own console line printed
`INGEST CHANNEL NOT CONFIRMED`: its `ok` check requires all five non-ranged requests to
equal exactly 200, which is a stricter bar than the compliance question it stands in
for. A 404 is not a block: it is not 403 and not an `httpx.HTTPError`, and none of the
other `download.bls.gov` endpoints show any sign of being throttled or refused. The
ingest channel for actual data retrieval is locked; the robots.txt absence is recorded
here as a separate, minor finding, not a channel failure, and carried forward below.

**Consequences:** Last-Modified and ETag are both present on every flat-file response
observed (CES, `ce.period`, JOLTS on `download.bls.gov`; the QCEW zip on
`data.bls.gov`) → §6.1 change detection has both signals available for all four file
families probed. Range is honored (206) → resumable streaming is available to Stage
2's transport, but with a caveat that this single probe only partially resolves.
JSONL line 2 (plain HEAD on `ce.data.0.AllCESSeries`) reports `Content-Length:
350208884` and `content-type: application/octet-stream`; JSONL line 6 (ranged GET, same
URL, same `ETag: "094a5766826dd1:0"`, same `Last-Modified`) reports `Content-Range:
bytes 0-1023/47300620` (roughly 7.4x smaller) and `content-type: text/plain`. That the
ranged response also carries `Content-Encoding: gzip` while the HEAD carries none is not
by itself informative — HEAD responses on this host omit `Content-Encoding` because no
body is sent (see the Content-Encoding row above), so its absence on line 2 is expected
regardless of what representation is being served. The content-type divergence is the
stronger signal: JSONL line 3 (`ce.period`) shows that on this host `Content-Encoding:
gzip` alone does not flip `content-type` away from `application/octet-stream` — that
response is gzip-encoded and still reports `content-type: application/octet-stream` — so
the ranged GET's `text/plain` is not explained by gzip encoding alone. Combined with the
size divergence, this is **consistent with** the ranged GET running over an
on-the-fly-gzipped encoding of the same resource — but it is at least as consistent with
a separately stored, statically pre-compressed sibling representation, and this probe
did not run any request that could distinguish the two. Nor did it establish whether the
47300620-byte stream is byte-stable across requests: lines 2 and 6 carry the identical
ETag despite reporting different sizes and content-types, so that ETag does not
distinguish the two representations at a single instant and cannot be assumed to change
if the compressed stream does — it remains a valid change-detection signal for §6.1 (did
the source file change) but is not a signal for which representation was served. Stage 2
therefore has two design options on the table, **neither validated by this probe**:
computing byte offsets against the compressed size actually being ranged over (requires
first confirming that stream is stable across requests — unverified here, and a false
assumption would silently corrupt a resumed download); or omitting `Accept-Encoding:
gzip` specifically for range-based bulk transfers to get range semantics against the
uncompressed size HEAD reports (a request shape this probe never issued, so also
unverified, though it carries less risk since it sidesteps the stability question
rather than depending on it — the likelier default for Stage 2 pending a dedicated
probe). HTTP/2 is negotiated on every request to both hosts.

The robots-policy question for `download.bls.gov` is open, not closed: JSONL line 1
records a plain 404 (IIS "File or directory not found," no redirects) for
`download.bls.gov/robots.txt`, which rules out a *published Disallow rule at that path*
but is not evidence of a block — the four data-serving checks above all returned 200 and
none of them show throttling or refusal. Task 4's probe of `www.bls.gov/robots.txt` is
the next place this gets a data point, though a policy found there would describe a
different host and would not by itself resolve whether `download.bls.gov` publishes (or
implies) one.

## 2. API surface — §7.1 `api` profile (Task 3)

What settles it: whether the `api` profile reaches `api.bls.gov`, authenticates with a
registered key, and §7.1's payload-inspection code path runs end-to-end.

**Probe:** `probes/transport_api.py`, run 2026-08-10
(`probes/results/transport_api-2026-08-10.jsonl`).

| Check | Result |
|---|---|
| v1 unregistered GET, HTTP status | 200 |
| v1 top-level `status` field | `REQUEST_SUCCEEDED` |
| v1 `message[]` | `[]` (empty — no registration nag on this run) |
| v1 per-series datapoints | 31 (1 series, `CES0000000001`, HTTP/2, `content-encoding: gzip`) |
| v2 registered POST | HTTP 200; `status` `REQUEST_SUCCEEDED`; `message[]` `[]`; 1 series, 24 datapoints (HTTP/2, `content-encoding: gzip`) |
| HTTP-vs-payload divergence | **Not observed on this run.** Both requests agreed across every signal (200, `REQUEST_SUCCEEDED`, non-empty data, empty `message[]`) — no case arose where HTTP-level and payload-level success disagreed. §7.1's not-sufficient rule is prior design content (see Verdict), not something this run demonstrated. |

**Verdict:** on 2026-08-10, both requests — the v1 unregistered GET and the v2
registered POST — returned HTTP 200, `status: REQUEST_SUCCEEDED`, non-empty per-series
data (31 and 24 datapoints respectively), and an empty `message[]`. HTTP-level and
payload-level signals agreed on both requests; no divergence between them was witnessed
on this run.

What this run does establish, plainly: the `api` profile reaches `api.bls.gov` and
completes requests over it (both records: HTTP/2, `content-encoding: gzip`, series data
returned). The registered v2 path works with a key on hand — the POST carrying the key
returned 1 series and 24 datapoints with an empty `message[]` and no auth-related
message — no auth-failure signal appeared (`.project.env`, git-ignored; exercised live,
spending one query against its daily quota). And the payload-inspection code path
itself executed end-to-end: `payload_verdict()` parses `Results.series`, sums each
series' `data[]`, and records `message[]` into every JSONL line; `success_by_payload`
is computed from series presence and datapoint count (`bool(series) and n_points > 0`),
not from HTTP status — that logic ran and produced a verdict from the payload on both
requests.

§7.1's rule — that HTTP 200 and a top-level `status: REQUEST_SUCCEEDED` are necessary
but not sufficient signals of success — is not established by this run. It is standing
spec policy stated in specs/bls-stats-spec.md §7.1, without in-repo citation of its
evidentiary basis, rather than on anything observed here. The discipline is adopted here by design, not
because this probe caught the API lying, and it still governs Stage 2's implementation.
This run's `message[]` happened to be empty on both requests — the commonly-cited v1
registration nag did not fire this time — so the "not sufficient" half is not
re-demonstrated here: the run is equally consistent with HTTP-level signals having
been sufficient on their own. That is a gap in this run's evidence, not in the rule
(see the table row above); the rule is carried forward unverified-by-probe, not
unsupported. Key registration (annual expiry, §7.1 alert
requirement) is a Stage-2 setup item; the key itself is already provisioned and working
as of this probe.

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
