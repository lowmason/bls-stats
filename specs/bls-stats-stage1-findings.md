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
| `Accept-Ranges` (advertised range support) | `bytes` on all four `download.bls.gov` data-bearing responses in this table (JSONL lines 2, 3, 4, 6) and on the QCEW singlefile HEAD on `data.bls.gov` (JSONL line 5) — absent on the `download.bls.gov` 404 (JSONL line 1). This is *advertised* support only, read off the response header; it is weaker than the 206 actually demonstrated on `download.bls.gov` by the Ranged GET row above. This run never issued a ranged GET against `data.bls.gov` (§4's envelope arithmetic notes the same gap), so `data.bls.gov`'s range support is advertised here, not demonstrated — do not upgrade the two to the same evidentiary strength. |
| HTTP version negotiated | h2 (HTTP/2) on every one of the six requests, on both `download.bls.gov` and `data.bls.gov` |
| Content-Encoding under pinned `Accept-Encoding: gzip` | `gzip` on two flat-file GETs: `ce.period` (JSONL line 3) and ranged CES (JSONL line 6), both with `Vary: Accept-Encoding`. Absent on the 404 page (JSONL line 1: 1245 bytes, uncompressed). HEAD responses omit `Content-Encoding` (no body sent). QCEW `.zip` probed only via HEAD (JSONL line 5); HEAD omits body headers by protocol, so no conclusion on transfer encoding. Task 5's full-file download will measure on-wire bytes and settle any double-compression question. |

**Verdict (issue 2):** CONFIRMED for data retrieval — the compliant contactable-UA
profile is not blocked on either flat-file host — **with the robots policy for
`download.bls.gov` unresolved** (no `robots.txt` was served at that path; see below).
All four data-bearing checks (HEAD `ce.data.0.AllCESSeries`, GET `ce.period`, HEAD
`jt.data.1.AllItems`, HEAD the QCEW singlefile zip on `data.bls.gov`) returned 200 with
full Last-Modified/ETag/Content-Length metadata, and the ranged GET returned 206 with
Range honored. Only `robots.txt` at `download.bls.gov/robots.txt` returned 404 (no
document at that path) — this is why the probe script's own console line, **as printed
for this recorded run**, read `INGEST CHANNEL NOT CONFIRMED`: at the time of this run,
the script's `ok` check required all five non-ranged requests to equal exactly 200,
folding the ancillary robots.txt check into the same gate as the four data-bearing
checks — a stricter bar than the compliance question it stands in for. Commit
`e6ebed0` (operator-authorized, after this run) narrowed that predicate to gate only on
the four data-bearing checks plus the ranged GET, reporting the robots.txt result on
its own line instead of folding it into the pass/fail. **The recorded run's printed
line above reflects the original, unnarrowed predicate** — neither the JSONL evidence
nor this section's verdict changes — and a re-run against equivalent results (four
200s, one 206, robots.txt 404) would now print `INGEST CHANNEL CONFIRMED`. A 404 is not
a block: it is not 403 and not an `httpx.HTTPError`, and none of the other
`download.bls.gov` endpoints show any sign of being throttled or refused. The ingest
channel for actual data retrieval is locked; the robots.txt absence is recorded here as
a separate, minor finding, not a channel failure, and carried forward below.

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

**Current posture as of 2026-08-10.** Summary only — the chronological record below
(original run, then the addendum) is the evidence; every claim here is traceable to a
passage further down this section.

- **Browser-shaped profile** (`_lib.BROWSER_HEADERS`): blocked (403, uniform Akamai
  block page) on all six probed `www.bls.gov` surfaces — robots.txt, `.ics` schedule
  feed, Atom release feed, errata table, news-release index, schedule index.
- **Contact profile** (`_lib.CONTACT_HEADERS`): genuine HTTP 200 content on all six of
  the same surfaces, counting the canary — `/errata/` via the canary, the other five via
  the addendum (four by marker match; `schedule_index` by direct inspection of
  `body_head` after its marker missed).
- **For Stage 2:** this supports building `html`-profile ingest on the contact profile
  and leaving the headless-browser backend (§7.2 mitigation 3) unbuilt-and-pluggable,
  rather than treating headless as mandatory.
- **Not established:** *why*. Mechanism is untested — which of the five headers that
  differ between the two profiles (or their combination, or the internal Chrome-claim
  inconsistency in the browser-shaped profile) is doing the work is not known (Confound
  1). The profile-vs-sequence-position question is weakened but not eliminated: this
  run's evidence is consistent with a profile effect, but a fully decisive,
  same-session interleaved-profile probe was not run (Confound 2).

What settles it: "a live probe across all three transport profiles … it determines the
whole HTML ingest posture" — browser-shaped `httpx` sufficient, or headless mandatory.

**Probe:** `probes/transport_html.py`, run 2026-08-10
(`probes/results/transport_html-2026-08-10.jsonl`).

Canary (compliant contact UA, one request): 200 — **updates** the baseline stated at
`specs/bls-stats-spec.md` line 728: "`www.bls.gov` returns 403 to plain `httpx` even with a
contact User-Agent per BLS's stated policy (review §0.3)." That line's own parenthetical
citation does not resolve in-repo — `specs/bls-stats-spec-review.md` has no §0.3 (its
headings are TL;DR / Key Findings / Details (1)-(4) / Recommendations / Caveats), the same
class of unresolved cross-reference section 2 already flags for §7.1's HTTP-vs-payload
rule. The pointer used below is therefore the spec line itself, not the review section it
names. That spec line predicts 403 for exactly this shape of request. JSONL line 1 shows
none: HTTP/2 200, no `server: AkamaiGHost` header (no Akamai *block-page* signature — this
does not by itself prove the request bypassed Akamai's edge entirely, only that it did not
receive an Akamai-generated block page), `content-length: 84713` on the wire
(`content-encoding: gzip`) decoding to 249986 bytes of genuine page — `body_head` opens
`<title>Errata Home : U.S. Bureau of Labor Statistics</title>` with the real page's script
includes (`dap.digitalgov.gov` analytics tag, `/javascripts/bls…`), not a challenge page.

**This is the most consequential result in this run, not a footnote.** *The causal
explanation offered later in this paragraph (a UA/TLS-fingerprint mismatch) is withdrawn —
and narrowed further on the evidence of `_lib.py` — by "Correction to the 'most
consequential result' paragraph above" further down this section — read that before
citing this paragraph's "likelier explanation" sentence. The underlying observation (a
same-URL, same-run A/B where the contact UA passed and the browser-shaped profile failed)
stands unmodified.* The canary and the `errata_table` browser-shaped probe (JSONL lines 1
and 5) hit the *identical* URL (`https://www.bls.gov/errata/`), 8 s apart (JSONL
`probed_at`: 22:22:36 for the canary, 22:22:44 for `errata_table` — corrected here from an
earlier "~15 s" estimate; the tighter window leaves less room for a time-drift
explanation, if anything strengthening this A/B comparison rather than weakening it), from
the same client IP, inside the same script run: the canary (contact UA) got 200 with real
content; the browser-shaped request to the same URL got 403 (Akamai block). That is a
controlled, same-URL, same-run A/B in which the plainer, cheaper, more-honestly-labeled
transport passed exactly where the transport §7.2 was designed around (browser-shaped
headers, meant to *look* more legitimate) failed. The likelier explanation is a
UA/TLS-fingerprint mismatch — `httpx` sending a header claiming Chrome 126 without
Chrome's actual TLS/HTTP-2 handshake is a textbook bot-management trigger, whereas a UA
that doesn't claim to be a browser isn't
held to that consistency check — but the two arms also ran through separate
`make_client()` contexts (separate TLS sessions) at different points in the six-request
sequence, so **session position and IP-reputation drift across the run are not excluded**
as alternative explanations; this run cannot distinguish between them. Either way, this is
one request, to one URL, on one date — it does not establish that the compliant contact UA
passes every `www.bls.gov` path, and per this task's non-negotiable rule it was not
repeated or extended to other surfaces to check. It is recorded here as exactly what it
is: a single dated data point that contradicts the spec §7.2 baseline (line 728) at this one URL,
and that undercuts the premise §7.2's mitigation ordering itself was built on (see
Verdict).

| Surface | Browser-shaped status | Marker found | Posture |
|---|---|---|---|
| robots.txt | 403 | False | Blocked. `body_head` is an Akamai "Access Denied" page (`server: AkamaiGHost`, 1323 bytes, `Cache-Control: no-cache, no-store, must-revalidate`), not a robots.txt document — no Disallow rules are observable from this run. |
| `.ics` schedule feed | 403 | False | Blocked. Identical 1323-byte Akamai block page. |
| Atom release feed (`empsit.rss`) | 403 | False | Blocked. Identical 1323-byte Akamai block page. Because the surface never returned real content, this run also does not settle whether the pinned `<feed` marker (Atom's root element) is even the right string for a URL path named `.rss` — that question stays open behind the block. |
| errata table (`/errata/`) | 403 | **True** (false positive) | Blocked under browser-shaped headers. Same Akamai block page as the other five — its body happens to open with `<html lang="en-us">`, which satisfies the pinned `<html` marker even though no errata content was served. `marker_found: True` here is misleading in isolation; `status != 200` is what correctly keeps this surface out of "passing" per the script's own `blocked` logic. **But** the compliant contact-UA canary (JSONL line 1) hit this exact URL in the same run and got 200 with genuine content — see the canary paragraph above. Of all six surfaces, this is the one with direct same-run evidence that a cheaper transport than headless already works here. |
| news-release index | 403 | **True** (false positive) | Blocked, same false-positive marker mechanism as errata table. |
| schedule index | 403 | **True** (false positive) | Blocked, same false-positive marker mechanism as errata table. |

All six browser-shaped requests returned bodies identical in both recorded respects: the
same `body_bytes` length (1,323) and the same first 500 recorded bytes (`body_head`,
byte-for-byte across all six) — not literally "byte-identical" in full, since `keep_body`
was not set for this probe and bytes 500–1,323 were never captured or compared. What this
run establishes is length-identity plus prefix-identity; full-body identity is consistent
with the data but not itself observed. Combined with `server: AkamaiGHost` on all six, this
reads as a single, uniform Akamai bot-management block, not per-surface variation. Three of the six (errata table, news-release index, schedule index) show
`marker_found: True` only because the pinned `<html` marker is weak enough that an Akamai
challenge page satisfies it trivially; this run's overall verdict is unaffected because
the script's `blocked` predicate requires `status == 200` first, and none of the six
cleared that bar. A future rerun that gets a `200` with a WAF interstitial and this same
weak marker could pass falsely — worth a harder marker (e.g. a phrase only the real page
contains) if Stage 2 ever re-probes these surfaces, though per this task's rules the
pinned script here is left as run, not edited.

**Verdict (issue 1):** **established** — browser-shaped `httpx`, configured exactly as
`_lib.BROWSER_HEADERS` configures it, is blocked (403, uniform Akamai block page) on all
six probed surfaces on 2026-08-10: robots.txt, `.ics` schedule feed, Atom release feed,
errata table, news-release index, schedule index. §7.2 mitigation 2, as currently
implemented, fails on every surface this run tried it against.

**Not established:** that §7.2 mitigation 3 (the headless `HtmlFetcher` backend) is
therefore the necessary next step. This is where this finding **diverges from the
script's own mechanical line** (`HEADLESS BACKEND MANDATORY for: robots_txt,
ics_schedule, atom_feed, errata_table, newsrels_index, schedule_index`), and the
divergence is structural, not a disagreement about the facts: the script's `blocked`
predicate is computed only over `browser` (`[r for r in records if r["surface"] !=
"canary_contact_ua"]`) — by construction it can never see the canary, so it has no way to
notice that the canary passed the *identical* `/errata/` URL that the browser-shaped
request to that same URL failed on, in the same run, 8 s apart, from the same IP (see
the canary paragraph and the errata-table table row above). The mechanical line correctly
answers "did mitigation-2-as-configured pass" (no, on all six); it is not designed to
answer, and does not answer, "is mitigation-3 the correct engineering response" — that
second question is exactly what a same-URL contact-UA pass undercuts. §7.2's own ordering
was built on the line-728 premise (above) that plain/contact-UA `httpx` also 403s
`www.bls.gov` — this run's canary contradicts that premise at one path, which weakens the
case for jumping straight to the *last* rung of the ladder without first testing a much
cheaper one.

**Recommended before Stage 2 commits to building the headless worker:** *Superseded: the
follow-up probe recommended in this paragraph is exactly what the addendum below
(`probes/transport_html_contact.py`) executes — see "Addendum — contact-profile pass over
the remaining five surfaces" further down this section for its results, and "Verdict
(issue 1) — revised after this addendum" for what they settle. This paragraph is left as
run, not deleted, per this task's chronological-record convention; do not re-run this
follow-up as an open item. The addendum's own live recommendation — the question it still
leaves open — is the same-session **interleaved-profile** probe described in "Confound 2
(sequence position) — direct read" below, not a repeat of this paragraph's 5-request
plan.* a single, separate follow-up probe — one contact-UA GET per remaining surface (5
requests, same 2 s spacing, same one-shot discipline as this task) — to see whether the
compliant contact UA that passed `/errata/` today also passes robots.txt, the `.ics` feed,
the Atom feed, the news-release index, and the schedule index. If it does, §7.2's ladder
may stop at "use the contact-UA profile for `html` surfaces too" rather than at headless —
a substantially cheaper Stage 2 outcome than standing up and operating a browser-automation
worker. If it does not, the case for mitigation 3 becomes solid rather than assumed.
Pending that follow-up, this run's only unqualified, decisive finding is that
**mitigation 2 as currently configured is not sufficient anywhere it was tried**; whether
mitigation 3 is mandatory, or whether a policy change to the `html` profile's UA would
have sufficed, is the open question this run leaves for that next probe to close. Note:
all of the above is a WAF property observed on 2026-08-10; §18.3 forbids treating it as
stable — the posture is re-checked whenever an HTML surface starts failing in operation,
and the recommended follow-up probe should itself be dated when it runs.

**robots.txt — the cross-task item.** *Superseded below for `www.bls.gov`: this
paragraph's "no policy text was retrieved... still fully open for both hosts" describes
the state before the addendum's contact-profile pass retrieved and reproduced the full
`www.bls.gov` robots.txt — see "robots.txt content and its bearing on this
project's paths" further down this section for the current state. `download.bls.gov` and
`data.bls.gov` remain as described here — unresolved by the addendum, which probed
`www.bls.gov` only.* Task 2 found `download.bls.gov/robots.txt` returns
404 (no document at that path; specs/bls-stats-stage1-findings.md §1). This run's
`www.bls.gov/robots.txt` request (JSONL line 2) returned 403 — the same Akamai block page
as every other browser-shaped surface here, not a robots.txt body. No policy text was
retrieved from either host this stage: `download.bls.gov` has no document at that path,
and `www.bls.gov` never got past the WAF to reveal whether one exists there. The R12
robots-policy question is therefore still fully open for both hosts under the profiles
tested so far — this run neither confirms nor rules out a published policy at
`www.bls.gov`, and even if it had returned one, a `www.bls.gov` policy would describe
`www.bls.gov` only; it would say nothing by itself about `download.bls.gov` or
`data.bls.gov`. (Whether the compliant contact-UA profile would reach
`www.bls.gov/robots.txt` is unprobed here — the canary spent its one contact-UA request on
`/errata/`, per this task's one-canary rule — and is a candidate for a future, separate,
single-request probe rather than a retry within this one.)

### Addendum — contact-profile pass over the remaining five surfaces (2026-08-10)

**Why this addendum exists.** The run above leaves a hole, not a posture: only one of six
`www.bls.gov` surfaces (`/errata/`, via the canary) has a *passing* contact-profile data
point — the browser-shaped profile is blocked on all six, and the contact profile is
untested on the other five. `probes/transport_html_contact.py` (new file;
`probes/transport_html.py` and `probes/_lib.py` untouched) fills that hole: it probes the
remaining five surfaces once each — not `/errata/`, which already has a result — using
`_lib.make_client()` (the contact profile) exclusively, ordered robots.txt → `.ics` →
release feed → newsrels index → schedule index, ≥2 s apart, run once. (The prior run's
`atom_feed` surface key and this addendum's `release_feed` key name the same URL,
`/feed/empsit.rss`.)

**Confounds this addendum's design targets, not resolves.** Two confounds make any "the
User-Agent caused the block" reading unsupported by either run:

1. **Five headers differ at once**, not one: `_lib.CONTACT_HEADERS` and
   `_lib.BROWSER_HEADERS` differ in `User-Agent`, `Accept`, `Accept-Language` (absent vs.
   present), `Accept-Encoding` (`gzip` vs. `gzip, deflate, br`), and
   `Upgrade-Insecure-Requests` (absent vs. present) — and a request claiming Chrome 126
   while sending none of `sec-ch-ua`, `sec-ch-ua-platform`, or `Sec-Fetch-*` is internally
   inconsistent on its face. Nothing below isolates which of these matters. Mechanism is
   untested and stays untested by this addendum.
2. **Sequence position is confounded with profile** in the run above: the single pass was
   request #1, every block was #2–#7. This addendum's design (robots.txt first, `position`
   recorded on every record) is built to let that confound be read off the results, not to
   eliminate it.

**Probe:** `probes/transport_html_contact.py`, run 2026-08-10, ~15 minutes after the run
above (`probes/results/transport_html_contact-2026-08-10.jsonl`).

| Pos | Surface | Status | What `body_head`/`body_text` actually contained | Posture |
|---|---|---|---|---|
| 1 | robots.txt | 200 | Genuine robots.txt (full body kept via `keep_body=True`): two `User-agent` blocks (`archive.org_bot` and `*`) with `Disallow` paths and a `Sitemap:` line. `content-type: text/plain`, no `server` header. Real policy, not a block page. | **Pass** |
| 2 | `.ics` schedule feed | 200 | Genuine ICS calendar: opens `BEGIN:VCALENDAR`, `PRODID:-//Department of Labor//Bureau of Labor Statistics//EN`. `content-type: text/calendar`. | **Pass** |
| 3 | release feed (`empsit.rss`) | 200 | `<?xml version='1.0' encoding='UTF-8'?><feed xmlns="http://www.w3.org/2005/Atom">…<title>Employment Situation</title>…` — a well-formed Atom document, served with `content-type: application/rss+xml` on the wire despite the `.rss` path and the Atom body. This settles the flavor question for *this response*, on this date, under the *contact* profile: the pinned `transport_html.py`'s `<feed` marker would have matched had the browser-shaped request to this same URL not been blocked before returning a body — but that request never returned a body, so this run has no observation of what the browser-shaped profile would have been served, and BLS pairing an Atom body with an `application/rss+xml` content-type is exactly the kind of inconsistency that argues for Stage 2 parsing this feed defensively (checking for either root element) rather than keying ingestion on the `.rss` path or the content-type header. | **Pass** |
| 4 | news-release index | 200 | Real BLS index page: `<title>Economic News Releases :  U.S. Bureau of Labor Statistics</title>`, same site template/analytics includes as the errata canary. | **Pass** |
| 5 | schedule index | 200 | 302 redirect from `/schedule/` to `/schedule/2026/08_sched.htm`, then 200. Real page: `<title>Schedule of Selected Releases for August 2026</title>`, same `dap.digitalgov.gov` analytics tag and `/javascripts/bls-…` includes as every other genuine BLS page in this dataset. **The script's own marker (`"U.S. Bureau of Labor Statistics"`) did not match** — this page's `<title>` genuinely doesn't carry the site-wide suffix the other pages use, but that's not the whole story: markers are checked only against `body_head`, and `_lib.probe()` truncates `body_head` to the first 500 decoded bytes of the response (`r.content[:500]`); `keep_body` was not set for this surface, so nothing past that window was ever recorded or searched. The marker miss is therefore partly a truncation artifact, not solely a title-wording difference — this run has no evidence either way about what, if anything, lies past byte 500 here. (Observation for future re-probes, not a change to this run: neither of this dataset's two candidate template strings survives the 500-byte truncation reliably — `newsrels_index`'s `body_head` contains the title suffix within its first 500 bytes but not the `dap.digitalgov.gov` analytics-tag include, which its leading whitespace pushes past the cutoff; `schedule_index`'s `body_head` contains the analytics-tag include within its first 500 bytes but not the title suffix, which its own title lacks. A future fix needs `keep_body=True`, or a marker validated against the full body rather than a different 500-byte-window string.) Direct inspection of `body_head` shows unambiguous genuine content, not a block page (`looks_like_block_page: false`, no `server: AkamaiGHost`, no "Access Denied" signature). Recorded as a pass on inspection, overriding the script's own marker miss. | **Pass** (on inspection; script recorded `other`) |

All five requests returned HTTP 200 with genuine BLS content; **zero Akamai-style blocks
occurred in this run.** None of the five responses carried a `server` header.

**robots.txt content and its bearing on this project's paths (R12, live compliance
item).** The document has two `User-agent` blocks:

- `User-agent: archive.org_bot` (specific): `Disallow` on `/include`, `/scripts`, `/crs`,
  `/_private`, `/iisadmin`, `/srchadm`, `/advisory/members/`, `/idcf`, `/*print*`,
  `/schedule/archives/`, `/*.PDF$`, `/data.json`.
- `User-agent: *` (generic — the block that applies to this project's UA, which is not
  `archive.org_bot`): `Disallow` on `/scripts`, `/crs`, `/_private`, `/iisadmin`,
  `/srchadm`, `/advisory/members/`, `/idcf`, `/*print*`.

None of the six `www.bls.gov` paths this project's probes have used to date
(`/robots.txt`, `/schedule/news_release/bls.ics`, `/feed/empsit.rss`, `/errata/`,
`/bls/newsrels.htm`, `/schedule/` and its `/schedule/2026/08_sched.htm` redirect target)
match any `Disallow` prefix in the generic block. (`/schedule/archives/` is disallowed
only under the `archive.org_bot`-specific block; it does not apply to the generic rule and
is not a path this project fetches.) **Under the generic `User-agent: *` rule published at
`www.bls.gov` on 2026-08-10, none of the six paths this task actually probed are
disallowed.**

**Scope of this clearance — it covers the six probed paths, not every `html`-profile
surface §7.1/§11.3 name.** The spec references at least three more `www.bls.gov` surfaces
this task did not probe: the archived news-release index
(`https://www.bls.gov/bls/news-release/home.htm`, spec §11.3), per-release archive links
(`.../archives/{slug}_MMDDYYYY.htm`, spec §11.3 — the exact path prefix isn't pinned down
enough in the spec to check against the `/schedule/archives/`-style `Disallow` entries
above, so this is left explicitly unverified rather than assumed clear), and per-program
dated notices pages (e.g. `https://www.bls.gov/cex/notices/2025/ce-2024-reschedule.htm`,
spec §12.5, one example of a path pattern that varies by program). None of these were
requested by either run in this stage. Stage 2 should check any additional `html`-profile
path actually used against this robots.txt (or a re-fetched one, per the §18.3 point-in-time
caveat) before relying on it — this finding does not extend blanket clearance to `html`
surfaces beyond the six listed above.

Host scoping, stated precisely per this task's instruction: this policy is published *at*
`www.bls.gov` and states a policy for `www.bls.gov` only. It says nothing about
`download.bls.gov` (Task 2 found no `robots.txt` at that path at all — a plain 404, not a
block, not a policy) or `data.bls.gov` (still unprobed for `robots.txt`). The R12
robots-policy question is now answered for `www.bls.gov` and remains open for the other
two hosts.

**Confound 2 (sequence position) — direct read.** This run recorded zero blocks at any of
the five positions, all within a single continuous contact-profile session (one
`make_client()` context reused for every request, ≥2 s apart; position 5's `probed_at` is
8 s after position 1's) — the same request cadence as the six browser-shaped requests in
the run above, none of which passed. The pattern does **not** track sequence position the
way the prior run's did: there, blocks began at position 2 and continued through position
7 regardless of profile detail; here, position 5 (the deepest position probed in this
session) passed cleanly, same as position 1. This is inconsistent with a pure "anything
after the first request in a session gets blocked" rule holding *independent of profile*,
and it is further undercut by a fact already recorded in the run above: `robots_txt` under
the browser-shaped profile was itself the very first request of its own client session
(JSONL line 2 of `transport_html-2026-08-10.jsonl`, ~2 s after the canary) and was still
blocked — so "the first request of a session always passes" was never true either. Taken
together, the explanation most consistent with both runs' data is that something
correlated with **profile** (which, per confound 1, could be the User-Agent, one of the
other four differing headers, the internal Chrome-claim inconsistency, or some
combination — this addendum does not and cannot distinguish among them) is doing the
work, not raw position-in-sequence alone. **What this run does not do:** it never ran a
single session that *interleaves* the two profiles request-by-request against the same
URLs, which is the one design that would cleanly separate "profile" from "position" as
rival explanations rather than merely making the position-only explanation less
parsimonious. That interleaved design is the natural next probe if full separation is
wanted.

**Observation, recorded without theorizing about it (per this task's instruction):** every
genuine-content response across both runs — the original canary and all five of this
addendum's passes — carried no `server` response header. Every one of the six
Akamai-block responses in the run above carried `server: AkamaiGHost`. No mechanism claim
follows from this; it is recorded as a consistent, repeated correlation across six
successful and six blocked responses on this date (the canary plus the addendum's five
passes on one side; all six browser-shaped requests on the other — matching the
enumeration two sentences above, not the "seven" this sentence originally said).

**Correction to the "most consequential result" paragraph above (added by this
addendum).** That paragraph states: *"The likelier explanation is a UA/TLS-fingerprint
mismatch — `httpx` sending a header claiming Chrome 126 without Chrome's actual TLS/HTTP-2
handshake is a textbook bot-management trigger…"* That sentence is withdrawn as written —
not deleted from the record above, but disclaimed here: it names a causal mechanism
(fingerprint mismatch) that neither that run nor this addendum tested, and the hedge later
in the same paragraph ("this run cannot distinguish between them") does not fully cancel
having named a "likelier" cause first. Per confound 1 above, five headers differ between
the two profiles simultaneously, plus an internal Chrome-claim inconsistency; no result in
either run isolates the User-Agent, the TLS/HTTP-2 handshake, or any other single header as
the operative variable. The paragraph's underlying *observation* (a same-URL, same-run A/B
where contact passed and browser-shaped failed) stands unmodified; the causal explanation
offered for it does not, and is treated from this addendum forward as untested.

The withdrawal is in fact stronger than the paragraph above states, once `_lib.py` is
read rather than assumed. `_lib.make_client()` builds both the contact-profile and the
browser-shaped clients with identical `http2=True`, identical `timeout`, and identical
`follow_redirects=True` — the only argument that differs between the two branches is the
`headers` dict (`CONTACT_HEADERS` vs. `BROWSER_HEADERS`). Both arms therefore ran over the
same underlying `httpx` TLS/HTTP-2 client stack; a genuine TLS-*fingerprint* difference
between the two requests could not have existed, because nothing in the TLS/HTTP-2 layer
itself was varied. What remains conceivable is narrower than "TLS-fingerprint mismatch" as
originally worded: an interaction between the claimed User-Agent header and that one
shared TLS/HTTP-2 handshake (a Chrome-claiming header riding a handshake that isn't
Chrome's) is not ruled out by this data — but it is untested by either run, exactly as the
rest of this correction says, and no claim about what *did* cause the blocks follows from
noting it.

**Safety check — no lasting IP block observed (R12).** `_lib.BROWSER_HEADERS`'s
User-Agent carries no contact address, and this task's design mandated using it anyway
(§7.2 mitigation 2 has to be tried as specified to be ruled out) — tripping Akamai
bot-management 6/6 on `www.bls.gov` is a designed-in consequence, not a defect. But R12
warns that a blocked IP can forfeit Stage 2's capture, so whether that trip left a lasting
block is itself a finding, not a formality, and the timestamps across this stage's runs
settle it as far as they go: the six 403s above ran 22:22:38–22:22:48 UTC
(`transport_html-2026-08-10.jsonl`, lines 2-7). The contact profile then got clean HTTP
200s **on the same host**, `www.bls.gov`, roughly fifteen minutes later, at
22:37:53–22:38:01 UTC (`transport_html_contact-2026-08-10.jsonl`, all five records — the
addendum's own results above). Separately, `data.bls.gov` — a different host, never
blocked — ran eight sequential HEADs at 23:05:58–23:06:13 UTC
(`qcew_sizes-2026-08-10.jsonl`), seven 200s for 2019–2025 and one 404 for 2026 (a
resource-not-found, not a block; §4), immediately followed in the same script run by the
single streaming GET of the 287,026,419-byte 2025 singlefile zip (no separate timestamp is
recorded for the GET itself in the JSONL, but it runs immediately after the HEAD sequence,
per `probes/qcew_sizes.py`'s own control flow). So: deliberately tripping Akamai
bot-management on `www.bls.gov` on 2026-08-10 produced no lasting block observable on
either host tested afterward — `www.bls.gov` itself (contact profile, ~15 min later) or
`data.bls.gov` (a different host, ~43 min later). Scoped honestly, not further: no
post-block datum exists for `download.bls.gov` or `api.bls.gov` — both were probed
**earlier** in the session, before the 22:22:38 block (`download.bls.gov`:
21:28:18–21:28:28 UTC, `transport_flatfile-2026-08-10.jsonl`; `api.bls.gov`:
21:56:22–21:56:24 UTC, `transport_api-2026-08-10.jsonl`), so this run says nothing about
whether either of those hosts would show a lasting block, only that `www.bls.gov` and
`data.bls.gov` did not.

**Verdict (issue 1) — revised after this addendum.** The verdict recorded above answered
one question decisively — mitigation 2, as currently configured, fails on every surface
tried — and left a second question open: whether mitigation 3 (headless) is the necessary
next step. This addendum answers the second question with new evidence, though not
completely:

- **Established, both runs combined:** on 2026-08-10, the browser-shaped profile exactly
  as `_lib.BROWSER_HEADERS` configures it was blocked (403, uniform Akamai block page) on
  all six probed `www.bls.gov` surfaces. The contact profile exactly as
  `_lib.CONTACT_HEADERS` configures it returned HTTP 200 with genuine content on all six
  distinct `www.bls.gov` surfaces tried across both runs (`/errata/` via the canary; the
  other five via this addendum) — a clean, uniform result on HTTP status (200 on all six,
  403 on none) and on content genuineness (no Akamai block page on any of the six). It is
  not literally zero exceptions on the *adjudication* axis: of the addendum's five
  surfaces, four passed by marker match (`outcome: "pass"` in the JSONL — robots.txt,
  `.ics` schedule, release feed, news-release index) and the fifth, `schedule_index`,
  passed only on direct inspection of `body_head` after its marker missed (`outcome:
  "other"` in the JSONL; see the table row above and the discussion below). That one
  script-level exception travels with this headline claim rather than being papered over
  by it.
- **Not established:** *why*. Confound 1 (five headers plus an internal inconsistency)
  means no claim about the User-Agent specifically, about TLS/HTTP-2 fingerprinting, or
  about "browser-shaped headers are counterproductive," follows from this data — those are
  mechanism claims this addendum was explicitly scoped not to make.
- **Confound 2, substantially weakened but not eliminated:** this addendum's all-contact,
  five-position session produced zero blocks at any position, including its deepest
  position (5) — unlike the run above, where blocks began at position 2 regardless of
  profile detail and even a position-1-of-a-fresh-session request under the browser-shaped
  profile was blocked. Sequence position alone, independent of profile, does not explain
  both runs' data as well as it explains either run in isolation. A fully decisive test —
  interleaving both profiles within one session against the same URLs — was not run and is
  the concrete next step if the residual ambiguity needs closing.

**Where this leaves issue 1: partly settled.** For Stage 2's actual engineering
decision — does `html`-profile ingest need a headless-browser fetcher backend (§7.2
mitigation 3), or does the existing, cheaper contact profile suffice — the evidence built
across both runs supports building `html` transport on the contact profile and leaving
mitigation 3 unbuilt-and-pluggable, rather than defaulting straight to headless: six for
six surfaces passed under the contact profile, six for six failed under the browser-shaped
profile, on the same date, with confound 2 weakened (not eliminated) by this addendum's
own within-session evidence. This is not the same as calling issue 1 fully closed: the
mechanism is untested (confound 1) and the position/profile separation is incomplete
(confound 2). What would settle the remainder: (a) a same-session, interleaved-profile
probe against the same URLs, to cleanly separate profile from position as explanations;
(b) a repeat of both profiles on a different date — §18.3 governs here exactly as it did
above, this is WAF behavior observed on 2026-08-10, not a stable property, and the posture
must be re-checked before being hard-coded as a permanent assumption or the moment an
`html` surface starts failing in production.

## 4. QCEW singlefile sizes — §20 issue 4, sizes half (Task 5)

What settles it: "actual byte sizes … the review's size figures are third-party."
Diff *cost* is measured at Stage 5 (roadmap).

**Probe:** `probes/qcew_sizes.py`, run 2026-08-10
(`probes/results/qcew_sizes-2026-08-10.jsonl`). The review's third-party size figures are
now superseded by measurement. This is the stage's one full-file download (Global
Constraints); eight sequential HEADs (2019–2026) preceded the single streaming GET, ≥2 s
apart throughout, per `_lib.probe`.

**Script provenance note.** `probes/qcew_sizes.py` was amended after this run (commit
`78e6865`, operator-authorized) to add a status check before any bytes are written to
disk, make the scratch-file cleanup unconditional (`try`/`finally`), and stamp
`probed_at` on every record. The committed JSONL above predates those changes — it was
produced by the pre-fix script — so its `downloaded` record below carries no `status`,
`http_version`, or `content_length` field, and its `downloaded`/per-member/`peak_rss_bytes`
records carry no `probed_at`. No measured figure (bytes, line counts, RSS) changes; only
additional provenance fields exist in runs made after this fix.

Per-year singlefile zip (HEAD Content-Length):

| Year | Status | Compressed bytes |
|---|---|---|
| 2019 | 200 | 304,975,812 |
| 2020 | 200 | 304,160,079 |
| 2021 | 200 | 312,925,144 |
| 2022 | 200 | 301,667,725 |
| 2023 | 200 | 323,674,437 |
| 2024 | 200 | 304,826,526 |
| 2025 | 200 | 287,026,419 |
| 2026 | 404 | — (no `content-length` on the 404 body; this is data, not a probe error) |

The 2026 row records what was actually observed: a plain 404 at this URL pattern, nothing
more. The likely explanation — 2026 has no quarterly release yet as of this run's date —
is inference, not observation; this run did not check for a 2026 file under any other
naming or confirm a release calendar, so a differently-named or differently-pathed 2026
artifact existing is not ruled out by this 404 alone.

2024's Content-Length here (304,826,526) matches Task 2's independent HEAD of the same
URL in §1 above exactly — same date (2026-08-10), ~1.6 hours apart (Task 2's probe:
21:28:26 UTC; this run's: 23:06:09 UTC, both JSONL `probed_at`). Two observations less
than two hours apart agreeing is consistent with a stable artifact but says nothing about
stability across dates (§18.3 point-in-time caveat still applies; a same-day repeat is not
a cross-date check). 2025 was the newest year returning 200, so it is the year downloaded.

Measured download (2025): 287,026,419 bytes on the wire; members:

| Member | Compressed | Decompressed | Lines |
|---|---|---|---|
| `2025.q1-q4.singlefile.csv` | 287,026,235 | 2,199,079,362 | 14,635,261 |

**The member name is itself a finding, not just a label.** `2025.q1-q4.singlefile.csv`
names all four quarters of 2025 in one file — this "quarterly singlefile" is a
per-*year* artifact, not a per-quarter file: that much is direct observation, from the
member's own filename plus its scale (2.2 GB decompressed, 14.6M lines for one year). That
the artifact bundles *every quarter released so far* for a year — as opposed to what a
same-pattern zip would contain for a year still in progress, before all four quarters are
out — is inference, not observation: this run measured only one year (2025), and its zip
member already names all four quarters, so mid-year behavior (what the file looks like
partway through a year) was not observed and remains open. That contradicts a premise
stated in specs/bls-stats-spec.md §8.3: *"QCEW's quarterly singlefile covers *one
quarter*"* (the sentence motivating `authoritative_scope`'s per-quarter framing there).
This probe did not open the CSV to confirm its column layout (e.g. whether a `qtr` column
disambiguates rows, or whether each release simply appends/overwrites the whole year's
rows).

**The contradiction has a direction, and it matters which way it runs.** §8.3's stated
hazard is *over-deletion*: if the differ treated absence-from-a-file as
deletion across the whole `program × ref_year` partition while the artifact only covers one
quarter, "every non-Q1 release would mass-delete the other three quarters." §8.3's fix is
to scope `deleted` rows to `artifact.authoritative_scope`, which R13 settled as per-quarter
(spec §20 issue 4; not reopened here). But if the artifact is actually per-year while
`authoritative_scope` stays declared per-quarter, the failure mode runs the *opposite*
way: for the three quarters present in the artifact but outside its declared per-quarter
scope, genuine deletions would never be emitted — **under-deletion**, rows that should be
marked removed but silently aren't. That is a quieter failure than over-deletion (nothing
visibly mass-deletes; the record just quietly fails to reflect a removal) and it changes
how urgently Stage 5 must resolve which frame governs before hardening the differ. Per this
task's scope (sizes, not the differ's design), no fix to §8.3's `authoritative_scope`
framing is proposed here; this direction is recorded as a measured fact for Stage 5 to
reconcile against the spec text.

**HEAD Content-Length vs. bytes actually received — the check Task 2's `download.bls.gov`
finding (§1) flagged as necessary.** HEAD reported `content-length: 287026419` for the
2025 URL (JSONL, HEAD record for year 2025); the streaming GET wrote exactly
`bytes_on_disk: 287026419` to disk (JSONL, `downloaded` record) — **these agree exactly.**
But the two checks are not the same comparison, and should not be read as testing the two
hosts equivalently. Task 2's `download.bls.gov` finding (§1) compared a plain HEAD
`Content-Length` (350,208,884) against a *ranged GET's* `Content-Range` total (47,300,620,
`probes/results/transport_flatfile-2026-08-10.jsonl` — an unexplained ~7.4x divergence).
This run compared a plain HEAD `Content-Length` against *bytes written to disk by a full,
unranged streaming GET* (`bytes_on_disk`) on `data.bls.gov`. This run did not issue a
ranged GET against `data.bls.gov`, so it did not repeat Task 2's exact
ranged-GET-vs-HEAD comparison on this host — that comparison simply was not run here; this
finding neither confirms nor contradicts what a ranged GET against `data.bls.gov` would
show. Per this task's instruction, no cause is asserted for either host's
behavior; the honest statement of what this run shows is narrower than "no discrepancy on
`data.bls.gov`": HEAD `Content-Length` and full-GET bytes-on-disk agree for this file, on
this date, on `data.bls.gov`, under a different comparison than the one that surfaced the
divergence on `download.bls.gov`. (Separately, and not to be confused
with the check above: the zip member's own `compress_size` field, 287,026,235, is 184
bytes less than `bytes_on_disk`, 287,026,419 — that gap is ordinary single-member zip
container overhead, i.e. local file header, central directory record, end-of-central-directory
record, around the one compressed CSV stream, not a transport anomaly. Both numbers come
from the same downloaded file; there is nothing to reconcile against a second, independent
source the way the Content-Length-vs-bytes-on-disk check above does.)

Peak RSS of this measurement (the probe process streaming the download and counting
newlines chunk-by-chunk, never `.read()`-ing the CSV): **78,807,040 bytes (~75.2 MiB)** —
roughly 102x (decimal, 8,000,000,000 / 78,807,040) to 109x (binary, 8,589,934,592 /
78,807,040) under the 8 GB budget, confirming the streaming/chunked approach the script
uses (never loading the ~2.2 GB decompressed CSV, let alone the ~14.6M-line file, into
memory at once). This number describes *this probe's* memory use; it is not a measurement
of Stage 5's differ, which does not exist yet — see the envelope arithmetic below for what
this run does and does not say about that.

**Envelope arithmetic (§1.4, informing Stage 5):** the measured decompressed member is the
*full year* (four quarters concatenated in one CSV, per BLS's own singlefile format) —
D_year = 2,199,079,362 bytes, exactly as measured above. **This is the frame this probe
actually measured, and the year file is the only artifact reachable via the one URL
pattern this run probed** (`data.bls.gov/cew/data/files/{year}/csv/{year}_qtrly_singlefile.zip`):
the zip contains a single member, `2025.q1-q4.singlefile.csv` (see the member-name finding
above), so there is no per-quarter artifact to download at that pattern — obtaining any one
quarter's rows via it requires first ingesting this entire 2,199,079,362-byte decompressed
year file.

**Scope of that claim.** This run probed exactly one QCEW URL pattern; it did not survey
QCEW's other artifact families. §20 issue 4 itself names two this run never touched — the
LABSTAT `en` prefix and the by-size ZIP — and the BLS downloadable-data-files page
(`bls.gov/cew/downloadable-data-files.htm`, the very page `probes/qcew_sizes.py`'s own
fallback error message points at) was never fetched to check for others. So "the year file
is the only artifact reachable at all" overstates this run's evidence; "the only artifact
reachable via the pattern probed" is what it actually shows. This is the same standard the
document already applies to the 2026 404 earlier in this section, above: there, a
differently-named or differently-pathed 2026 artifact existing was explicitly not ruled
out by the 404 alone. Treating this pattern's absence of an alternative as settled while
treating the 2026 case as open would apply that reasoning inconsistently within one
section. Consequently, the ingest cost measured above is real for this artifact family and
this pattern, but calling it **forced, not conditional on any design choice Stage 5
makes** overstates it: it is forced *given this artifact*; whether a different QCEW
artifact family would change the picture is unsurveyed, not ruled out. **This does not
change the conclusion below** — stream rather than materialize — which holds for the
artifact this run actually measured regardless of what an unsurveyed family might show;
only the "no alternative exists" premise is being narrowed here.

What *is* conditional is whether the differ ever needs to hold two such year files in
memory at once — e.g. comparing one vintage of a year against a later vintage of the same
year, rather than quarter-over-quarter. If it does, that working set is 2 × D_year =
4,398,158,724 bytes (~4.10 GiB). Against the 8 GB peak-RSS budget (§1.4) —
8,000,000,000 bytes under the decimal convention, or 8,589,934,592 bytes under the binary
(GiB) convention — that raw-bytes figure **fits**, but with only ~1.82x (decimal,
8,000,000,000 / 4,398,158,724) to ~1.95x (binary, 8,589,934,592 / 4,398,158,724) headroom
— a thin margin, not a comfortable one. This is the frame Stage 5 should lead its sizing
with: it is the one this run's measurement actually supports without an additional
assumption layered on top.

A per-quarter number is also reported, because it is the frame §8.3's stated (and, per the
finding above, contradicted) premise assumes the diff unit to be — but it is derived, and
available only *after* the D_year ingest above, never as a standalone download or a
substitute for it. This run did not download per-quarter files, so a single quarter's size
is not measured directly; approximating it as an even split, D_quarter ≈ D_year / 4 =
549,769,840.5 bytes (~524 MiB) — an approximation, not a measurement, and it assumes the
four quarters are close to equal size (plausible for a stable annual dataset, unverified
here). A naive two-quarter in-memory diff — holding two quarters' decompressed CSV bytes
simultaneously, before any parsing, and reachable only once the year file above has already
been ingested and split into quarters — would then hold ~2 × D_quarter = D_year / 2 ≈
1,099,539,681 bytes (~1.02 GiB), fitting the same budget with roughly 7.3x (decimal) to
7.8x (binary) headroom (8,000,000,000 / 1,099,539,681 ≈ 7.28; 8,589,934,592 / 1,099,539,681
≈ 7.81). Which frame actually governs `authoritative_scope` is a §8.3 design question this
probe does not resolve (see the member-name and contradiction-direction findings above) —
both numbers are reported so Stage 5 does not have to re-derive either from raw
measurements, but the year frame above, not this one, is the frame this run's evidence
actually establishes.

Both figures above are a **lower bound only**, and should be read as one: they count
decompressed CSV bytes, not the in-memory *parsed* footprint (DataFrame columns, dtype
expansion, string interning, per-row Python object overhead if a naive row-oriented
structure were used) that Stage 5's actual differ would carry — a quantity this probe does
not measure and did not attempt to estimate with an invented multiplier, per this task's
instruction. What arithmetic (not estimation) shows: at the year frame's
4,398,158,724-byte working set, a parse overhead as low as 2x — chosen here only to check
the boundary, not asserted as the true multiplier — would put the parsed footprint at
2 × 4,398,158,724 = 8,796,317,448 bytes, which is over the 8,000,000,000-byte decimal
budget by 796,317,448 bytes and over the 8,589,934,592-byte binary budget by 206,382,856
bytes. A 2x parse multiplier alone, before any larger multiplier a real parser might carry,
already busts both conventions of the 8 GB budget at the year frame. The per-quarter
frame's larger ~7.3-7.8x raw-bytes headroom does not change this: it is reachable only
downstream of the year-frame ingest step whose own margin is this thin.

**Conclusion:** on this lower-bound evidence, the honest reading is that Stage 5 should
plan its differ to **stream rather than assume it may materialize whole years (or,
downstream of that, full quarters) in memory**: the frame this probe's measurement actually
supports — two whole-year files, 4.10 GiB — leaves only ~1.8-1.95x headroom on raw
decompressed bytes alone, and the boundary check above shows that headroom does not survive
even a modest, illustrative 2x parse multiplier. The more comfortable per-quarter margin
does not rescue this: it is available only after paying the year-frame ingest cost whose
margin is this thin, and it inherits the same unmeasured-parse-overhead risk. Stage 5 must
measure actual parsed-in-memory RSS directly (R18's memory-envelope gate) before relying on
any degree of materialization, since this probe's ~75 MiB peak RSS describes only the
streaming *measurement* process above (never materializing the CSV), not a parsed diff of
any size. Diff cost itself (the join/compare work, as opposed to the load) is measured, not
argued, at Stage 5 (roadmap, issue 4).

## 5. Object-store endpoint capability matrix — §20 issue 14, §1.4, §17.4 (Task 6)

What settles it: "confirming the deployment endpoint's address and reachability from
the container network," plus which §17.4 requirements the endpoint can satisfy.

**Probe:** `probes/objstore_capabilities.py`, run 2026-08-10
(`probes/results/objstore-workstation-dev-127.0.0.1-2026-08-10.json`). Contexts probed:
`workstation-dev` only. The two deployment contexts (`workstation-deploy`, `container`)
did not run this session — both are blocked on missing operator inputs, not a probe
failure; see the issue-14 verdict below.

| Capability (§17.4 / §1.4) | dev endpoint | deployment (workstation) | deployment (container) |
|---|---|---|---|
| Reachable + authenticated | ok — 2 buckets visible | **blocked** — no deployment endpoint credentials | **blocked** — no deployment endpoint credentials; no container runtime on PATH |
| Conditional PUT (`If-None-Match`) | enforced (`PreconditionFailed`) | **blocked** — no deployment endpoint credentials | **blocked** — no deployment endpoint credentials; no container runtime on PATH |
| Versioning | Enabled; 2 versions after two PUTs | **blocked** — no deployment endpoint credentials | **blocked** — no deployment endpoint credentials; no container runtime on PATH |
| Object lock at bucket creation | Enabled | **blocked** — no deployment endpoint credentials | **blocked** — no deployment endpoint credentials; no container runtime on PATH |
| Retention with a duration (GOVERNANCE/3650d) | accepted | **blocked** — no deployment endpoint credentials | **blocked** — no deployment endpoint credentials; no container runtime on PATH |
| Locked-version delete denied | enforced (`InvalidRequest`) | **blocked** — no deployment endpoint credentials | **blocked** — no deployment endpoint credentials; no container runtime on PATH |
| Delete-deny bucket policy enforced | NOT ENFORCED (root credential bypasses bucket policy) | **blocked** — no deployment endpoint credentials | **blocked** — no deployment endpoint credentials; no container runtime on PATH |
| Lifecycle API | accepted, 1 rule readable | **blocked** — no deployment endpoint credentials | **blocked** — no deployment endpoint credentials; no container runtime on PATH |
| Replication API | API present, none configured (`ReplicationConfigurationNotFoundError`) | **blocked** — no deployment endpoint credentials | **blocked** — no deployment endpoint credentials; no container runtime on PATH |
| PUT/HEAD/GET/LIST medians (1 KiB, ms) | 1.4 / 1.1 / 1.1 / 0.9 | **blocked** — no deployment endpoint credentials | **blocked** — no deployment endpoint credentials; no container runtime on PATH |

This table shows 10 of the run's 12 `checks`; `create_bucket.plain` ("created") and
`object_lock.put_retention` (folded into the Object lock discussion in section 7) are
omitted here for brevity — the full matrix is in
`probes/results/objstore-workstation-dev-127.0.0.1-2026-08-10.json`.

**Issue 14 verdict:** not answerable in this run. Issue 14 asks for the deployment
endpoint's address and its reachability from the container network; both of the
execution-time inputs needed to answer that are missing on this workstation this
session: (1) **deployment endpoint credentials** — `.project.env`'s `AWS_*` values name
the dev MinIO endpoint (`127.0.0.1:9000`) only, and no other credential source exists
here, so `workstation-deploy` could not run; (2) **a deployment-side container
shell/runtime** — no `docker`, `podman`, or equivalent is on PATH, so the `container`
context could not run either, independent of (1). Per the brief's own scoping, this task
did not invent a deployment endpoint or install a container runtime to work around
either gap. Consequently **§20 issue 14 does not close in this stage's current run**
and stays open pending both operator inputs; the same gap means §17.3's writer-lease
contention question ("in practice or only theoretically") is equally unanswered here,
since it turns on the same deployment container-scheduling topology issue 14 asks
about. The dev endpoint above must not stand in for either answer — §1.4 explicitly
anticipates dev and deployment differing, so a dev-only result cannot be read as if it
described the deployment.

**§1.4 note honored:** only the dev endpoint was probed this session, so no
dev-vs-deployment capability *divergence* was observed — that comparison is exactly
what remains blocked. What this run does confirm is that the matrix is
endpoint-agnostic by construction (same checks, same script, driven only by
`AWS_ENDPOINT_URL`/credentials): once the two missing operator inputs are supplied,
Steps 4 and 5 can run this same script unmodified and slot their output directly into
the two blocked columns above. Nothing in later stages may rely on a capability only
the dev endpoint is confirmed to have; `doctor` (Stage 7) reports this same matrix live
against whichever endpoint is actually configured.

**Credential note:** the probing credential is MinIO's root credential, and it
**bypassed** the delete-deny bucket policy — `delete_deny_policy` reported "NOT
ENFORCED for this credential (admin/root bypasses bucket policy?)". Stage 2 must
provision a distinct non-admin runtime credential before first capture. This is a
dev-endpoint observation only; whether the deployment endpoint's root credential
behaves the same way is untested and remains open alongside the rest of the deployment
columns.

**Probe-bucket state (§7.3, §17.4 — bears on section 7's bucket-creation
parameters).** Two claims, kept separate because their evidence differs:

- **No *real* bucket exists anywhere — established, on-disk evidence.**
  `probes/objstore_capabilities.py`'s module docstring states it "Creates ONLY
  throwaway `bls-stats-probe-*` buckets and deletes them. NEVER creates the real
  buckets: §7.3 — those are created in Stage 2 from the findings' parameter sheet."
  The two buckets this run created are named `bls-stats-probe-plain-{today}` and
  `bls-stats-probe-lock-{today}` (script lines 74-75); neither `bls-stats-raw` nor
  `bls-stats-main` (section 7's real bucket names) appears anywhere in this run's
  twelve `checks`. Section 7 itself is framed as parameters written *before* either
  real bucket exists. This is the claim §7.3 cares about, and it holds.
- **No leftover `bls-stats-probe-*` bucket remains — not verifiable from what this
  stage captured to disk; recorded as a gap, not asserted.** The script's cleanup
  step (lines 220-235) deletes all object versions in each probe bucket, then the
  bucket itself, printing `cleaned up {bucket}` (or a `cleanup {bucket}: <error>` line
  on failure) to stdout — but `finish()` writes only the twelve `checks` to the JSON
  results file; the cleanup step's outcome is never captured there. The one on-disk
  data point that bears on bucket count is the JSON's first check,
  `reachability.list_buckets`: "2 buckets visible" — but that check runs *before*
  `create_bucket.plain` (the second check), so it establishes only a pre-run count of
  2 pre-existing, unrelated buckets, not their names and not the post-cleanup state.
  What would close this gap: capture the cleanup step's per-bucket outcome into the
  JSON result on a future run (a small addition to the script, out of scope this stage
  per the pinned-script constraint), or a dedicated post-run `list_buckets` call whose
  output is saved to `probes/results/`.

## 6. Replication options — §20 issue 15, mechanism (Task 7)

What settles it: "the deployment's replication decision" — options recorded here,
`doctor`-confirmed at Stage 7.

**Source:** analysis, not a new probe — options A and C are read off Task 6's probe,
`probes/objstore_capabilities.py`, run 2026-08-10
(`probes/results/objstore-workstation-dev-127.0.0.1-2026-08-10.json`, section 5
above); option B needs no probe (buildable by design — it is the §16.1 adapter's
existing `list`/`get`/`put` surface).

Requirement (§17.4, R7): an independent second copy of `raw/` + `log/fetch/`; any single-copy
period longer than one release cycle is a standing finding (N12, reported by `doctor` from
Stage 7). The mechanism is the deployment's choice — these are the options, recorded with a
recommendation:

| Option | Mechanism | Independence | Available here? | Notes |
|---|---|---|---|---|
| A. Provider-side replication | endpoint's bucket-replication API | independent media, same provider failure domain | **dev endpoint only** — `replication_api` check: "API present, none configured (`ReplicationConfigurationNotFoundError`)". **Deployment endpoint: unprobed** (§20 issue 14, blocked on missing deployment-endpoint credentials; Task 6's matrix ran the `workstation-dev` context only) — the dev result does not confirm or rule out A for the deployment endpoint (§1.4 anticipates the two differing) | lowest effort if present, but confirmation is still open |
| B. Second endpoint + pull job | scheduled container job: paginated LIST diff + streaming copy to a second, different-provider endpoint | full provider independence | always buildable — it is the §16.1 adapter's `list`/`get`/`put` | strongest; job lands alongside Stage 2's capture loop or later |
| C. Provider backup/snapshot feature | vendor-specific | varies | not probed — Task 6's capability matrix (`probes/objstore_capabilities.py`) has no vendor snapshot/backup check; unevaluated at either endpoint this stage | record only if A and B both unavailable |

**Recommendation: conditional, not a single letter — the matrix that would decide between A and
B is partial.** §20 issue 14 is open: Task 6 probed the `workstation-dev` context only; the
deployment endpoint's `replication_api` result is unknown, and the dev endpoint's own result
(API present, none configured) does not settle the question for deployment either way (§1.4).
Decision rule for the operator (or `doctor`) to execute once the deployment matrix exists
(finding 5's blocked `workstation-deploy`/`container` columns, or a live Stage 7 report):

- **If** the deployment endpoint's `replication_api` check confirms a working, configurable
  replication API **and** the resulting failure domain (independent media, same provider) is
  judged acceptable → **choose A.**
- **Otherwise** (API absent, unconfirmable, or the same-provider failure domain judged
  insufficient against P1's "irreplaceable archive") → **choose B**, which needs no
  endpoint-specific capability beyond the §16.1 adapter's `list`/`get`/`put` and is therefore
  buildable and confirmable today regardless of what the deployment matrix eventually shows.

**Default until that evidence exists: B.** It is the only option confirmed buildable this run;
A is neither ruled in nor ruled out, only unconfirmed — treating dev-endpoint presence of the
replication API as a deployment guarantee would be exactly the promotion §1.4 warns against.
**Timing:** N12 starts accruing from first capture — the second copy should exist by the end of
Stage 2, or the single-copy period is a known standing finding from day one, by choice.

**Decision required (operator):** confirm the mechanism (and, for B, the second provider).
Recorded as pending until the plan-completion gate; `doctor` (Stage 7) verifies whichever lands.

## 7. Archive-bucket creation parameters — §7.3, §17.4 (Task 7)

Written **before any bucket exists**: object-lock *enablement* (`ObjectLockEnabledForBucket=true`)
is settable only at bucket creation (§7.3) — a bucket created without it can never have object
lock retrofitted. Default retention itself is a separate call issued after creation (see the
Object lock row and the required post-creation verification gate below); this sheet still has to
be right before Stage 2 creates anything, because enablement is the one-shot decision.

Executed by Stage 2 **before the first capture**. §7.3: "the archive bucket must be created with
immutability configured before the first byte lands … the one storage decision that cannot be
corrected later."

Two buckets (§17.4: whole-bucket policy statements; compaction rewrites kept clear of immutable
objects):

| Parameter | raw bucket `bls-stats-raw` | main bucket `bls-stats-main` |
|---|---|---|
| Contents | `raw/blob/` (content-addressed blobs, never expire) **and** `attest/<release_event_id>` (timestamp proofs, §9.1/R5 — evidence-class, not a rebuild input) — both per `specs/bls-stats-spec.md` §9.2 (lines 1213-1215); not `raw/` alone. Plus, one-off and not `<release_event_id>`-shaped: the `attest/_verify/lock-inherit-check` gate artifact this sheet itself writes (see "Post-creation verification" below) — recorded here so that key isn't mistaken for an undeclared prefix the way the original `raw/_verify/...` placement was. | `log/`, `ledger/`, `store/`, `ops/` |
| Created | Stage 2 — only `ObjectLockEnabledForBucket` (Object lock row) is part of the `CreateBucket` call itself; versioning, default retention, the delete-deny policy, and lifecycle are each separate calls issued immediately afterward, in this sheet's row order (see the Object lock row and the required post-creation verification gate below) | Stage 2 |
| Versioning | Enabled — **dev endpoint (2026-08-10 probe):** `versioning` check: "status=Enabled, versions_after_two_puts=2". Deployment endpoint: unprobed (§20 issue 14) — re-verify before Stage 2 executes this sheet; a §17.4 hard requirement must not rest on dev-endpoint evidence alone at execution time. **Unlike the Object lock row, there is no fallback here: if re-verification shows the deployment endpoint doesn't support versioning, that is a stop, not a fallback** — conditional-PUT dedup, per-version `PutObjectRetention`, and compaction all assume versioning is on, and this sheet does not proceed to bucket creation until it is confirmed. | Enabled (same caveat) |
| Object lock | Two separate calls, not one: (1) `ObjectLockEnabledForBucket=true`, part of `CreateBucket` itself — creation-only, cannot be added to an existing bucket; (2) immediately afterward, a distinct `PutObjectLockConfiguration` call setting default retention **GOVERNANCE, 3650 days** (a mode *and* a duration — §17.4). **Required gate before first capture:** see "Post-creation verification" immediately below this table — this row is not satisfied until that gate passes. | Off — §17.4 generation-swap GC must delete `gen=<n>/` |
| Endpoint supports lock? | **dev endpoint (2026-08-10 probe): yes**, on every check the probe script runs — `object_lock.create_enabled`: "Enabled"; `object_lock.default_retention_with_duration`: "default retention GOVERNANCE/3650d accepted (a *duration* — §17.4)"; `object_lock.put_retention` (per-object GOVERNANCE retention, the mechanism delete-denial depends on — no row in finding 5's table, JSON-only): "GOVERNANCE +90s on version d73a3374-e800-4092-a820-2c57bc31269a"; `object_lock.delete_denied`: "enforced (`InvalidRequest`)". **Caveat:** the probe labels *any* `ClientError` on the delete attempt as "enforced," and the code actually observed, `InvalidRequest`, is not an access-denial-specific code — so "locked-version delete denied" rests on weaker evidence than the label suggests. **None of these checks cover inheritance** — a new object automatically acquiring the bucket's *default* retention, rather than having retention set explicitly per object. `object_lock.put_retention`/`object_lock.delete_denied` (`probes/objstore_capabilities.py`, functions `lock_retention` then `lock_delete_denied`) set retention *explicitly* on that object, before `object_lock.default_retention_with_duration` (`lock_default_retention`) ever configures the bucket default, and no object is written after the default is configured. That link is untested by this run; it is exactly what the required post-creation verification gate below closes. **This is dev-endpoint evidence only. Deployment endpoint: unprobed** (§20 issue 14, blocked on missing deployment credentials) — **re-verify against the deployment endpoint before Stage 2 executes this sheet**; a dev-endpoint capability must not be promoted to a deployment guarantee (§1.4). Re-verifying the checks above at the deployment endpoint does not by itself close the inheritance gap either — the post-creation gate below must still run there. If the deployment endpoint turns out not to support object lock, §17.4's fallback becomes the *actual* parameter here: delete-deny alone — in that branch, delete-deny is the *sole* protection layer against the runtime credential, so it must be positively verified for the actual non-admin runtime credential (§17.4) before relying on it; the only evidence on record today is NOT ENFORCED, for the root credential only (see the Delete policy row below) — with the gap recorded as a standing finding at Stage 7. | — |
| Delete policy | Deny `s3:DeleteObject` + `s3:DeleteObjectVersion` to `"AWS": "*"` on `arn:aws:s3:::bls-stats-raw/*` (JSON below); enforced for the runtime credential — **dev endpoint (2026-08-10 probe):** `delete_deny_policy` check: "NOT ENFORCED for this credential (admin/root bypasses bucket policy?) — Stage 2 needs a distinct runtime credential." The credential probed was MinIO's root/admin credential, and it bypassed its own bucket policy. **Stage 2 must provision a distinct non-admin runtime credential before first capture and re-run this check against that credential** — the root-credential result neither confirms nor denies enforcement for any other principal. Deployment endpoint: unprobed (§20 issue 14) — re-verify before Stage 2 executes this sheet. **`specs/bls-stats-spec.md` §17.4 (line 2317) marks this a hard requirement: "Delete denied to the runtime credential on the archive bucket … Do not run without it." Unlike the Object lock row's fallback (delete-deny alone, if object lock is unavailable), there is no fallback for delete-deny itself: if re-verification against the non-admin runtime credential does not show it enforced, that is a stop, not a fallback** — the only evidence on record today (NOT ENFORCED) is for the root credential at the dev endpoint, which is not the credential or the requirement this hard requirement is about. | runtime credential MAY delete (generation GC) |
| Lifecycle | **None. `raw/` never expires; no tiering** (§17.4, §1.4) | none initially |
| Region / endpoint | as configured (`AWS_ENDPOINT_URL` — §1.4: never a constant) | same |

**Post-creation verification (required gate — first capture must not proceed if this fails).**
The object-lock checks in the table above establish that the endpoint accepts lock enablement and
an explicit per-object retention, and enforces delete-denial on a version retained that way — they
do not establish that a new object dropped into the bucket *without* an explicit
`PutObjectRetention` call actually inherits the bucket's default retention. That inheritance link
is the one the capture path depends on: Stage 2's capture writer is nowhere specified to call
`PutObjectRetention` per object, so immutability rests entirely on inheritance. No probe run in
this stage tested it — `objstore_capabilities.py`'s `lock_retention`/`lock_delete_denied` checks
(`probes/objstore_capabilities.py`) set retention *explicitly*, before the bucket default even
existed, and `lock_default_retention` never writes an object afterward to confirm what a plain PUT
would inherit. Re-running that same probe against the deployment endpoint does not close this gap
either — the script still never writes an object after configuring the default.

Before Stage 2's capture loop writes its first real object, run this sequence against
`bls-stats-raw` and treat a failure as a stop, not a warning:

1. After creating the bucket with `ObjectLockEnabledForBucket=true` and issuing the
   `PutObjectLockConfiguration` call for default retention GOVERNANCE/3650d, call
   `GetObjectLockConfiguration` and assert it returns `ObjectLockEnabled=Enabled` with
   `Rule.DefaultRetention = {Mode: GOVERNANCE, Days: 3650}`.
2. `PutObject` one throwaway object at `attest/_verify/lock-inherit-check` — **not** under
   `raw/blob/`. Default retention is a bucket-level setting (`PutObjectLockConfiguration`
   is not prefix-scoped), so this still exercises the inheritance path the gate needs to
   test; the choice of prefix only affects where the object lands, not whether it inherits.
   `raw/blob/` is specifically the content-addressed namespace (§9.2, line 1214); N11 (spec line
   2036) is defined as a sampled re-hash of "`raw/` blobs" against their content-addressed keys —
   worded at the `raw/` level, not narrowed to `raw/blob/` in that line — so a verification marker
   keyed by an arbitrary string rather than the SHA-256 of its bytes risks exactly the same problem
   under either reading of N11's scope: re-hashing it would find a key that no re-hash can ever
   match, a standing false fixity concern for the life of the object. Two ways to avoid that were available: key the object by the SHA-256 of
   its own bytes and let it land at the real `raw/blob/sha256=<hh>/<hh>/<sha256>` path (making
   it fixity-clean by construction), or place it outside `raw/blob/`'s namespace entirely. This
   sheet takes the second path — `attest/` (added to the Contents row above) already exists in
   §9.2 as a sibling, evidence-class prefix untouched by the fixity sweep, and a marker proving
   the bucket's retention setup works is exactly that kind of evidence, not a rebuild input,
   matching §9.2's own description of what `attest/` holds. The SHA-256-keyed alternative was
   rejected here because it would make an internal test fixture indistinguishable from a
   genuine archived BLS blob under `raw/blob/` — conflating an operational check with P2's
   content-addressed dedup mechanism for actual retrieved artifacts, which is a stronger
   property than this marker needs. The key `attest/_verify/lock-inherit-check` is not
   `<release_event_id>`-shaped and must not be mistaken for one — **with no `Retention`
   argument on the PUT** — so it can only pick up retention by inheritance, not by an explicit
   call.
3. Call `GetObjectRetention` on that object's version and assert `Mode=GOVERNANCE` and that
   `RetainUntilDate` is ~3650 days out (use a tolerance window, e.g. ±1 day, rather than an exact
   timestamp — the server computes the date, this sheet does not).
4. If either assertion fails, **first capture does not proceed** — inheritance is not confirmed at
   this endpoint, and the parameter sheet's central claim (new objects are protected automatically)
   does not hold.

Note for whoever runs this: step 2's object is written under real GOVERNANCE/3650d retention in
the real raw bucket — once written, it is immutable for the full duration absent an admin
governance-bypass delete. Key it as an explicit verification artifact, not operational data, since
it cannot simply be deleted afterward.

**If the gate fails — remediation, not just a stop.** These remediation steps are reasoned from the bucket's stated constraints but were not exercised by any probe in this stage — they are derived, not demonstrated. Stage 2 should treat them as best-available guidance and verify recovery as it proceeds. Step 4 says first capture does not proceed;
it does not say what to do with the bucket that failure leaves behind. `ObjectLockEnabledForBucket`
is already `true` on it and that cannot be undone (§7.3), and — per the Versioning row above,
already `Enabled` by the time this gate runs (object-lock enablement forces versioning on regardless)
— a plain, unversioned delete of the throwaway object creates a **delete marker rather than removing
a version**, exactly as the Stage-2 note below (under the delete-deny policy JSON) already documents
for this same bucket. `DeleteBucket` fails with `BucketNotEmpty` while any version or delete marker
remains, so "delete it, then `DeleteBucket`" is not sufficient in any branch below; every branch
must list and remove **every version and every delete marker** of the throwaway object (the same
pattern `probes/objstore_capabilities.py`'s own cleanup step uses on its throwaway probe buckets —
section 5 above) before the bucket is actually empty. A second constraint stacks on top of the
first: per the Created row above, the delete-deny policy is applied "in this sheet's row order" —
before Lifecycle but after Object lock and its retention call — so by the time this gate runs (a
check gating *first capture*, which comes after all of Created's setup calls), the bucket policy
denying `s3:DeleteObject`/`s3:DeleteObjectVersion` to `"AWS": ["*"]` is already attached and denies
those same version-delete and delete-marker calls to *every* principal, including whoever is running
this remediation. **Every branch below therefore needs the same first step this section's
break-glass paragraph already describes: remove or suspend the bucket policy before attempting any
delete, then reattach it (or apply it fresh to the replacement bucket) once teardown and recreation
are done** — this applies regardless of whether an object ended up retained, since the policy blocks
plain version deletes just as much as governance-bypassed ones. The operator is left holding a
bucket that may also contain a retained object, and recovery differs by exactly which assertion
failed:

- **Step 1's assertion fails** (`GetObjectLockConfiguration` doesn't return
  `GOVERNANCE`/`3650`) — check whether step 2 still ran. If the throwaway object's
  `GetObjectRetention` (step 3) shows no retention applied to any of its versions, inheritance
  genuinely did not happen and every version is plain and deletable: list all versions and delete
  markers of the object, delete each by `VersionId`, then `DeleteBucket` on the now-empty bucket.
  Object-lock enablement blocks deleting *retained* object versions, not an empty bucket, so once
  every version and delete marker is gone this is a clean recovery — the bucket can be recreated
  under the **same** name once the `PutObjectLockConfiguration` call is corrected and the gate is
  re-run.
- **Step 3's assertion fails on the duration specifically** (`Mode=GOVERNANCE` is present and
  correct, but `RetainUntilDate` is not ~3650 days out — e.g. the default was configured with the
  wrong day count) — the retained version genuinely *cannot* be deleted normally. Because the mode
  is GOVERNANCE (not COMPLIANCE), the admin/root credential's governance-bypass delete — already
  named above as "the deliberate break-glass" — can remove that specific version (see the
  break-glass note under the delete-deny policy below for what else that bypass requires); any
  *other* versions or delete markers on the same key (e.g. from a prior failed attempt) still need
  ordinary versioned deletes on top of that. Once every version and delete marker is gone,
  `DeleteBucket` the emptied bucket and recreate it under the same name with the corrected
  default-retention duration.
- **Step 3's assertion fails and the mode itself came back wrong** (e.g. `COMPLIANCE` where
  GOVERNANCE was intended, or any other non-bypassable state) — check `RetainUntilDate` before
  concluding anything about how long this lasts: this branch is defined by the *mode*, not the
  *duration*, and a COMPLIANCE lock with a short (if still wrong) duration means waiting out days,
  not treating the bucket as permanently lost. If `RetainUntilDate` is far out, the retained version
  cannot be removed by any credential, admin included, until it elapses, so the bucket cannot be
  emptied or deleted in any practical timeframe: treat it as unusable for capture — do not write real
  data into it, and do not keep retrying bucket creation under its name. A replacement bucket must be
  created under a **different** name — and per Decision 2 below, a name change is not a free edit: it
  propagates into Stage 2's config and reopens that decision for operator sign-off before this sheet
  is executed again. If `RetainUntilDate` is close enough to wait out, that is the cheaper path and
  does not require a name change.

In every branch, the gate is re-run against the (possibly renamed) bucket from step 1, and first
capture waits for a clean pass — a failed run does not get silently retried under looser criteria.

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

The policy JSON above is plan-verbatim and is left exactly as pinned; the two gaps below are
recorded in prose, as pending operator decisions, not applied as edits to it.

**Break-glass path is not free with this `Principal`.** The policy denies to `"AWS": ["*"]` —
every principal, with no carve-out. An endpoint that honors the policy uniformly therefore also
denies the admin/root credential's own governance-bypass delete — the same break-glass this
section already relies on: the gate failure-branch above (its wrong-duration case) and the "Why
GOVERNANCE and not COMPLIANCE" paragraph below both assume an admin can bypass-delete a
GOVERNANCE-retained object. As written, that bypass is not available while this bucket policy is
attached: break-glass requires
first removing (or suspending) the bucket policy itself, then performing the governance-bypass
delete, then — if the policy is meant to keep protecting the bucket afterward — reattaching it.
This is a real operational step, not a detail; whoever holds break-glass authority needs
`s3:PutBucketPolicy`/`s3:DeleteBucketPolicy` on hand to use it against this policy shape.

**The policy's action list does not cover every path to deletion.** It denies only
`s3:DeleteObject` and `s3:DeleteObjectVersion`. It does **not** deny `s3:PutBucketLifecycleConfiguration`
(an expiration rule is itself a path to deletion, distinct from a direct delete call) or
`s3:PutBucketPolicy`/`s3:DeleteBucketPolicy` (a credential that can rewrite or remove this policy
can self-authorize the deletes it currently blocks). This matters most precisely in the branch this
section already names as delete-deny-alone — the fallback if the deployment endpoint turns out not
to support object lock (see the Object lock row above): in that branch this policy is the *sole*
protection layer against the runtime credential, so gaps in its action coverage are the actual
enforcement surface, not a secondary concern. This sheet mandates provisioning a distinct
non-admin runtime credential (Delete policy row above) but never specifies what IAM/policy actions
that credential itself may or may not perform — whether it can call `PutBucketLifecycleConfiguration`,
`PutBucketPolicy`, or `DeleteBucketPolicy` is undetermined by anything on record. Recommendation,
left as a decision rather than an edit to the JSON above: the runtime credential's own permission
grant should explicitly exclude those three actions on the raw bucket, in addition to whatever this
bucket policy denies — this is a gap in what the *credential* may do, not in what the *policy*
denies, and closing it does not require touching the pinned JSON. (This is separate from the
already-recorded "two independent layers" qualification below, which is about the *evidence* on
record for the delete-deny layer; this is about the *scope* of what that layer, and the credential
behind it, actually covers.)

**Stage-2 note (delete-deny re-run scope — not a fix here; `probes/objstore_capabilities.py` is
pinned for this stage).** The `delete_deny_policy` check's delete attempt
(`probes/objstore_capabilities.py`, function `delete_deny`) calls `delete_object` on the `plain`
bucket without a `VersionId`, and that same bucket had versioning enabled earlier in the same run
(the `versioning` check runs against `plain` too). An unversioned delete call against a versioned
bucket creates a delete marker rather than removing a version — it exercises `s3:DeleteObject`
only, never `s3:DeleteObjectVersion`. The result recorded (NOT ENFORCED) is negative, so this gap
errs safe: it under-tested rather than over-reported enforcement. But it means the deny policy's
coverage of an actual version-delete attempt is untested. When Stage 2 re-runs this check against
the provisioned non-admin runtime credential (Delete policy row above), the re-run should cover
**both** actions — a delete-marker attempt (no `VersionId`) and a versioned-delete attempt
(`VersionId` set to a real version) — before treating the deny policy as confirmed for that
credential.

**Why GOVERNANCE and not COMPLIANCE (recommendation):** COMPLIANCE retention cannot be shortened
by anyone, including the account root — a mis-set duration is permanently unfixable, and the 2025
lesson cuts both ways (irreversibility is the point *and* the risk). GOVERNANCE + the delete-deny
policy gives two independent layers against the runtime credential **once the non-admin runtime
credential is verified against the deny policy — unverified as of 2026-08-10.** The only evidence
on record this stage is the `delete_deny_policy` check: NOT ENFORCED, and that result is for the
root credential, the only credential and only endpoint tested (see the Delete policy row above and
the Stage-2 note above it) — no positive enforcement evidence for the deny-policy layer exists yet
for any credential. Admin governance-bypass remains the deliberate break-glass. P1's protection
target is the *runtime* path — no operational code path may be able to delete a vintage.

**Decisions (operator):**
1. Lock mode + duration: GOVERNANCE/3650d recommended above — **confirmed by the
   operator, 2026-08-11.**
2. Final bucket names: `bls-stats-raw` / `bls-stats-main` are the defaults — **confirmed
   by the operator, 2026-08-11** (names propagate into Stage 2's config, nowhere else
   yet).

Both confirmed 2026-08-11; the reasoning recorded above for each stands as the record of
why. Stage 2 executes this sheet using these parameters. **Section 6's replication
mechanism decision is separate and remains pending** — it is conditional on the
deployment endpoint's replication API (§20 issue 14, unprobed), and is not resolved by
this sign-off.

## 8. Consequences for later stages (Task 8)

- **Stage 2 (capture):** Transport posture per host (sections 1-3):
  `download.bls.gov` and `data.bls.gov` are open to the compliant contact profile for
  data retrieval — four HEAD/GET checks returned 200 and one ranged GET returned 206
  across both hosts — build capture on that profile; no browser-shaped headers are
  needed there. `www.bls.gov`'s `html` transport: the browser-shaped profile (§7.2
  mitigation 2) was blocked 6/6 by Akamai; the compliant contact profile passed 6/6 —
  build `html`-profile ingest on the contact profile and leave the headless
  `HtmlFetcher` backend (§7.2 mitigation 3) **unbuilt-and-pluggable, out of scope**,
  pending the recommended single-session interleaved-profile follow-up probe that
  would isolate mechanism (section 3, confounds 1-2 remain open). Range is honored
  (206) on `download.bls.gov`, but the plain-HEAD `Content-Length` (350,208,884) and
  the ranged GET's `Content-Range` total (47,300,620) diverge on an identical ETag —
  unresolved. Default to **omitting `Accept-Encoding: gzip` on range-based bulk
  transfers**, so ranges run against the uncompressed size HEAD reports, rather than
  computing offsets against a compressed-stream size whose stability across requests
  was never confirmed. `Accept-Encoding: gzip` did not change `content-type` away
  from `application/octet-stream` on `ce.period` — don't key transfer-encoding
  decisions on `content-type`. Bucket creation executes section 7 verbatim, including
  its required post-creation retention-inheritance gate (section 7, steps 1-4: create
  with `ObjectLockEnabledForBucket=true` → set default retention →
  `PutObject` an unretentioned probe object → `GetObjectRetention` and assert it
  inherited GOVERNANCE/3650d — treat failure as a stop, not a warning) and its
  non-admin runtime-credential provisioning (the root credential bypassed the
  delete-deny policy at dev; re-run that check against the actual runtime credential,
  covering both a delete-marker attempt and a versioned-delete attempt).
  Deployment-endpoint versioning is a stop-not-fallback if re-verification shows it
  absent. Replication decision: **conditional, not yet resolved** — default to
  option B (second endpoint + pull job) until the deployment endpoint's
  `replication_api` result is known; §20 issue 14 (deployment endpoint
  identification, container reachability) is the blocking prerequisite for ever
  resolving to option A, and remains open past this stage (see
  `specs/bls-stats-spec-roadmap.md`, Stage 1 entry). R12 compliance: the
  `www.bls.gov` robots clearance (section 3 addendum) covers only the six probed
  paths under the generic `User-agent: *` rule — check any additional
  `html`-profile path (the addendum names three unprobed spec-referenced surfaces:
  the archived news-release index §11.3, per-release archive links §11.3, per-program
  notices pages §12.5) against a re-fetched policy before relying on it (§18.3
  point-in-time caveat). No robots policy was retrieved for `download.bls.gov`
  (404 at that path, section 1) or `data.bls.gov` (unprobed for `robots.txt`).

- **Stage 3 (store mechanism):** Conditional PUT (`If-None-Match`) is **present and
  enforced** at the dev endpoint (`PreconditionFailed`) — not absent-as-assumed, not
  present-but-unused. It is unprobed at the deployment endpoint (§20 issue 14 blocks
  it). §1.4's single-writer discipline (the lease) stands regardless of what the
  deployment endpoint's conditional-PUT support turns out to be: the design targets
  the *intersection* of what both endpoints guarantee, not the dev endpoint's best
  case, so Stage 3 must not build the lease as an optional optimization layered on
  top of an assumed-present conditional PUT — the lease is the correctness mechanism
  until the deployment endpoint's own conditional-PUT support is confirmed.

- **Stage 4 (control plane):** the `.ics` schedule feed and the release feed both
  return genuine content under the contact profile (section 3 addendum). The release
  feed is an **Atom** document served with `content-type: application/rss+xml` at a
  `.rss` path (`/feed/empsit.rss`) — parse defensively for either root element; do not
  key ingestion on the path or the content-type header, since this run shows they
  disagree.

- **Stage 5 (data plane):** QCEW envelope arithmetic (section 4): the 2025 singlefile
  zip's one member, `2025.q1-q4.singlefile.csv`, is a **whole-year** artifact (2.2 GB
  decompressed, 14.6M lines) at the one URL pattern this stage probed
  (`{year}_qtrly_singlefile.zip`), not the **per-quarter** artifact §8.3 and R13's settled
  `authoritative_scope` assume. Section 4 scopes this precisely: other QCEW artifact
  families named in §20 issue 4 (the LABSTAT `en` prefix, the by-size ZIP) were not
  surveyed, so this is a finding about the probed pattern, not a closed claim that no
  per-quarter QCEW artifact exists anywhere — the stream-not-materialize conclusion below
  holds regardless. The contradiction's direction is **under-deletion**,
  not §8.3's stated over-deletion hazard: quarters present in the year artifact but
  outside a per-quarter `authoritative_scope` would silently fail to emit genuine
  deletions — a quieter failure than the over-deletion §8.3 was written to prevent.
  Stage 5 must reconcile which frame governs `authoritative_scope` before hardening
  the differ; this is flagged here, not resolved (out of this stage's scope).
  Memory implication: two whole-year files held in memory at once (the frame this
  measurement actually supports — e.g. comparing vintages of the same year) is
  ~4.10 GiB, only ~1.8-1.95x under the 8 GB peak-RSS budget, and arithmetic shows even
  a modest, illustrative 2x parse-overhead multiplier busts both conventions of that
  budget at the year frame. Stage 5's "a QCEW-scale run stays inside the RSS budget"
  exit criterion is therefore a live risk, not a formality: the differ must stream
  rather than materialize whole years (or downstream quarters), and Stage 5 must
  measure actual parsed-in-memory RSS directly (R18's memory-envelope gate) rather
  than extrapolate from this probe's ~75 MiB figure, which describes only the
  never-materializing streaming-measurement process, not a parsed diff of any size.

- **Stage 7 (ops):** `doctor` must report the section-5 capability matrix live
  against whichever endpoint is actually configured — a dev-only result must never
  stand in for the deployment endpoint (§1.4) — including the two columns still
  blocked here (`workstation-deploy`, `container`) until §20 issue 14 closes. N11
  (fixity) and N12 (replication single-copy-period) baselines start accruing from
  Stage 2's first capture, not from this stage's probe date: N11 re-hashes `raw/`
  blobs and N12 tracks `raw/` + `log/fetch/`'s single-copy period, and neither
  directory exists yet. §7.1's "HTTP 200 is necessary but not sufficient" rule ships
  to Stage 2 as standing policy, carried forward unverified-by-probe (no
  HTTP-vs-payload divergence was witnessed this run, on either the v1 or v2 request);
  Stage 7's daily report/`doctor` should be positioned to catch that trap the first
  time it actually fires in production. The API key's annual-expiry alert (§7.1) is a
  Stage-2 setup item — the key itself is already provisioned and working as of this
  stage's probe.

- **Roadmap re-validation:** this changes three stages' assumptions, not none.
  **Stage 2** — its build now targets the contact profile for `html` ingest with
  headless out of scope pending a follow-up probe, and §20 issue 14's two
  operator-blocked items (deployment-endpoint credentials; a deployment-side
  container shell/runtime) become Stage-2 prerequisites, not leftover Stage-1 work
  (see `specs/bls-stats-spec-roadmap.md`, Stage 1 entry, for the exact open item).
  **Stage 3** — conditional PUT is confirmed present and enforced at the dev
  endpoint, sharpening rather than changing its Exit criteria, with
  deployment-endpoint confirmation still pending. **Stage 5** — §8.3's per-quarter
  `authoritative_scope` premise is contradicted by measurement (the artifact is
  whole-year; the failure direction is under-deletion, not over-deletion), and its
  RSS-budget Exit criterion is now a live risk at the year-frame working set rather
  than a formality; both need resolution before Stage 5's plan is written.
  **Spec text needing amendment, not just stage plans:** this bullet has enumerated
  stages so far, but `specs/bls-stats-spec.md` itself still states the pre-probe
  posture in two places section 3 bears directly on. §7.1's `html` row (line 721:
  "Known to 403 ordinary fetchers. Full browser-shaped headers, HTTP/2, low rate.")
  describes exactly the profile this stage found blocked 6/6, with no mention of the
  contact profile that passed 6/6 on the same surfaces. §7.2's mitigation ordering
  (lines 726-745) ranks browser-shaped `httpx` (mitigation 2) ahead of a headless
  backend (mitigation 3) as the fallback, with no lighter-weight contact-UA rung
  between them. An implementer who builds Stage 2's `html` transport from spec text
  alone — without also reading this findings document — would still build the
  blocked browser-shaped profile the spec currently documents as the mitigation to
  reach for. Both passages need updating to reflect section 3's result before Stage 2
  is planned, not only Stage 2's own roadmap entry. No other stage's Objective or
  Exit text changes; Stage 4 inherits the feed-flavor finding above as an
  implementation constraint, not a criterion change.
