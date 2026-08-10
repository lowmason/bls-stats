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

Canary (compliant contact UA, one request): 200 — **updates** the review-§0.3 baseline.
Review §0.3 (cited at spec §7.2: "`www.bls.gov` returns 403 to plain `httpx` even with a
contact User-Agent") predicts 403 for exactly this shape of request. JSONL line 1 shows
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
is: a single dated data point that contradicts the review-§0.3 baseline at this one URL,
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

All six browser-shaped requests returned byte-identical 1323-byte bodies with
`server: AkamaiGHost` — a single, uniform Akamai bot-management block, not per-surface
variation. Three of the six (errata table, news-release index, schedule index) show
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
was built on review §0.3's premise that plain/contact-UA `httpx` also 403s
`www.bls.gov` — this run's canary contradicts that premise at one path, which weakens the
case for jumping straight to the *last* rung of the ladder without first testing a much
cheaper one.

**Recommended before Stage 2 commits to building the headless worker:** a single,
separate follow-up probe — one contact-UA GET per remaining surface (5 requests, same
2 s spacing, same one-shot discipline as this task) — to see whether the compliant
contact UA that passed `/errata/` today also passes robots.txt, the `.ics` feed, the Atom
feed, the news-release index, and the schedule index. If it does, §7.2's ladder may stop
at "use the contact-UA profile for `html` surfaces too" rather than at headless — a
substantially cheaper Stage 2 outcome than standing up and operating a browser-automation
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
