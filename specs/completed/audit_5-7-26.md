# bls-stats codebase audit — 2026-07-05

> **STATUS: REMEDIATED** (2026-07-06) — all 25 confirmed findings (C-1…C-25) fixed test-first on branch `impl-plan-2` (PR #1); see `specs/plans/completed/2-audit_5-7-26.md`. Retained as the historical source of record for a
> whole-codebase adversarial review. Confirmed findings are written as actionable requirements
> (location, failure scenario, remediation, acceptance criteria) so this can feed a plan through
> the normal specs → plans → implementation flow. All 25 items are now remediated (see the plan).
>
> **Verification complete (2026-07-05).** The adversarial pass the spend limit originally cut short
> was re-run to completion. Of **40 raw findings**: **31 confirmed** (consolidated to 25 requirement
> items C-1…C-25 — the one contested item was resolved in scope on 2026-07-05 as C-25), **9 refuted
> or downgraded below threshold**. No item is left unadjudicated.

**Companion documents:** architecture spec `specs/bls-stats-architecture.md` (cited as "ARCH §N"),
behavioral contract `bls-stats.md` (BEH), already-adjudicated deferrals `specs/deferred_items.md`
(not re-litigated here).

---

## 1. Method and scope

Nine read-only reviewers each swept one dimension of the tree (`src/`, `tests/`, `docs/`):
vintage semantics, storage/concurrency, engine parsing, releases/time, HTTP/security, test
quality, CLI/config contract, docs accuracy, and layering/dead-code. Each was given the
architecture spec and the accepted-deferrals list so settled items would not resurface. Every raw
finding then faced three adversarial verifiers with distinct lenses — **reachability** (is the
failure path real in the code as written), **spec-alignment** (is the "correct" behavior actually
what ARCH/BEH mandate), and **materiality** (does an existing backstop intercept it). A finding
needed **≥2 of 3 upheld votes** to survive.

### 1.1 Two-pass history (why the numbering has two series)

The first run's verification phase was cut off by the account spend limit: 17 findings cleared
with ≥2 votes (§2, IDs **C-1…C-12** after consolidating cross-dimension duplicates), 1 was
genuinely refuted, and 22 were starved of a verdict when their verifiers died. A second, verified
run (2026-07-05, with budget restored) adjudicated all 23 remaining findings (the 22 starved + the
1 refute, re-checked). That second pass produced:

- **13 newly confirmed** (≥2/3 votes) → promoted into §2b, consolidated into **C-13…C-24** (the
  feed-date-parse finding, V17, folds into C-2), including both criticals.
- **6 refuted at their proposed severity but with a real, reproduced mechanism** → §3, kept as
  downgraded/sub-threshold items (not silently dropped — several are worth a cheap hardening fix).
  One of these (V14) is the reconciliation of an earlier hand-verification, in §3.2.
- **1 contested** (a 1-uphold/1-refute split on a spec-interpretation question) → §3.1;
  **resolved in scope on 2026-07-05 as C-25**.
- **3 cleanly refuted** → §4.

Where the second pass disagreed with my own earlier hand-verification, I reconciled it in §3.2
rather than silently trusting either — the agents corrected one of my calls (V14). (13 + 6 + 1 + 3
= the 23 re-checked findings.)

### 1.2 Severity scale

- **Critical** — data corruption/loss, wrong vintage semantics, or a security defect.
- **Important** — incorrect behavior in a realistic scenario, or a broken documented contract.
- **Minor** — robustness/polish with a plausible trigger.

---

## 2. Confirmed findings (verified, ≥2/3 adversarial votes)

Ordered important-first, then minor. Several were flagged independently by more than one
dimension — noted where so, since independent rediscovery raises confidence.

### C-1 (Important) — `gaps` audits the wrong thing and ignores `--program`

**Where:** `src/bls_stats/cli.py:186-214` (the `gaps` command); doc mirror at `docs/cli.md:88`.
Flagged independently by the CLI-contract and docs-accuracy reviewers.

**What:** ARCH §5.3, the §8 CLI table, the command docstring, and `docs/cli.md` all define `gaps`
as *expected-release-vs-ledger*: a calendar entry with a past `release_date` and **no ledger row
of any status** is an unexplained gap and must exit non-zero. The implementation instead calls
`find_gaps(cal)` (`releases/calendar.py`), which returns reference periods **missing from the
calendar** (scrape-coverage holes), and anti-joins *those* against the ledger. A release that is
in the calendar but absent from the ledger is never examined. Separately, when `--program` is
given only `cal` is filtered; the `ledger`, the `acknowledged` count, and the `--strict` missed
check stay global.

**Failure scenario:** the ingest cron is down for five weeks; a CES release ages out of the
~12-entry feed window, so no ledger row is ever written; the calendar has full period coverage.
`bls-stats gaps` prints `unexplained: 0` and exits 0 — but the spec requires exit 1, and the
permanently-lost print is never surfaced. This is the exact failure `gaps` exists to catch. Also:
`gaps --program ces --strict` exits 1 because an unrelated JOLTS slot is `missed`.

**Remediation:** derive the expected set from the calendar —
`cal.filter(release_date.is_not_null() & (release_date <= today)).select("program","ref_date").unique()`
— and anti-join that against the resolved ledger (optionally union `find_gaps(cal)` to keep the
coverage-hole check as a separately-labelled signal). `today` must come from an injected/CLI date,
consistent with the clock-injection rule. Scope `ledger`, `acknowledged`, and the `--strict`
missed set to the selected program.

**Acceptance:** a test where the calendar has a past-dated release with zero ledger rows asserts
`gaps` exits 1 and lists it; a test with a `missed` row in program B asserts
`gaps --program A --strict` exits 0. Update `docs/cli.md:88` to match final behavior.

### C-2 (Important) — one malformed feed entry or non-XML feed body crashes the entire daily ingest

**Where:** `src/bls_stats/releases/feeds.py:125` (`ElementTree.fromstring`), `:134`
(`date(int(...))`), `:184-190` (`poll` try/except). Flagged independently by the HTTP-security and
layering reviewers.

**What:** `parse_feed`'s per-entry tolerance covers only the regex-no-match case. Two paths escape
it: (a) an archive href whose digits match `_LINK_DATE` but form an impossible date (e.g.
`empsit_02292023.htm`, Feb 29 in a non-leap year) raises `ValueError` at the `date(...)`
construction; (b) a feed URL returning a non-XML body with HTTP 200 (a maintenance/block page)
raises `ElementTree.ParseError`. `poll` wraps only the `get()` fetch in `try/except
httpx.HTTPError`; `parse_feed` runs outside it, and `run_ingest`'s `for release in poll_fn(...)`
header has no guard. Because `poll` builds the full release list before `run_ingest` processes
anything, one bad entry in **any** feed aborts **every** program's ingest with a raw traceback —
contradicting `parse_feed`'s documented skip-and-warn contract, `poll`'s documented failed-feed
tolerance, and the ARCH §7.4 exit-code contract. The defined-but-never-raised `FeedParseError`
(feeds.py:45) is exactly the seam this gap should use. (Offline repro confirmed both raises.) BLS
hand-edits these feeds — the module itself notes ids edited in place — so a typo'd filename is
realistic and persists for weeks; vintages missed while the cron is down are permanently
unobservable because flat files flip in place.

**Remediation:** wrap the `date(...)` construction in `parse_feed` in `try/except ValueError`,
logging and skipping the entry like the no-match case; in `poll`, move `parse_feed` inside the
per-feed try (or a nested per-feed guard) and catch `ElementTree.ParseError` alongside
`httpx.HTTPError` — optionally by having `parse_feed` translate `ParseError` into `FeedParseError`
so that class earns its keep. One bad feed must degrade to warn-and-skip.

**Acceptance:** offline `MockTransport` tests — a feed entry with an impossible embedded date, and
a feed serving a non-XML 200 body — each assert `poll` returns the other feeds' releases and logs
a warning, with no exception escaping. (This also closes the deferred "offline poll() test" item.)

### C-3 (Important) — multi-year OEWS backfill silently fetches only the first year and exits 0

**Where:** `src/bls_stats/pipeline.py:157` (`_fetch_event` OEWS branch).

**What:** the OEWS branch calls `fetch_oews(client, refs[0].year, ...)` — a single year — but
`run_backfill` passes the slot list for the whole range (annual grammar yields one `(year, 1)` per
year). Only the first year's workbook is downloaded; its `ref_date` is that year's May 12, so the
other years hit the empty-piece branch, which is a bare `continue` — no ledger record, no warning.
The run logs `backfilled 1 period(s)` and returns 0. The CLI's per-year loop covers only QCEW, so
nothing compensates. Re-running the same day advances one year per invocation; runs on different
days restart under a new `snapshot_date`, re-appending earlier years as fresh backfill vintages
while later years stay missing.

**Failure scenario:** `bls-stats backfill --program oews --start 2020 --end 2023` fetches only the
2020 workbook, silently skips 2021–2023, exits 0 with no deferred/missed rows. The operator
believes the range is seeded; `gaps` cannot flag it because backfill slots were never recorded.

**Remediation:** mirror the QCEW branch — loop `sorted({r.year for r in refs})`, call `fetch_oews`
per year, `pl.concat` the results — or extend the CLI per-year splitting to OEWS. Additionally,
`run_backfill` should log (or record) empty backfill pieces instead of the silent `continue`.

**Acceptance:** a test with a fake OEWS fetch over a 3-year range asserts all three years'
`ref_date`s are committed and recorded; a test asserts an empty backfill slice emits a warning.

### C-4 (Important) — `ingest --program <typo>` crashes with a raw KeyError and exit 1

**Where:** `src/bls_stats/cli.py:59` (ingest), also reachable via `store query` and `calendar
show`. Flagged independently by the CLI-contract, HTTP-security, and layering reviewers (three
dimensions).

**What:** `--program` is passed unvalidated into `run_ingest` → `feeds.poll` →
`REGISTRY[p].feed_url`, which raises `KeyError` for any name not in the registry (uppercase,
typo). The `PROGRAMS` constant (cli.py:26) exists but is never used to validate. Exit 1 is
documented to mean "partial failure", so a monitoring wrapper misclassifies a config typo as a
partial ingest, and the user gets a stack trace. `backfill` handles the same mistake cleanly
(PeriodError → message, exit 2), so the two commands have inconsistent contracts.

**Failure scenario:** crontab `bls-stats ingest --program cesd` → `KeyError('cesd')` traceback,
exit 1, indistinguishable from a real partial failure; every scheduled ingest is skipped until
noticed.

**Remediation:** validate `program in REGISTRY` (or `PROGRAMS`) at the top of every command that
accepts `--program` (`ingest`, `backfill`, `store query`, `calendar show`); on mismatch
`typer.echo(f"unknown program {program!r} — choose from {PROGRAMS}", err=True); raise
typer.Exit(2)`.

**Acceptance:** `runner.invoke(app, ["ingest","--program","nope"])` asserts exit 2 and a
message containing the valid names, with no traceback; same for `store query` and `calendar show`.

### C-5 (Important) — docs promise "validation runs before every commit"; `run_backfill` validates nothing

**Where:** `docs/concepts/pipeline.md:25` and the `run_backfill` docstring
(`src/bls_stats/pipeline.py:450-451`) vs the code path `pipeline.py:494-527`.

**What:** `docs/concepts/pipeline.md` states validation "runs before every commit" and the
`run_backfill` docstring says `--dry-run` will "fetch and validate", but `run_backfill` never
calls `validate()` — pieces go fetch → stamp → `append_observations`. The store's schema gate
checks only the six vintage columns, not the unit-column Utf8 locks, null-rate, or row band. So a
truncated or malformed bulk download during backfill commits with no §7.3 gate firing.

**Remediation:** decide the contract and make code and docs agree. Either call `validate()` per
period slice in `run_backfill` (matching the docs/spec — note the row-band comparator question in
P-4 below if you do), **or** correct `docs/concepts/pipeline.md:25` and the docstring to say
backfill relies only on the store's vintage-schema gate. This finding pairs with pending item
**P-3** (same code location, flagged by the vintage reviewer) — resolve together.

**Acceptance:** whichever path is chosen, a test pins it: either a malformed backfill frame raises
`ValidationError`, or a test documents and asserts that backfill deliberately skips validation and
the docs say so.

### C-6 (Important) — `doctor` hard-fails (exit 1) on settings the docs call optional/warn-only

**Where:** `src/bls_stats/storage/doctor.py:43-63` (`check_env`), `src/bls_stats/cli.py:364`
(doctor exit) vs `docs/getting-started.md:23-25`.

**What:** the env table calls `BLS_API_KEY` "Optional" and says `doctor` merely "warns" on a local
store path; the bootstrap section says "Fix any red line before continuing." But `check_env`
returns `ok=False` for a missing API key, a non-`s3://` store URI, and the default contact email,
and the doctor command exits 1 if any check is not ok. `check_env`'s own docstring claims "Each
sub-check warns (does not hard-fail)" — contradicted by the code, which has no warn tier.

**Failure scenario:** a laptop user follows getting-started exactly (`BLS_STORE_URI=./data/store`,
no API key); `doctor` prints ✗ lines and exits 1 forever. Any scripted bootstrap gating on
doctor's exit never proceeds, though both settings are documented as supported/optional.

**Remediation:** add a warn tier to `CheckResult` (an `ok=True`-with-warning state, or a
three-state status) for `api_key`/`store_uri`/`contact_email` so doctor's exit code matches the
documented contract; the daily bucket-reachability and conditional-PUT checks stay hard-fail.
Alternatively, rewrite getting-started to say these produce failing red lines — but the warn tier
is the better fix since the doctor docstring already promises it.

**Acceptance:** a test asserts `doctor` exits 0 with a local store URI and no API key while still
printing the warnings; a test asserts a genuinely unreachable store still exits 1.

### C-7 (Minor) — CPS metadata cache path is relative to the process cwd

**Where:** `src/bls_stats/cli.py:298, 316, 343` — all three `metadata` commands hard-code
`Path("data/cps_metadata")`.

**What:** `fetch_metadata` mkdirs and writes the cache + `manifest.json` there, so the location
depends on the process's launch directory. This is the one cwd-relative durable write in the
package (everything else routes through `Settings.store_uri` or `TemporaryDirectory`). From cron or
a container with `cwd=/` the mkdir raises `PermissionError`; run from varying directories, the
cache silently duplicates and every `ln.*` file is re-downloaded each time, defeating the
manifest's skip-unchanged purpose and the politeness throttle.

**Remediation:** resolve the cache dir from configuration (a `BLS_METADATA_CACHE` setting, or
anchor it under a settings-derived data root) instead of the repeated cwd-relative literal.

**Acceptance:** a test asserts the cache path derives from `Settings`, not `Path.cwd()`; the env
table in README/getting-started documents the new variable.

### C-8 (Minor) — malformed `--ref-date`/`--as-of` produce raw ValueError tracebacks

**Where:** `src/bls_stats/cli.py:274, 279` (`store query`), `:342` (`metadata enrich`).

**What:** `date.fromisoformat(...)` with no error handling. A non-ISO string (e.g.
`--ref-date 2026-3-12`, unpadded month) raises `ValueError` → traceback + exit 1, which for
`store query` collides with its documented "exit 1 = no observations table" contract. Because the
parse happens after the table-existence check, behavior even differs by store state.

**Remediation:** wrap the parses in `try/except ValueError` → `typer.echo(..., err=True); raise
typer.Exit(2)`, or declare the options as typer/click date types so bad input becomes a usage
error.

**Acceptance:** `store query --ref-date 2026-3-12` on a populated store asserts exit 2 with a
message, no traceback.

### C-9 (Minor) — `cli.PROGRAMS` duplicates the registry and can silently diverge

**Where:** `src/bls_stats/cli.py:26`. Flagged by the layering reviewer.

**What:** `PROGRAMS = ["ces", …, "ep"]` re-states the `REGISTRY` keys as a literal. The pipeline
derives its default set from `REGISTRY`, but `calendar build`, `calendar refresh`, `store info`,
and `store maintain` iterate `cli.PROGRAMS` — two sources of truth, violating the ARCH §3
programs-are-data principle. `registry.py` has no heavy deps, so the lazy-import rationale doesn't
apply.

**Failure scenario:** a ninth program added to `REGISTRY` without touching `cli.py` gets ingested
but never scraped by `calendar build` (so `backfill` exits 2 on an empty calendar), never
compacted by `store maintain` (unbounded small-file growth), and omitted from `store info`/`gaps`.

**Remediation:** `PROGRAMS = list(REGISTRY)` (module-level import of `REGISTRY` is cheap); keep the
per-command `!= "ep"` exclusions.

**Acceptance:** a test asserts `cli.PROGRAMS == list(REGISTRY)`.

### C-10 (Minor) — `export_metadata` bypasses the Store protocol and welds Delta into the enrich layer

**Where:** `src/bls_stats/enrich/cps.py:179`.

**What:** `export_metadata` reaches into `store.uri`/`store.storage_options` and calls
`tagged.write_delta(..., mode="overwrite")` directly instead of through a `Store` method.
`storage/backend.py` documents the protocol as the ARCH §4.1 backend-swap boundary, but the
protocol offers no snapshot-replace operation, so this caller hard-codes the Delta backend into
enrich. (Distinct from the deferred `schema_mode`/`downloaded` items on the same function.)

**Failure scenario:** swapping the Delta backend for the Parquet escape hatch by replacing the one
storage module leaves `metadata export` still writing Delta under the Parquet store root — mixed
formats, broken one-module-swap contract.

**Remediation:** add a `replace_table(path, df)` snapshot-replace method to the `Store` protocol,
implement it in `VintageStore` with the existing overwrite `write_delta`, and have
`export_metadata` call it — folding in the deferred `schema_mode="overwrite"` fix at the same time.

**Acceptance:** `export_metadata` no longer references `write_delta` or `store.uri` directly; a
test double implementing `Store` (without `write_delta`) satisfies it.

### C-11 (Minor) — QCEW slot-expansion doc states the wrong within-year revision formula

**Where:** `docs/concepts/release-detection.md:43-45` vs `src/bls_stats/releases/profiles.py:43-45`.

**What:** the doc says a QCEW release for quarter *q* carries all quarters so far "each at terminal
revision 4 − q". The code assigns revision `i = (release quarter − carried quarter)`: Q3's release
carries Q3 at rev 0, Q2 at rev 1, Q1 at rev 2. The `4 − q` formula applies only to prior-year
quarters in a benchmark window (`_terminal_revision`). The `Slot` docstring already states it
correctly.

**Failure scenario:** a revision-behavior study filters `prints(revision=3)` expecting Q1's
within-year re-publications; actual within-year Q1 prints carry revisions 0/1/2/3 across the
Q1..Q4 releases, so the query misattributes vintages.

**Remediation:** rewrite the bullet — routine carries get revision `(release quarter − q)`; quarter
*q* reaches its terminal revision `4 − q` only at the year's Q4 release, and `4 − q` is also used
for prior-year quarters in a benchmark window.

**Acceptance:** doc text matches `_routine_periods`; no code change.

### C-12 (Minor) — `doctor` docs claim probes that don't exist

**Where:** `docs/cli.md:170-172` vs `src/bls_stats/storage/doctor.py:43-77`.

**What:** `docs/cli.md` lists "deltalake: local round-trip write/read" and "credentials present".
`check_deltalake` only imports the package and reports its version — no write/read. `check_env`
never inspects AWS credentials (only `BLS_API_KEY`). So an environment where deltalake imports but
local Delta writes fail passes green.

**Remediation:** change the docs to "deltalake importable (version reported)" and drop/qualify
"credentials present" — or implement the round-trip and AWS-credential checks. (Doc fix is the
minimal correct step; implementing the checks is the stronger option, coordinate with C-6.)

**Acceptance:** doc lines match what the probes actually do.

---

## 2b. Confirmed in the second pass (C-13…C-24)

These cleared the completed verification (≥2/3 votes). The two criticals are the most important
output of the entire review.

### C-13 (Critical) — re-polled benchmark release re-appends its whole window with a fabricated, climbing `benchmark` counter

**Where:** `src/bls_stats/pipeline.py:299` (the `prior_benchmark` callback) →
`src/bls_stats/releases/profiles.py:120` (`benchmark = prior_benchmark(rd) + 1`) →
`src/bls_stats/vintage/ledger.py:162-188` (`prior_benchmark_count`). Verified 3/3; independently
reproduced offline by a verifier (run 1 committed the window at `benchmark=1`; re-running the same
benchmark release added the whole window again at `benchmark=2`, 58 duplicate rows on the
candidate key).

**What:** a benchmark window slot's counter is `prior_benchmark_count(...) + 1`, computed from
already-**ingested** rows for that `(program, ref_date)` with **no `release_date` filter** — so it
counts the rows this same release ingested on the previous run. Both idempotency guards key on that
counter: the run-level anti-join `slot_status(..., benchmark)` (pipeline.py:301-304) and the
commit-level `slot_exists(..., benchmark)` (pipeline.py:404-405). When the recomputed counter
climbs (1 → 2 → 3…), neither guard finds a match, so the entire window is re-appended every run.
BLS feeds retain ~12 entries (~a year for monthly programs, ARCH §5.2), and the ledger anti-join is
the only thing stopping a still-visible release from being reprocessed — so the daily cron
duplicates the window every day the benchmark release stays in the feed. Routine slots are
unaffected (their `benchmark = prior_benchmark(rd)` is unchanged), which is why `test_rerun_is_noop`
passes; there is **no test that re-runs a benchmark release** — the replay suite runs the Feb-2027
benchmark exactly once. This violates ARCH §4.3 candidate-key uniqueness and the §7.2/§11 re-run
convergence guarantee. `latest()`/`as_of()` then surface the highest (fabricated) benchmark counter,
corrupting reads.

**Failure scenario:** CES publishes its February annual benchmark; the release stays in the empsit
feed for months; the daily `bls-stats ingest` cron re-appends the entire multi-year benchmark
window every day, each copy stamped `benchmark = 2, 3, 4, …` — none of which are real benchmark
events. Storage grows without bound and the vintage history is corrupted with counters that never
happened.

**Remediation:** make the benchmark-window idempotency independent of the derived counter. Options
(pick one, with a design note): (a) compute the window's benchmark from prints at strictly-**earlier**
`release_date`s only — i.e. have `prior_benchmark_count` exclude rows whose `release_date` equals
the release being processed; (b) short-circuit at the event level — if any window row for this exact
`release_date` is already ingested, treat the benchmark window as done; (c) key the "already done"
check on `(program, ref_date, release_date, kind)` rather than the counter-bearing slot key.

**Acceptance:** a regression test that runs the same benchmark release through `run_ingest`
**twice** (advancing the injected clock) asserts the observation count and the candidate-key
uniqueness are identical after the second run, and that no `benchmark=2` row is fabricated.

### C-14 (Critical) — catch-up/bootstrap `ingest` fetches the current flat file and stamps it as a historical print

**Where:** `src/bls_stats/pipeline.py:294` (the poll loop) and the `_fetch_event` LABSTAT branch.
Verified 3/3.

**What:** when `poll` returns more than one release — first-ever run, or the cron recovering after
an outage longer than a day — each not-yet-ingested older release is expanded and fetched. But the
LABSTAT fetch always downloads the **current** bulk file (latest revised state), and the pipeline
stamps it with the older release's `release_date` + expanded `revision`/`benchmark`. The result is
a vintage row claiming an old release date while carrying today's revised values — exactly the
clairvoyance the vintage model exists to prevent. The freshness guard does not save it: the current
file's `Last-Modified` is recent, so it passes. The intended catch-up tool is `backfill` (which
honestly stamps snapshot-date vintages with null counters), but nothing stops `ingest` from
processing back-dated releases.

**Failure scenario:** the ingest cron is down for a week; on recovery `poll` returns ~5 releases;
for each missed month `ingest` fetches the current file and writes a `revision=0` "first print" for
that month containing already-revised numbers — a fabricated point-in-time record that a backtest
would read as what BLS published that day.

**Remediation:** decide and enforce the contract. Either (a) guard `ingest` so it only fetches/commits
the **newest** release per program and defers/denies back-dated ones (recording them so `gaps` can
surface them), or (b) explicitly document that `ingest` must never be used for catch-up, and gate it
(e.g. refuse when `poll` returns a release older than the latest ingested for that program). Pairs
with C-1 (the `gaps` audit) and C-13.

**Acceptance:** a test where `poll` returns a release older than the store's latest ingested print
for that program asserts the old release is not committed as a live vintage (deferred/denied, per
the chosen contract).

### C-15 (Important) — QCEW / OEWS / EP bypass the stale-file freshness guard

**Where:** `src/bls_stats/pipeline.py:374-378`. Verified 2/3.

**What:** the guard fires only when `spec.increment_url.startswith("https://download.bls.gov")`.
QCEW's increment URL is `https://data.bls.gov/...`, OEWS's is `https://www.bls.gov/...`, so both
skip the freshness check entirely. For QCEW, a feed announcement that precedes the year-zip refresh
means a not-yet-updated zip can be fetched and committed as a current print. The host-prefix
allowlist is the root cause.

**Remediation:** key the freshness gate on whether the source supports a meaningful `Last-Modified`
freshness check (a per-program capability flag, or a `HEAD`-supported probe), not a hardcoded
`download.bls.gov` prefix. For QCEW specifically, confirm whether the `year_to_date` empty-slice
deferral already covers the newest quarter and document the residual.

**Acceptance:** a test asserts the freshness guard is consulted for QCEW (via `fresh_fn`), or a
documented rationale explains why QCEW is exempt and what protects it instead.

### C-16 (Important) — row-band validation gate is silently skipped for every program's first increment

**Where:** `src/bls_stats/pipeline.py:214` (`_comparator`). Verified 3/3.

**What:** `_comparator` selects prior ingested rows with `revision.eq_missing(...)` against the new
slot's revision. Backfill rows carry null `revision`/`benchmark`, so a live `revision=0` increment
finds no comparator and `validate`'s row-band gate is skipped for the first increment after a
backfill (i.e. the first live print of every program). Real but bounded: the schema and null-rate
gates still run; only the ±20% row-count band is skipped, and only once per program.

**Remediation:** decide whether the first increment should compare against the backfill row count
(treat a null-revision backfill row as a valid comparator for `revision=0`), or accept that the
first increment has no meaningful band and document it. If comparing, adjust `_comparator` to fall
back to the latest ingested row for the program regardless of revision when no same-revision
comparator exists.

**Acceptance:** a test pins the chosen behavior — either the first increment is band-checked against
the backfill count, or a comment/doc records the deliberate skip.

### C-17 (Important) — conditional-PUT doctor probe writes to the bucket root, ignoring the store prefix

**Where:** `src/bls_stats/storage/doctor.py:133-134`. Verified 2/3.

**What:** the probe extracts the bucket as `store_uri.removeprefix("s3://").split("/", 1)[0]` —
discarding the key prefix — and PUTs the probe object at the bucket root. Under prefix-scoped IAM
(credentials permitted only under `s3://bucket/prefix/…`, a common least-privilege setup), the
root PUT is denied and `doctor` reports the store as *not* conditional-PUT-safe even though the
actual store path is. A false red line that blocks bootstrap.

**Remediation:** write the probe object under the store's actual prefix (reuse the full
`store_uri` path, appending a probe key), so the probe exercises the same authorization scope the
pipeline uses.

**Acceptance:** a test (or doctor self-check) asserts the probe key includes the store prefix, not
just the bucket.

### C-18 (Important) — OEWS code-column dtype lock is illusory

**Where:** `src/bls_stats/engines/oews.py:53`. Verified 2/2.

**What:** the engine casts only `area`/`occ_code` to `Utf8` after schema inference; other code
columns (`naics`, `own_code`) are left at their inferred types, so leading zeros can be lost before
the frame ever reaches the store's vintage-column-only guard. The string-lock invariant (all code
columns `Utf8`, leading zeros preserved) is not actually enforced for OEWS.

**Remediation:** read the OEWS workbook with all code columns forced to `Utf8` (schema override at
read time, as the LABSTAT engine does), or extend the post-read cast to every code column
(`naics`, `own_code`, and any other), not just `area`/`occ_code`.

**Acceptance:** a test on a fixture workbook asserts `naics`/`own_code` come back `Utf8` with
leading zeros intact.

### C-19 (Important) — QCEW reads the entire uncompressed CSV into memory, contradicting the streaming/<8 GB design target

**Where:** `src/bls_stats/engines/qcew.py:57-63` (`_read_zip_csv`). Verified 2/3.

**What:** `_read_zip_csv` does `pl.read_csv(fh.read(), ...)` — `fh.read()` decompresses the entire
CSV member (a single-file QCEW year is multiple GB uncompressed) into an in-memory buffer and
`read_csv` parses it eagerly. ARCH §10 states flat files are parsed via lazy/streaming scans with a
peak-RSS target < 8 GB and QCEW processed one year at a time; the eager full-member read is the one
place that can blow the target on a large year.

**Remediation:** stream the CSV member — extract to a temp file and `pl.scan_csv(...).collect(streaming=True)`,
or use a streaming reader over the zip member — so peak RSS stays bounded. Verify against the
largest recent QCEW year.

**Acceptance:** a test or a documented measurement shows QCEW single-year parse peak RSS stays under
the target; the eager `fh.read()` into `read_csv` is gone.

### C-20 (Important) — `run_backfill` success, idempotency, and crash-repair paths are entirely untested

**Where:** `tests/test_pipeline.py:172` (the four existing backfill tests all assert `== 2`, the
error paths). Verified 3/3.

**What:** every existing `run_backfill` test exercises a failure exit (2); none covers the success
path, the re-run-is-a-no-op idempotency, or the `slot_exists` crash-repair. Given C-14/C-15 touch
backfill and C-16 touches the comparator, this is the highest-value coverage gap.

**Remediation:** add tests for: a clean backfill commits the expected snapshot vintages with null
counters; a second identical backfill is a no-op; a crash between append and ledger record is
repaired on re-run without duplication.

**Acceptance:** the three behaviors above are pinned by passing tests.

### C-21 (Minor) — `run_ingest` mixed-outcome exit 1 and cross-event isolation are never exercised

**Where:** `tests/test_pipeline.py:124`; behavior at `pipeline.py:323-326`. Verified 3/3.

**What:** no test produces a run with both a failed and a succeeding event, so the "mixed → exit 1"
branch and the per-event isolation (one program failing must not block others) are uncovered.

**Remediation:** add a test with two events where one raises and one succeeds; assert exit 1 and that
the succeeding event's data committed.

**Acceptance:** the mixed-outcome path is pinned.

### C-22 (Minor) — `gaps` command logic has zero tests

**Where:** `src/bls_stats/cli.py:195` (and the whole command). Verified 3/3. Couples with **C-1** —
add these tests as part of the C-1 fix, since C-1 changes what `gaps` computes.

**What:** the program filter, the unexplained anti-join, the acknowledged count, and the exit-code
contract are untested. Given C-1 shows the command doesn't do what it's documented to do, tests must
land with the fix.

**Remediation/Acceptance:** covered by C-1's acceptance criteria.

### C-23 (Minor) — `test_bad_log_level_falls_back_to_info` asserts the opposite of its name

**Where:** `tests/test_cli.py:42-51`. Verified 3/3.

**What:** the test is named for the INFO fallback and its comment says `_setup()` "falls back to
INFO", but its only assertion is `assert settings.log_level == "verbose"` — it checks the raw
setting, never that logging actually falls back to INFO. The fallback could be deleted and the test
would still pass.

**Remediation:** assert the effective logging level after `_setup()` (e.g. the root logger's level
is `INFO` given a bad `BLS_LOG_LEVEL`), not the stored string.

**Acceptance:** the test fails if the INFO-fallback branch is removed.

### C-24 (Minor) — `calendar build` docstring says "rebuilds from scratch"; the code appends

**Where:** `src/bls_stats/cli.py:101-111` — docstring says "Rebuilds the `release_calendar` state
table from scratch," but the code calls `store.append_state(...)`, which appends. Verified 3/3.

**What:** re-running `calendar build` appends another full scrape rather than replacing, so the
state table accumulates duplicate calendar rows across runs (downstream consumers are duplicate-safe,
but the docstring is wrong and the growth is real).

**Remediation:** either make `calendar build` replace the table (snapshot-overwrite) to match the
docstring, or correct the docstring to say it appends and note the dedup expectation. Coordinate
with the deferred `calendar.build` dedup item.

**Acceptance:** docstring and behavior agree; if replace is chosen, a test asserts a second build
does not double the row count.

---

## 3. Refuted at proposed severity, but with a real mechanism (kept as sub-threshold)

The second pass reproduced the **mechanism** in each of these but the majority refuted the proposed
(important) severity because a backstop, spec mandate, or unreachable trigger neutralizes the harm.
They are recorded — not dropped — because several are worth a cheap hardening fix and one may be
re-raised if assumptions change. Severity shown is the corrected one.

- **V4 → subsumed by C-5 (not a separate defect).** `run_backfill` committing without `validate()`
  is **spec-mandated** (ARCH §8, §7.3: backfill is the trusted comparator baseline). So the behavior
  is correct; the only real issue is the docstring/docs claiming validation happens — which is
  exactly **C-5**. No new work beyond C-5.
- **V6 (Minor) — Delta append can silently cast data columns.** `append_observations` dtype-checks
  only the six vintage columns; delta-rs coerces others on write, and Float64→Int64 is silently
  lossy. But **no engine emits an Int64 native column** (all value columns are pinned Float64), so
  the lossy path is inert today. Residual worth a guard: the string-lock on code columns is enforced
  at parse time, not here — a defensive dtype assertion on unit columns in `append_observations`
  would make the invariant local. Low priority.
- **V7 (Minor) — `store maintain` is a second Delta writer.** `optimize.compact()`/`vacuum()` write
  Delta commits, so concurrent overlap with ingest could lose a commit **in unsafe-rename mode
  only**. The default `aws_conditional_put=etag` mode is immune, and the README schedules maintain
  and ingest disjointly. Residual: a doc note "don't run ad-hoc ingest during maintain in
  unsafe-rename mode," or an advisory lock. Low priority.
- **V11 (Minor) — `parse_matrix` can fabricate a degenerate 1-row frame.** Real and reproducible
  (a matrix page where every data row's cell count mismatches the header yields a 1-row
  occupation-only frame), but **EP is unwired** (guarded exit 2), so no production path reaches it,
  and the surviving concern sits inside the accepted EP-wiring deferral. Fix when EP is wired: log
  and skip the degenerate case instead of writing it to the Parquet cache.
- **V16 (Minor) — `filter_published` counts future scheduled rows toward `max_ref`.** True at the
  unit level (`max_ref` is inflated by scheduled rows with non-null `release_date`), but the
  `run_backfill` empty-slice guard intercepts, so no unpublished period is ever emitted to the store.
  Residual: a redundant fetch attempt for a not-yet-present period. Low priority; add a future-dated
  fixture when touched.

### 3.1 Contested — RESOLVED (2026-07-05): in scope, promoted to C-25

- **V15 → C-25 (Important) — pinned cancel-drop rule permanently omits periods republished under a
  later release** (`releases/calendar.py:331`, cites CES October 2025). The verifiers split (1
  refute / 1 uphold / 1 stalled): the refute called the drop ARCH §5.4-mandated; the uphold called
  it a real defect for a store first seeded **after** a government shutdown, where a period
  cancelled in the schedule but later published under a rescheduled release is permanently omitted
  from backfill. **Decision (human, 2026-07-05): the post-shutdown-seeding case IS in scope.** The
  lapse overlay / `filter_published` must distinguish "cancelled and never published" (stays
  dropped) from "rescheduled and later published" (must be retained). Treated as a confirmed
  Important defect — see **C-25** below.

### C-25 (Important) — rescheduled-and-later-published periods must not be dropped by the cancel rule

**Where:** `src/bls_stats/releases/calendar.py:331` (`filter_published` cancel-drop set) and the
lapse overlay `apply_lapse_overlay` (~calendar.py:202-211). Promoted from contested V15 by human
decision.

**What:** `filter_published` drops periods whose `release_date` is null (cancelled). When a release
is cancelled in the schedule but subsequently republished under a *rescheduled* later release, the
overlay/pin currently leaves it dropped — so a store first seeded after the shutdown never backfills
that period, even though BLS did publish it. The drop set must exclude periods that were ultimately
published under a rescheduled `release_date`.

**Failure scenario:** a government shutdown cancels the CES October 2025 release in the schedule;
BLS later publishes October 2025 under a rescheduled date. A store seeded in 2026 runs
`backfill --program ces` over a range including 2025/10; `filter_published` drops 2025/10 as
cancelled, so it is never fetched — a permanent, silent hole for that vintage.

**Remediation:** in the lapse overlay, when a rescheduled release supplies a non-null
`release_date` for a period that a prior schedule row cancelled, retain the period (keyed on
`original_release` → rescheduled `release_date`) rather than treating it as cancelled. Equivalently,
compute the cancel-drop set in `filter_published` from periods that have *no* published
`release_date` in *any* calendar row (cancelled and never republished), not merely a null in the
row being examined.

**Acceptance:** a calendar fixture with a cancelled-then-rescheduled period asserts
`filter_published` **retains** that period, while a cancelled-and-never-published period is still
dropped; a companion test asserts the backfill period set includes the republished period.

### 3.2 Reconciliation of my earlier hand-verifications vs the verified pass

- **V14 (was P-12) — the agents corrected my call.** I hand-verified that `_ABBR_DATE`
  (`calendar.py:54`) rejects "Sept." and full month spellings, and concluded the finding held. The
  verified pass showed I over-credited it: "June"/"July" are **reference-period** tokens parsed by a
  *different* regex (`_MONTH_YEAR`, line 51) that accepts full month names — so 2 of the 3 spellings
  the finding cites are non-issues (category error). The narrow true residual is that `parse_abbr_date`
  (release dates + lapse overlay) rejects the 4-letter "Sept."; the schedule path has a numeric-date
  backstop, but the lapse-overlay path does not. Net: **refuted at "important", real as a Minor**
  lapse-overlay hardening item — widen `_ABBR_DATE` to accept "Sept"/"Sept." (and ideally full
  month names) for robustness. My other hand-verifications held: **V1/C-13** and **V3/C-15** were
  both confirmed by the pass; **V2/C-14** confirmed.

---

## 4. Refuted (mechanism does not hold, or no reachable harm)

- **V18 / `core/http.py:179` — "naive `Last-Modified` interpreted in host-local time" (0/3).** The
  skew requires `parsedate_to_datetime` to return a *naive* datetime, which happens only for
  asctime-format or `-0000` headers; the sole caller chain is gated to `download.bls.gov` increment
  URLs, and those emit standard tz-aware IMF-fixdate `Last-Modified`, making the `.astimezone(UTC)`
  a correct no-op. RFC 9110 forbids the numeric-zone form. The two-line defensive fix is valid
  polish but the failure path is not reachable with real BLS inputs.
- **V9 / `storage/doctor.py:137` — conditional-PUT probe reports "probe failed" instead of a
  NOT-honored verdict (0/3).** Refuted as a defect: the probe's cleanup/verdict handling is
  acceptable for its purpose, and the scenario (an endpoint that rejects `If-None-Match` on the
  first PUT) does not produce a materially misleading result in practice.
- **V13 / `enrich/cps.py:156` — row-count invariant via `assert`, vanishes under `python -O` (0/3).**
  Refuted: the package is not run under `-O` in any documented path, and the invariant is also
  guaranteed structurally by the left-join semantics; the `assert` is a belt-and-braces check, not
  the sole guard.

---

## 5. Recommended triage and status

**Verification is complete** — every finding has a verdict. Fix order by risk × likelihood:

1. **C-13** (critical) — benchmark re-append is active, unbounded data corruption on the normal
   daily cron. Highest priority; ships with the twice-run regression test.
2. **C-14** (critical) — catch-up/bootstrap `ingest` fabricating historical prints. Settle the
   `ingest`-vs-`backfill` catch-up contract.
3. **C-1 / C-2** — the `gaps` monitoring blind spot and the ingest-wide feed-parse crash; both
   undermine core guarantees. (C-2 closes the old P-16 too.)
4. **C-3, C-4, C-6** — silent OEWS backfill data loss, CLI KeyError tracebacks, doctor exit contract.
5. **C-15, C-16** — freshness-guard host allowlist and the skipped first-increment row band.
6. **C-5** — settle the backfill-validation docs/contract (subsumes V4).
7. **C-17, C-18, C-19** — prefix-scoped doctor probe, OEWS string-lock, QCEW memory.
8. **C-25** — cancelled-then-rescheduled periods dropped from backfill (resolved in scope).
9. **C-20 / C-21 / C-22 / C-23** — the test-coverage gaps (backfill, mixed-ingest, `gaps`, log-level).
10. Confirmed doc/polish minors (C-7…C-12, C-24) and the §3 sub-threshold hardening items.

**Provenance:** first review run `wf_349cd0e4-437` (finders + partial verify); completion run
`wf_2f1d5888-6ea` (23 findings × 3 lenses, 2 verifier stalls that still reached quorum). Both
workflow scripts are under the session's `workflows/scripts/`. Every confirmed finding above has a
concrete remediation and acceptance criterion — none is a speculative "consider adding".
