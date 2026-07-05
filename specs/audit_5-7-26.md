# bls-stats codebase audit — 2026-07-05

> **STATUS: OPEN** — findings only, no code changed. This document is the source of record for a
> whole-codebase adversarial review. Confirmed findings are written as actionable requirements
> (location, failure scenario, remediation, acceptance criteria) so this can feed a plan through
> the normal specs → plans → implementation flow. Nothing here is fixed yet.

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

### 1.1 Verification was truncated — read this before acting

The account hit its spend limit **partway through the verification phase**. The dimension finders
all completed, but a large batch of verifier agents died mid-flight with budget errors. Because a
finding is only "confirmed" once it collects ≥2 upheld votes, **findings whose verifiers all died
were pushed into the refuted/inconclusive bucket regardless of merit.** Concretely:

- **17 findings** cleared verification with 2–3 upheld votes → **§2 Confirmed**.
- **1 finding** was genuinely refuted by surviving verifiers → **§4 Refuted**.
- **22 findings** never received a completed verdict — their verifiers all errored out. These are
  **not refuted**; they are **unadjudicated** → **§3 Pending verification**. Several are
  high-severity. I hand-verified the two criticals and two of the importants directly against the
  code (results noted inline); the rest still need the adversarial pass that the budget cut short.

**Do not treat §3 as dismissed.** When budget resets, the remaining verification should be
completed (§5 has the mechanism).

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

## 3. Pending verification (finders flagged; adversarial pass did not complete)

**These are not refuted.** Their verifier agents all errored out on the budget limit before
returning a verdict, so they carry the finder's proposed severity but no independent confirmation.
Four are hand-verified below (marked ✔ hand-verified); the rest need the §5 completion pass.
Severities are the finder's own until verified.

### Critical (unverified) — verify these first

- **P-1 ✔ hand-verified — Re-polled benchmark release re-appends its whole window with
  `benchmark+1` every run.** `pipeline.py:299` → `profiles.py:120` → `ledger.py:162`. The
  benchmark counter for a window slot is `prior_benchmark_count(...) + 1`, computed from
  already-**ingested** rows; the idempotency guard `slot_status` keys on that same `benchmark`
  value. So when a benchmark release is re-polled — which happens for ~a year, since feeds list
  the last ~12 entries — run 1 ingests the window at `benchmark=1`, run 2 reads prior=1 and
  assigns `benchmark=2` (guard finds no match → re-appends), run 3 assigns 3, and so on. The
  entire benchmark window is duplicated once per run with a fabricated, monotonically-climbing
  counter until the release scrolls out of the feed. `latest()` still returns sane values (highest
  benchmark, stable data), which is why it hides, but the store accumulates hundreds of duplicate
  window copies and the vintage history records benchmark counters 2,3,4… that never occurred.
  **I confirmed this mechanically against the code.** The replay integration test polls each
  release exactly once, so it cannot catch it.
  *Likely remediation:* make the window-slot idempotency independent of the counter — e.g. key the
  "already done" check on `(program, ref_date, release_date, kind)` rather than the derived
  `benchmark`, or have `prior_benchmark_count` exclude rows from the current `release_date`, or
  compute the next benchmark once per release and short-circuit if the release already produced any
  ingested window row. Needs a design decision + a re-poll regression test.

- **P-2 — Older polled releases fetch the *current* flat file and stamp it as a historical print.**
  `pipeline.py:294`. On bootstrap-via-`ingest` or outage catch-up, `poll` returns multiple
  releases; for each not-yet-ingested older release, `_fetch_event` downloads the current LABSTAT
  file (always latest state) and stamps it with the old `release_date` + expanded
  revision/benchmark — fabricating a vintage that claims an old release date but carries revised
  values, the exact clairvoyance the vintage model exists to prevent. *Hand-read confirms the code
  path exists* (the freshness guard passes because the current file's `Last-Modified` is recent);
  materiality hinges on whether `ingest` is expected to do catch-up at all (the intended tool is
  `backfill`). Needs adjudication: either guard `ingest` against back-dated releases (fetch the
  current file only for the newest release; defer/deny older ones), or document that `ingest` must
  not be used for catch-up and gate on it.

### Important (unverified)

- **P-3 — `run_backfill` commits the seed vintage with no validation gates** (`pipeline.py:504`).
  Same code location as confirmed **C-5**; the vintage reviewer flagged the behavior, the docs
  reviewer flagged the doc mismatch. Resolve as one unit.
- **P-4 — Row-band comparator never matches backfill rows, silently skipping the gate for every
  program's first increment** (`pipeline.py:214`). `_comparator` filters ledger rows on
  `revision.eq_missing(...)`; backfill rows have null revision/benchmark, so the first live
  increment (revision 0) finds no comparator and the row-band gate is skipped. Plausible; verify
  whether that's acceptable (first increment has nothing to compare to anyway) or a real hole.
- **P-5 ✔ hand-verified — QCEW/OEWS/EP bypass the stale-file freshness guard.** `pipeline.py:374-377`.
  The guard fires only when `increment_url.startswith("https://download.bls.gov")`. QCEW's URL is
  `https://data.bls.gov/...`, OEWS's is `https://www.bls.gov/...` — **confirmed** both skip the
  guard. For QCEW a not-yet-refreshed year zip could be fetched and committed as a current print.
  Materiality needs checking (the year_to_date empty-slice-defer may cover the newest quarter);
  the guard's host allowlist is the root cause and should key on freshness support, not a hostname
  prefix.
- **P-6 — Delta append silently casts non-vintage columns, incl. lossy Float64→Int64**
  (`storage/delta.py:82`). `append_observations` enforces dtypes only on the six vintage columns;
  delta-rs may coerce data columns on write. Verify whether any engine emits a column whose Delta
  cast is lossy.
- **P-7 — `store maintain` is a second Delta writer; overlap with ingest/backfill in
  unsafe-rename mode can lose a committed vintage** (`cli.py:242`). Verify against the
  single-writer operational assumption (README schedules them at different times).
- **P-8 — Conditional-PUT probe writes to the bucket root, ignoring the store prefix**
  (`doctor.py:134`) — false failure under prefix-scoped IAM. Plausible; verify against how the
  probe key is constructed.
- **P-9 — OEWS code-column dtype lock is illusory: cast-after-inference is lossy and `naics`/
  `own_code` are unguarded** (`engines/oews.py:53`). Verify against a real workbook's column types
  (the T16 review found leading-zero risk theoretical, but this claims a broader gap).
- **P-10 — `parse_matrix` silently drops mismatched rows and can fabricate a 1-row
  occupation-only frame, poisoning the year-long Parquet cache** (`engines/ep.py:123`). EP is
  unwired (guarded exit 2), so blast radius is limited today, but the cache is written regardless.
- **P-11 — QCEW reads the full ~4 GB uncompressed CSV into memory, breaking the ARCH §10
  streaming/<8 GB claim** (`engines/qcew.py:59`). Verify whether the scan is truly eager; if so it
  contradicts a stated design target.
- **P-12 ✔ hand-verified (code side) — `parse_abbr_date` rejects "Sept." and full month names,
  silently dropping schedule/lapse rows** (`releases/calendar.py:54`). The regex
  `[A-Z][a-z]{2}\.?\s+` matches exactly a 3-letter abbreviation then whitespace. **Confirmed**
  "Sept. 2, 2026" (4-letter) and "June 2, 2026" (full) both fail to match → row dropped.
  September is the month BLS routinely abbreviates to "Sept.", so this is realistic. Trigger
  depends on the live page format (offline I can only confirm the parser rejects those spellings).
  A dropped schedule/lapse row means a missing calendar entry → wrong `backfill` filtering and a
  `gaps` blind spot.
- **P-13 — Pinned cancel-drop rule permanently omits periods whose data was published under a
  later release** (`releases/calendar.py:331`, cites CES October 2025). Verify against
  `filter_published` semantics and a shutdown/lapse fixture.
- **P-14 — `filter_published` counts future scheduled releases as published, emitting unpublished
  periods** (`releases/calendar.py:330`). Verify the date comparison against a pinned "today".
- **P-15 — `run_backfill` success path, idempotency, and crash-repair are completely untested**
  (`tests/test_pipeline.py:172`). Coverage gap distinct from the deferred-list items; verify none
  of those paths are exercised.

### Minor (unverified)

- **P-16 — `feeds.py:134` invalid embedded date aborts the whole poll** — same root as confirmed
  **C-2**; fixing C-2 closes it.
- **P-17 — Naive `Last-Modified` interpreted in host-local time skews the freshness guard**
  (`core/http.py:179`). *Note: a sibling finding at the same line was genuinely refuted — see
  §4.* Verify whether this variant differs.
- **P-18 — Enrich row-count invariant enforced via `assert`, vanishes under `python -O`**
  (`enrich/cps.py:156`).
- **P-19 — `run_ingest` exit-code-1 (mixed failed/ok) and cross-event isolation never exercised**
  (`tests/test_pipeline.py:124`).
- **P-20 — `gaps` unexplained/acknowledged logic and exit codes have zero tests** (`cli.py:195`) —
  couples with **C-1**; add tests when C-1 is fixed.
- **P-21 — `test_bad_log_level_falls_back_to_info` cannot detect loss of the INFO fallback it
  names** (`tests/test_cli.py:51`) — a test that asserts less than its name claims.
- **P-22 — `calendar build` docstring says "rebuilds from scratch"; the code appends**
  (`cli.py:101`).
- **P-23 — Conditional-PUT probe reports "probe failed" instead of a NOT-honored verdict when an
  endpoint rejects `If-None-Match` on the first PUT** (`doctor.py:137`).

---

## 4. Refuted (survived verification as false)

- **`core/http.py:179` — "Naive `Last-Modified` interpreted in host-local time."** Verifier
  established the skew requires `parsedate_to_datetime` to return a *naive* datetime, which only
  happens for asctime-format or `-0000` headers, and no real input on this code path produces one:
  the sole caller chain (`head_last_modified` ← `is_fresh` ← `_process_event`) is gated to
  `download.bls.gov` increment URLs, and a live HEAD returns a tz-aware RFC-1123 date. Refuted as
  written. (A differently-scoped variant remains as pending **P-17** — the finder claimed a broader
  trigger; worth a look but low priority.)

---

## 5. Recommended triage and how to finish the review

**Fix order (by risk × likelihood):**

1. **P-1** (critical, hand-confirmed) — benchmark re-append is active data corruption on every
   benchmark cycle. Highest priority.
2. **C-1 / C-2 / P-2** — the monitoring blind spot, the ingest-wide crash, and the historical-print
   fabrication together undermine the pipeline's core guarantees.
3. **C-3, C-4, C-6** — silent data loss and broken CLI/doctor contracts operators rely on.
4. **C-5 + P-3 + P-4** — settle the backfill validation contract as one unit.
5. Remaining confirmed minors (C-7…C-12) and the pending list.

**Completing verification:** when account budget resets, re-run the adversarial verification for
the §3 pending findings. The workflow script is saved at
`.../workflows/scripts/thorough-codebase-review-wf_349cd0e4-437.js` and supports resume
(`Workflow({scriptPath, resumeFromRunId: "wf_349cd0e4-437"})`) — completed finder/verifier agents
return cached results, so only the starved verifiers re-run. Alternatively, verify the pending
items directly against the code as I did for P-1/P-2/P-5/P-12.

**Note on the fixes:** every confirmed finding has a concrete remediation and acceptance criterion
above; none is a speculative "consider adding". The confirmed set is dominated by two clusters —
the `gaps` command not matching its own contract, and CLI/doctor error-handling that turns
operator mistakes into tracebacks — plus the OEWS backfill data-loss bug. Those clusters, and P-1,
are where a fix plan should start.
