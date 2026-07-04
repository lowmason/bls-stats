# bls-stats — Architecture Specification

**Status:** approved design, awaiting implementation plan
**Date:** 2026-07-04
**Companion document:** [`bls-stats.md`](../bls-stats.md) (the behavioral recreation spec)

---

## 0. How this document relates to `bls-stats.md`

`bls-stats.md` describes the *behavior* of the original system: program source URLs, output
columns, `ref_date` rules, and per-program gotchas. **Its §2 (program data contracts), §3
(reference-period generation), and §4 (`ref_date` rules) remain authoritative** and are not
duplicated here.

This document supersedes or fills the rest:

| Original section | Disposition |
|---|---|
| §1 overview | Superseded by the two-stage, vintage-aware model (§2 below) |
| §2 program contracts | **Authoritative as written**, except: CPS-PUMS and CPS-telework are **out of scope** (all dangling references to them, incl. in §4/§10, are void); every output schema gains the vintage columns of §4 below |
| §5 release-date tracking | Retained, extended with typed release events and revision profiles (§5 below) |
| §6 ingest | Superseded by §7 below (same crash-safety invariant, new mechanism) |
| §7 storage (TBD) | Filled by §4 below |
| §8 CLI (TBD) | Filled by §8 below |
| §9 cross-cutting | Retained; config table replaced by §10 below |

Scope note: the package covers **eight** products — `ces`, `sae`, `jolts`, `cps`, `bed`,
`qcew`, `oews`, `ep` — plus the CPS metadata dimension tables.

---

## 1. Fixed constraints

- Python library **and** CLI. **Polars** for all tabular data (no pandas).
- Toolchain: **uv** + **hatchling**, **ruff** (lint + format), **typer** (CLI), **pytest**.
- Python ≥ 3.12. Package name `bls-stats`, import `bls_stats`.
- Deployment target: small ephemeral containers (**4 cores / 25 GB RAM / 300 GB scratch
  disk**). **No durable local storage** — anything persistent goes to the object store.
- The employer is never named in code, comments, docs, or published artifacts. Storage is
  described generically as "an S3-compatible object store"; env vars use standard AWS names.
- Secrets live in **`.project.env`** (python-dotenv, loaded explicitly by that name), which is
  gitignored from the first commit.

---

## 2. System model: two-stage, vintage-aware ingest

The system maintains a **vintage store**: every observation is identified not only by *what it
measures* (`series_id`, `ref_date`) but by *which BLS release said so* (`release_date`, plus
semantic print counters). Old vintages are immutable; new releases only append.

**Stage 1 — backfill (one-time per program).** Bulk-download full history from the flat
files / ZIPs / workbooks in `bls-stats.md` §2, filter to published periods, commit as the seed
vintage.

**Stage 2 — daily increment (cron).** Poll the BLS feeds; when a program published today,
fetch exactly the reference periods that release touches (its "prints"), stamp them with the
release's vintage identity, and append. Benchmark-classified releases additionally trigger a
windowed history re-snapshot.

### 2.1 The revision/benchmark print model

Each `(program, ref_date)` accumulates prints identified by a tuple of two independent
counters, stored as separate columns:

- **`revision`** — routine print number, **derived from the release's own structure** (a CES
  release always carries slots t, t−1, t−2 → revisions 0, 1, 2). Structurally correct even for
  months whose earlier prints predate tracking.
- **`benchmark`** — count of benchmark events observed for this `ref_date` **since tracking
  began**. Honest but tracking-relative; absolute counts are calendar-derivable later without
  touching stored data.

Program lifecycles (working model; verify empirically during implementation, esp. QCEW):

| Program | Routine prints | Benchmark events | Example lifecycle |
|---|---|---|---|
| `ces` | 3 (first/second/third) | annual benchmark, lands with January data (SA reach ~5 yr) | (0,0)→(1,0)→(2,0)→(2,1)→…→(2,5) |
| `sae` | 2 (prelim/revised) | annual rebenchmark to QCEW+NCE | (0,0)→(1,0)→(1,1)→… |
| `jolts` | 2 | annual re-alignment to CES (~5 yr) | (0,0)→(1,0)→(1,1)→… |
| `cps` | 1 | January population controls + SA reestimation | (0,0)→(0,1)→… |
| `qcew` | prelim + revision by each subsequent quarterly release | annual finalization with Q1 | (0,0)→(1,0)→(2,0)→(3,0)→(3,1)→… |
| `bed` | 1 | annual revision with Q1 | (0,0)→(0,1)→… |
| `oews` | 1, never revised (each May release is its own vintage) | none | (0,0) |
| `ep` | full replace per projection cycle | none | (0,0) per cycle |

### 2.2 Event-scoped capture rule

- **Routine releases** store only the profile-declared print slots (CES: 3 ref_dates/release).
- **Benchmark events** store one windowed full snapshot (window per program config; CES ≈ 5
  years to match SA revision reach), sourced from the bulk flat file, stamped `benchmark+1`.
  Unchanged values inside the window are stored — "republished unchanged" is information.
- Concurrent-seasonal-adjustment drift between events is **deliberately not captured**.
- **Knowability boundary:** bulk files contain only the current vintage, so print history is
  capturable only from the day the cron starts. A print not captured while it was the live
  vintage is **permanently lost** and is recorded as such (§5.3), never fabricated.

---

## 3. Package layout & dependency rule

```
bls_stats/
├── registry.py        # ProgramSpec dataclasses + 8-program registry (pure data)
├── core/
│   ├── periods.py     # reference_periods(), ref_date rules, period math
│   ├── series_id.py   # fixed-width series-ID codec (prefix → field layout)
│   └── http.py        # the one httpx client: UA, retry/backoff, throttle
├── engines/
│   ├── protocol.py    # Engine protocol: backfill(spec, periods), increment(spec, release)
│   ├── labstat.py     # flat-file engine for ces, sae, jolts, cps, bed
│   ├── api_v2.py      # BLS API v2 utility engine (targeted fetches, spot checks)
│   ├── qcew.py        # per-year ZIP streaming
│   ├── oews.py        # workbook extraction
│   └── ep.py          # throttled HTML matrix scraper
├── releases/
│   ├── feeds.py       # Atom feed poll → typed Release events
│   ├── calendar.py    # schedule/archive scrape → release-date table; gap detection
│   └── profiles.py    # RevisionProfile: Release → [(ref_date, revision, benchmark)] plan
├── vintage/
│   └── ledger.py      # ingest ledger; (revision, benchmark) assignment
├── storage/
│   ├── backend.py     # protocol: exists / append / scan / state tables
│   ├── delta.py       # primary backend (delta-rs)
│   ├── s3_parquet.py  # escape-hatch backend (plain Parquet, write-once)
│   └── doctor.py      # endpoint capability probes
├── enrich/
│   └── cps.py         # CPS series catalog + ln.* mapping joins; footnote resolution
├── pipeline.py        # orchestrator: detect → expand → fetch → validate → commit → record
└── cli.py             # typer app
```

**Dependency rule (one-directional):**
`cli → pipeline → {engines, releases, vintage, storage} → core → registry`

- Engines **return** DataFrames; they never import storage.
- Storage takes frames and paths; it doesn't know what a "program" is.
- Only `vintage/ledger.py` mutates state.
- Programs are **data** (registry entries selecting an engine + parameters), not subclasses.
  The EP scraper is the honesty check: bespoke behavior stays code (its own engine), and the
  registry schema must not grow conditionals to avoid becoming a DSL.

---

## 4. Storage: the Delta vintage store

### 4.1 Backend decision (recorded)

**Delta Lake via `delta-rs` (`deltalake` PyPI package) is the primary backend.**

- *Why:* a release event lands as one **atomic multi-file commit** — the crash-mid-push
  problem that plain Parquet would solve with a hand-rolled commit-marker scheme (a worse,
  homemade transaction log). Plus schema enforcement on write and native Polars reads
  (`pl.scan_delta`).
- *Evidence:* `deltalake` verified installable and functional in the deployment container
  (uv round-trip, 2026-07-04). Local MinIO verified to support conditional PUT (`If-None-Match`
  → 412) and a live Delta write/append/scan round-trip (probe, 2026-07-04).
- *Commit-safety mode is per-environment, decided by `doctor`:* conditional PUT supported →
  `aws_conditional_put=etag` (fully safe commits); unsupported → single-writer mode (documented
  discipline; equivalent to what plain Parquet offers). The design has exactly one writer (the
  daily cron), so both modes are sound.
- *What would reverse it:* delta-rs breakage in the deployment environment, or multi-writer
  requirements arriving while conditional PUT is unavailable. The escape hatch (`s3_parquet.py`)
  shares the identical logical schema, so the swap cost is one module.
- *New chore it introduces:* small-file accumulation from daily appends → `store maintain`
  runs Delta optimize/compact + vacuum (weekly cron).

### 4.2 Layout

```
{BLS_STORE_URI}/
  ces/observations/        # one Delta table per program, partitioned by release_date
  sae/observations/
  …
  qcew/observations/
  cps/metadata/series/     # CPS dimension tables (bls-stats.md §2.5)
  cps/metadata/mappings/{name}/
  state/ledger/            # small Delta tables: transactional appends
  state/release_calendar/
```

One Delta table per program (schemas differ; no unified mega-table). A release event writes
only new files under its commit — existing objects are never rewritten.

### 4.3 Vintage columns (appended to every program's native columns)

| Column | Type | Semantics |
|---|---|---|
| `ref_date` | `Date` | canonical period date (`bls-stats.md` §4 rules) |
| `release_date` | `Date` | which BLS release produced this row — physical vintage, partition key |
| `revision` | `Int16`, nullable | routine print number (§2.1) |
| `benchmark` | `Int16`, nullable | benchmark counter (§2.1) |
| `source` | `Utf8` | `backfill` \| `increment` |
| `downloaded` | `Datetime` (UTC) | wall-clock ingestion time (injected clock, never `datetime.now()` inline) |

**Backfill honesty rule:** stage-1 rows get `release_date` = snapshot date and `revision` /
`benchmark` = **null** — print history that was never observed is not fabricated.

Candidate key (uniqueness enforced by tests and the §7 presence check):
`(program, series_id-or-equivalent, ref_date, release_date)`.

### 4.4 Canonical read patterns (shipped as library helpers)

- **latest:** max `release_date` per (`series_id`, `ref_date`).
- **as-of D:** filter `release_date <= D`, then latest. **Never returns a row whose
  `release_date` is after D** — the no-future-leakage guarantee (tested, §9).
- **specific print:** filter on `revision` / `benchmark`.

### 4.5 State tables

- **`state/ledger`** — one row per committed unit: `program`, `ref_date`, `release_date`,
  `revision`, `benchmark`, `source`, `row_count`, `ingested_at` (UTC). Also carries
  **`status`**: `ingested` | `missed` (§5.3) | `deferred` (§7 validation gate).
- **`state/release_calendar`** — `program`, `ref_date`, `release_date`, `original_release`
  (pre-revision date, null if unchanged), **`is_benchmark`**. Built by scrape, kept current by
  poll (§5.4). `null` `release_date` = cancelled release.

---

## 5. Release detection & revision profiles

### 5.1 Daily flow

```
poll feeds → parse to Release(program, release_date, ref_date, is_benchmark)
          → anti-join ledger → new events (oldest first)
          → profiles.expand(event) → fetch plan → engines (§6)
```

### 5.2 Empirical feed facts (verified live 2026-07-04; encode in parser + fixtures)

- Feeds are **Atom 1.0** despite the `.rss` extension — parse with the Atom namespace. Fields:
  `title`, `link` (href), `id`, `content`, `published`, `updated`. No
  `description`/`pubDate`/`guid`.
- Each feed retains **12 entries** (~1 year for monthly programs). The daily cron plus ledger
  anti-join makes this ample; a >11-month outage loses detection (and the prints were
  unrecoverable anyway, §2.2).
- **Stable identity key is the link href** (`…/archives/{feed}_{MMDDYYYY}.htm`, release date
  embedded). The Atom `id` is **not** stable (observed edited in place on `cewbd`).
- Titles/content give the reference **month but never the year** → infer year as the most
  recent occurrence of that month strictly before `published` (safe under shutdown lags of
  2+ months, observed autumn 2025).
- **No benchmark wording appears in entries** → benchmark detection must be structural:
  `benchmark_rule` on the profile (e.g. CES: reference month == January). Feed text serves
  only as a corroborating signal; disagreement logs a warning.
- Timestamp quirks: newest entry carries a pre-embargo post time later normalized to the
  scheduled 08:30/10:00 ET; `cewqtr`/`ocwage` stamp ET as `Z`. Parse dates from the archive
  link, not from timestamps, whenever precision matters.
- Calendar gaps are real (shutdowns): missing entries must be tolerated, not treated as parse
  errors.
- CES and CPS share the Employment Situation feed (`empsit`); one entry fans out to two
  program events.

### 5.3 Revision profiles

Registry data, one per program:

```python
RevisionProfile(
    routine_slots=3,            # CES: release carries prints for t, t-1, t-2
    benchmark_rule="jan_data",  # structural rule; None for oews/ep
    benchmark_window_years=5,   # re-snapshot window at benchmark events
    benchmark_source="bulk",    # benchmarks always come from the flat file
)
```

`expand()` maps an event to `[(ref_date, revision, benchmark)]` slots; benchmark events add
the windowed re-snapshot. **Missed prints:** the calendar gives expected releases; `gaps`
compares expected vs ledger; a print whose live-vintage window has passed is recorded in the
ledger with `status='missed'` — a permanent, explicit gap. Current values are ingested only
under the *current* release's correct slots.

### 5.4 Calendar subsystem

Retained from `bls-stats.md` §5: archive-page scrape (full history) + schedule-page scrape
(upcoming) + government-lapse revision overlay; feed poll for cheap refresh; dedupe on
`(program, release_date)`. Per-program tolerance for missing sources (QCEW's schedule page
404s → skip + warn). `filter_published` gates the backfill against this table.

---

## 6. Fetch paths

### 6.1 Decision (recorded): flat-file-primary, API-as-utility

The BLS API v2 **cannot** carry full-universe daily increments on one registered key
(500 queries/day, 50 series/query, 50 req/10s; one query returns all periods in a ≤20-year
window, so queries scale with series count only). Verified series counts and query needs
(2026-07-04):

| Program | Series (catalog) | Queries/release-day | Fits 500/day? |
|---|---|---|---|
| `jolts` | 2,060 (989 active) | 20–42 | yes, trivially |
| `ces` | 22,049 | 441 | alone, at 88% |
| `sae` | 22,927 | 459 | alone, at 92% |
| `bed` | 34,464 | 690 | **no** |
| `cps` | 68,630 (40,002 active) | 801–1,373 | **no** |
| **Employment Situation morning (`ces`+`cps` together)** | | **≥ 841** | **no — decisive** |

Meanwhile the flat files are re-stamped **at the embargo minute** (verified to the minute on
all five LABSTAT programs: 08:30 ET for `ce`/`ln`, 10:00 ET for `jt`/`sm`/`bd`), making one
HTTP GET the exact same-morning vintage. `Last-Modified` doubles as vintage verification.

The API v2 engine survives as a **utility**: targeted series pulls, `latest=true` probes,
spot-check validation of ingested values, and catalog lookups. It uses `BLS_API_KEY`,
enforces the 50-req/10s cap internally, and **must check the response `message` array** —
BLS returns errors as HTTP 200 + `"status": "REQUEST_SUCCEEDED"` with the failure in
`message` (verified live). Keys expire annually; `doctor` warns.

### 6.2 Per-program source table (registry data; sizes observed 2026-07-04)

| Program | Backfill source | Routine increment | Benchmark snapshot |
|---|---|---|---|
| `ces` | `ce.data.0.AllCESSeries` (333 MB, 1939→) | same file (no `.Current` exists) | same file, 5-yr window |
| `cps` | `ln.data.1.AllData` (371 MB, 1948→) | same file (no `.Current` exists) | same file |
| `sae` | `sm.data.1.AllData` (517 MB) | `sm.data.0.Current` (313 MB, 2006→) | `sm.data.0.Current` (window ⊂ coverage) |
| `jolts` | `jt.data.1.AllItems` (33 MB) | `jt.data.0.Current` (20 MB, 2011→) | `jt.data.0.Current` |
| `bed` | `bd.data.0.Current` (196 MB, 2000→) | `bd.data.0.Current` | `bd.data.0.Current` |
| `qcew` | per-year ZIPs (`bls-stats.md` §2.2), streamed | re-download touched year ZIP(s), filter to touched quarters | annual finalization = touched-year re-pull |
| `oews` | per-year workbooks (`bls-stats.md` §2.3) | new workbook | n/a |
| `ep` | scrape (`bls-stats.md` §2.4), cached | scheduled/on-demand re-scrape, full replace | n/a |

`.Current` files are truncated trailing windows — every benchmark window (≤5 yr) fits inside
every observed `.Current` coverage (15–20 yr), so the 517 MB `sm.data.1.AllData` is needed
only at backfill. Parser gotchas pinned by fixtures: `sm` files include `M13` rows (drop, per
contract); SM's datatype mapping file is `sm.data_type` (underscore).

### 6.3 Increment mechanics

1. Download the program's designated file **once per release event**.
2. **Stale-file guard:** verify `Last-Modified` ≥ the detected release timestamp; if the file
   hasn't flipped, bounded retries with backoff, then loud non-zero exit.
3. Stream-filter to the plan's `ref_date`s (Polars lazy scan; peak RSS target < 8 GB).
4. Stamp vintage columns; hand one frame per event to the pipeline.

---

## 7. Orchestration, crash-safety, error handling

### 7.1 Pipeline per event

```
expand → fetch → validate → commit (Delta, atomic; skipped on --dry-run) → record (ledger)
```

### 7.2 Crash safety

Order is **commit-data, then record-ledger**; the only crash-inconsistent state possible is
"data committed, ledger missing" — the safe direction. **Idempotent commit via presence
check:** before appending, query the target table for rows matching the event's exact vintage
key (`program, ref_date, release_date, revision, benchmark`); if present, skip the append and
repair the ledger row (logged as a crash-repair branch). Re-running always converges; a dumb
daily cron is the whole scheduler. (A Delta merge/upsert was rejected: heavier file rewrites,
and it hides whether a re-run was a repair.)

### 7.3 Validation gates (fail the event, not the run)

Pre-commit, each frame must pass:
1. **Schema match** — exact column names/dtypes vs the program's contract, including string
   locks on `series_id`, `footnote_codes`, `area_fips`.
2. **Non-empty** — an empty slice for a detected release means data lags the announcement →
   record `status='deferred'`, retry next run; never commit empty.
3. **Sanity bands** — row count within a configured band of the prior vintage; `value`
   null-rate under threshold. Catches truncated/malformed downloads.

Failures raise a typed `ValidationError`, fail *that event*, and continue.

### 7.4 Failure isolation, exit codes, logging

- Per-event `try` isolation; HTTP layer: **4xx fail fast** (log the offending URL), **5xx /
  transport retry with backoff**; generous timeouts for 300+ MB files.
- Exit codes: `0` success or nothing new; `1` partial failure; `2` total failure.
- Structured stderr logging with per-event context (`program`, `ref_date`, `release_date`,
  slot, row counts: fetched / kept / committed / skipped-as-duplicate). Every skipped or
  failed sub-unit logs at WARNING+. Nothing fails silently.

---

## 8. CLI surface (typer)

Thin adapters only; global options (`--store-uri`, `--log-level`, `--dry-run`) via callback
with env-var fallback.

| Command | Purpose |
|---|---|
| `bls-stats backfill --program ces --start 2010/01 --end 2026/06` (or `--all`) | Stage 1. `reference_periods` → `filter_published` (errors if calendar absent — bootstrap order) → fetch → commit seed vintage. Streams per year where applicable. |
| `bls-stats ingest [--program X] [--dry-run]` | Stage 2, **the one crontab line**. Full §7 pipeline; exit codes cron-friendly. |
| `bls-stats calendar build` / `refresh` / `show --program jolts` | Full scrape / cheap poll / inspect release-date table. |
| `bls-stats gaps [--program X]` | Expected vs ledger; lists missing periods + `missed` prints. Exit 0 = clean (doubles as monitoring probe). |
| `bls-stats store info` / `maintain` / `query --program ces --ref-date 2026-06-12 [--as-of D]` | Inspect tables; optimize+vacuum; vintage-aware reads (`--as-of` = point-in-time). |
| `bls-stats metadata fetch` / `export` / `enrich` | CPS dimension tables: download+cache with integrity manifest / push to store / label-join demo. |
| `bls-stats doctor` | Pre-flight checklist: store reachability, **conditional-PUT probe** (selects Delta commit-safety mode), delta-rs availability, BLS reachability with configured UA, presence/validity of `BLS_CONTACT_EMAIL`, `BLS_API_KEY`, creds. |

---

## 9. Testing strategy

Default run is **offline and fast** (`addopts = -m "not network and not slow"`).

- **Markers:** unmarked (pure/fast, the bulk), `network` (live BLS canaries, nightly/manual),
  `slow` (full-size files, Delta round-trips), `real_store` (MinIO/S3; skipped without creds).
- **Recorded fixtures over mocks:** committed, trimmed real payloads — flat-file excerpts for
  all five LABSTAT programs (locking M13 exclusion, string locks, `ref_date` derivation),
  QCEW singlefile+by-size ZIP pair, one OEWS workbook, archive/schedule/lapse HTML, and the
  **awkward real Atom feeds** found in research: the shutdown-gap `empsit`, the edited-in-place
  `cewbd` entry, the mislabeled-UTC timestamps.
- **Invariants over golden values:** exact schema; candidate-key uniqueness; row-count bands;
  null-rate ceilings; no `M13` leakage; monthly `ref_date` on the 12th, JOLTS on the last
  business day.
- **Vintage suite (highest value):** (1) synthetic release sequences produce the right
  `(revision, benchmark)` tuples; (2) benchmark events append windowed snapshots without
  touching prior vintages; (3) **as-of queries never leak a `release_date` after the as-of
  date** — the guarantee the whole store exists for; (4) missed prints record `missed` and are
  never backfilled with later values.
- **Crash-safety suite:** kill between commit and record → re-run repairs ledger without
  duplicate append; kill mid-append → no partial vintage visible (Delta log).
- **Determinism:** injected clock (no inline `datetime.now()`), seeded/injected jitter and
  ordering.

---

## 10. Configuration & cross-cutting

Loaded from the environment via python-dotenv (**`.project.env`**, explicit name):

| Variable | Default | Controls |
|---|---|---|
| `BLS_STORE_URI` | `./data/store` | store root; dev: `s3://bls-stats/store` on local MinIO; deployment: corporate S3 URI |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_ENDPOINT_URL` | none | standard AWS-style creds; MinIO ↔ corporate endpoint differ by one variable |
| `BLS_API_KEY` | none | API v2 utility engine |
| `BLS_CONTACT_EMAIL` | `research@example.com` + startup warning | User-Agent contact |
| `HTTPS_PROXY` / `HTTP_PROXY`, `SSL_CERT_FILE` | none / system | corporate proxy & CA (both cases checked) |
| `BLS_LOG_LEVEL` | `INFO` | stderr verbosity |

- **HTTP:** one client, one policy — UA `bls-stats/<version> (<contact>)`; 4xx fast-fail;
  5xx/transport backoff retries; long timeouts; throttled scrapes; API rate cap enforced.
- **Memory/disk:** streaming/lazy scans everywhere; QCEW strictly one year at a time; peak RSS
  target < 8 GB; downloads to scratch, deleted after commit; nothing durable local.
- **Dependencies (deliberately short):** `polars`, `deltalake`, `httpx`, `typer`,
  `python-dotenv`, `lxml` + `beautifulsoup4`, `fastexcel`. Atom parsing via stdlib
  `xml.etree` (no feedparser).
- **Hygiene:** `.gitignore` covers `.env` and `.project.env` from the first commit; no
  employer names anywhere.

---

## 11. Success criteria

1. **Behavioral fidelity:** for the eight in-scope products, `bls-stats.md` §10 criteria 1–3
   hold (exact sources, columns, `ref_date` rules including JOLTS last-business-day and QCEW
   `area_fips` as `Utf8`; period generation; calendar build by scrape and poll; gap
   detection).
2. **Vintage correctness:** replaying a release sequence yields the §2.1 tuples; benchmark
   events produce windowed `benchmark+1` snapshots; **as-of queries never leak future
   vintages** (automated test).
3. **Crash safety:** re-running after any simulated crash point converges with no duplicate
   rows and no phantom ledger entries; exit codes distinguish partial vs total failure.
4. **Increment freshness:** on a release morning, `ingest` detects via feed, verifies
   `Last-Modified`, and commits the release's prints the same day — without exceeding any BLS
   quota (zero API queries on the bulk path).
5. **Environment fit:** backfill of the largest program completes within the 4-core/25 GB
   container with peak RSS < 8 GB; `doctor` passes on a fresh container before first use.

---

## 12. Open items (not blocking implementation start)

1. Run `doctor` (incl. conditional-PUT probe) against the **corporate** S3 endpoint on first
   deploy; select Delta commit-safety mode accordingly.
2. Create the dedicated `bls-stats` MinIO bucket for dev (one-liner, part of project setup).
3. Empirically verify the QCEW routine print count (§2.1) during implementation; adjust its
   `RevisionProfile` config (not code).
4. OEWS/EP feed cadence is slow (`ocwage` retains ~12 years); confirm their profiles treat
   each release as a fresh vintage with no routine slots.
