# bls-stats — Recreation Specification

> **Purpose of this document.** This is a *behavioral* specification. It describes **what** `bls-stats`
> does — its inputs, outputs, external data contracts, and observable behavior — so that a capable agent
> can rebuild an equivalent system from scratch. It deliberately says little about **how** the current
> code is structured internally (module layout, class design, private helpers). Where an exact detail is a
> genuine *contract* (a URL, a date rule, an output schema) it is pinned down verbatim; where the detail is
> voluminous or authoritative elsewhere, this doc describes the behavior and **points** to the source of
> truth (a BLS page or the current source file) rather than reproducing it.
>
> **Fixed constraints for a recreation.** The reimplementation **must** be a Python library **and** CLI,
> and **must** use **Polars** for all tabular data (no pandas). Every download/enrichment function returns a
> `polars.DataFrame` (or yields them). Beyond that, internal architecture is free to change.
>
> **What this is not.** Not a port guide, not an API reference of the current code. If you find yourself
> copying private function names, you've gone too deep — recreate the *contract*, not the implementation.

---

## 1. What the system is

`bls-stats` downloads U.S. Bureau of Labor Statistics (BLS) employment, wage, and labor-turnover data from
public bulk-download servers (**no API key required**), tracks **when** BLS publishes each dataset, and
pushes normalized Parquet to an S3-compatible object store.

It has three cooperating capabilities:

1. **Download** — for a chosen program and date range, fetch the raw BLS (or Census) files, normalize them
   into a tidy Polars DataFrame with a canonical `ref_date`, and (optionally) export partitioned Parquet to
   object storage.
2. **Release-date tracking** — discover the calendar of BLS publication dates (historical + upcoming, plus
   government-shutdown/lapse revisions) so downloads only request periods that have actually been published.
3. **Ingest** — poll for newly published releases, download only what's new, push to storage, and record
   what was done so the next run doesn't repeat it.

The unit that ties everything together is a **program** (a BLS survey) and a **reference period**
(the month/quarter/year the data describes), which every program maps to a single canonical **`ref_date`**.

---

## 2. Programs and their data contracts

The system covers **ten** data products. (Note: the repo's top-level `CLAUDE.md` predates several of these
and lists only seven — treat this table as authoritative for a recreation.)

Each program is identified by a short name (used on the CLI and as a storage prefix) and, for series-based
programs, a two-letter BLS **series-ID prefix**.

### 2.1 Series-based bulk flat-file programs

These fetch a single tab-delimited "time series" file from `download.bls.gov`, filter to the requested
reference periods, and attach a `ref_date`. They share an output shape.

| Short name | Series prefix | Program | Source file (verbatim) | Frequency |
|---|---|---|---|---|
| `ces` | `CE` | Current Employment Statistics (national) | `https://download.bls.gov/pub/time.series/ce/ce.data.0.AllCESSeries` | monthly |
| `sae` | `SM` | State & Area Employment | `https://download.bls.gov/pub/time.series/sm/sm.data.0.Current` | monthly |
| `jolts` | `JT` | Job Openings & Labor Turnover Survey | `https://download.bls.gov/pub/time.series/jt/jt.data.0.Current` | monthly |
| `cps` | `LN` | Current Population Survey (labor force, "LN") | `https://download.bls.gov/pub/time.series/ln/ln.data.1.AllData` | monthly |
| `bed` | `BD` | Business Employment Dynamics | `https://download.bls.gov/pub/time.series/bd/bd.data.0.Current` | quarterly |

**Input contract:** a list of `(year, period_number)` tuples — month `1–12` for monthly programs, quarter
`1–4` for quarterly. (Generate these with the period logic in §3; don't hand-build them.)

**Output DataFrame contract (all five):**
- All native columns from the BLS flat file — at minimum `series_id`, `value`, `footnote_codes`.
- **`ref_date`** (Polars `Date`): the canonical date for the reference period (§4).
- **`downloaded`** (Polars `Datetime`): ingestion timestamp.
- The raw `year` / `period` columns are **dropped** (superseded by `ref_date`).
- `series_id`, `value`, `footnote_codes` are whitespace-trimmed and typed as strings; `footnote_codes`
  must tolerate mixed content (force to Utf8).
- BLS period code **`M13`** (the annual average) is **excluded**; only `M01–M12` become observations.
  Quarterly programs keep only `Q01–Q04`.

**Series-ID structure (for downstream parsing / validation).** Each series prefix has a fixed positional
layout. The recreation should expose this as a small registry (prefix → ordered fixed-width fields) so a
`series_id` can be decoded. The current layouts:

| Prefix | Total len | Ordered fields (name : width) |
|---|---|---|
| `CE` | 13 | prefix:2, seasonal:1, supersector:2, industry:6, data_type:2 |
| `SM` | 20 | prefix:2, seasonal:1, state:2, area:5, supersector:2, industry:6, data_type:2 |
| `BD` | 28 | prefix:2, seasonal:1, area_code:10, industry_code:6, unit_analysis:1, data_element:1, size_class:2, data_class:2, rate_level:1, record_type:1, ownership:1 |
| `JT` | 21 | prefix:2, seasonal:1, industry_code:6, state_code:2, area_code:5, size_class:2, data_element:2, rate_level:1 |
| `LN` | 11 | prefix:2, seasonal:1, series_code:8 |
| `OE` | 26 | prefix:2, seasonal:1, state_code:2, area_code:7, industry_code:6, occupation_code:6, datatype_code:2 |
| `EP` | 15 | prefix:2, seasonal:1, occupation_code:6, industry_code:6 |

> Pointer: the authoritative field breakdowns live in each program's `*.series` documentation under
> `https://download.bls.gov/pub/time.series/<prefix>/`. Verify widths there rather than trusting this table
> blindly if BLS revises a layout.

### 2.2 QCEW — quarterly bulk ZIPs

- **Short name:** `qcew`. No series IDs — this is structured establishment/wage data.
- **Input:** `(year, quarter)` tuples.
- **Source files (verbatim), per year:**
  - `https://data.bls.gov/cew/data/files/{year}/csv/{year}_qtrly_singlefile.zip`
  - `https://data.bls.gov/cew/data/files/{year}/csv/{year}_q1_by_size.zip` (only when size detail requested)
- **Behavior:** download per-year ZIP(s), extract the CSV(s), filter to requested `(year, qtr)` pairs,
  deduplicate rows (the singlefile carries aggregate `size_code == 0` rows; the by-size file carries
  `size_code > 0`), attach `ref_date`. Process **one year at a time** (memory-efficient) — the recreation
  should support streaming/iterating per year, not loading all years at once.
- **Output columns (contract):** `area_fips`, `own_code`, `industry_code`, `agglvl_code`, `size_code`,
  `disclosure_code`, `qtrly_estabs`, `month1_emplvl`, `month2_emplvl`, `month3_emplvl`,
  `total_qtrly_wages`, `taxable_qtrly_wages`, `qtrly_contributions`, `avg_wkly_wage`, plus `ref_date`
  (`Date`) and `downloaded` (`Datetime`). Raw `year`/`qtr` dropped.
- **Critical gotcha:** **`area_fips` must be kept as a string (`Utf8`)** — it mixes numeric county codes
  (`01001`) with alpha MSA codes (`C1010`); inferring it numeric corrupts the data.

### 2.3 OEWS — annual occupational wage workbook

- **Short name:** `oews`. Series prefix `OE` (but delivered as a wide Excel workbook, not the time-series file).
- **Input:** `(year, 1)` tuples (annual; period sentinel is always `1`).
- **Source file (verbatim):** `https://www.bls.gov/oes/special-requests/oesm{yy}all.zip` where `{yy}` is the
  2-digit year. The ZIP contains one `.xlsx` workbook; the relevant sheet is named `All May <YYYY> data`.
- **Output:** every column from the sheet (names lowercased, trimmed) plus `ref_date` and `downloaded`.
- **`ref_date` rule:** **May 12 of the reference year** (OEWS survey reference month).

### 2.4 Employment Projections (EP) — scraped HTML matrix

- **Short name:** `ep`. Not periodic — a single national industry–occupation projection matrix.
- **Sources (verbatim):**
  - Index of occupations: `https://www.bls.gov/emp/tables/industry-occupation-matrix-occupation.htm`
  - Per-occupation table: `https://data.bls.gov/projections/nationalMatrix?queryParams={soc}&ioType=o`
- **Behavior:** scrape the index for every occupation SOC code, then fetch and parse each occupation's HTML
  projection table (be polite — throttle between requests; log and continue on individual failures, error
  only if *all* fail). Normalize year-specific headers (e.g. `2023 Employment`, `Projected 2033 Employment`)
  to stable names like `base_year_employment` / `projected_year_employment`.
- **Output columns (contract):** `occupation_code`, `occupation_title`, `occupation_type`, `industry_title`,
  `industry_code`, `industry_type`, `base_year_employment`, `base_year_pct_of_occupation`,
  `base_year_pct_of_industry`, `projected_year_employment`, `projected_year_pct_of_occupation`,
  `projected_year_pct_of_industry`, `employment_change`, `employment_pct_change`, `industry_sort`,
  `display_level`, plus a `downloaded_at` timestamp. Numeric fields parsed to floats (strip commas, treat en-dash as null).
- Because scraping is slow, results should be **cached to Parquet** and re-read unless a `refresh` is requested.

### 2.5 CPS metadata (dimension tables)

Separate from the CPS observation download. The recreation should be able to fetch and expose the CPS
**series catalog** and its **code→label mapping tables**, and to enrich raw CPS observations with them.

- **Sources (verbatim base):** `https://download.bls.gov/pub/time.series/ln/`
  - Series catalog: `ln.series`
  - ~36 mapping files: `ln.ages`, `ln.occupation`, `ln.indy`, `ln.race`, `ln.sexs`, `ln.education`,
    `ln.class`, `ln.lfst`, `ln.seasonal`, `ln.footnote`, `ln.periodicity`, … (full list is the set of
    `ln.*` files at that URL — treat the directory listing as source of truth).
- **Enrichment contract:** given a raw CPS observation DataFrame, left-join the series catalog and all
  mapping tables onto it (turning coded fields into human-readable labels) and resolve `footnote_codes`
  into readable footnote text. Joins must be left-joins (never drop observations).
- **Metadata export contract:** write the series catalog and each mapping to storage as separate Parquet
  objects under `cps/metadata/series/…` and `cps/metadata/mappings/{name}/…`, each tagged with a
  `program='cps'` column.
- Downloaded metadata files should be **cached locally with an integrity manifest** (e.g. checksum per
  file) so re-runs skip unchanged files unless a refresh is forced.

---

## 3. Reference-period generation (the shared input contract)

Every download takes `list[(int, int)]`. These are produced from human date strings by a single period
function so the whole system speaks one dialect.

- **`reference_periods(program, start, end) -> list[(year, period_number)]`**
  - Program frequency determines parsing and output:
    - **Monthly** (`ces`, `cps`, `sae`, `jolts`): parse `YYYY/MM`; return `(year, month)`, month `1–12`.
    - **Quarterly** (`qcew`, `bed`): parse `YYYY/Q` (or `YYYY/0Q`); return `(year, quarter)`, quarter `1–4`.
    - **Annual** (`oews`, `ep`): parse `YYYY`; return `(year, 1)`.
  - `start` and `end` are **inclusive**. Invalid program, malformed period, or `start > end` → error.
- **`filter_published(program, periods, release_dates_path)`** — drop any period whose canonical `ref_date`
  is **not** present in the release-date table (§5). This is what prevents requesting an unpublished month.
  Requires the release-date Parquet to exist first (**bootstrap it before using this**, or it errors).

Recreation note: `filter_published` maps each period to its `ref_date` using the **same** rules as §4
(quarter→quarter-end-month-12th, annual→May-12th, monthly→that-month-12th) before checking membership.

---

## 4. The `ref_date` rule (canonical date per program)

Every observation is stamped with one `ref_date`. This is the join key across the whole system and the
storage partition key — getting it right is essential.

| Program(s) | `ref_date` rule |
|---|---|
| `ces`, `sae`, `cps` (monthly), CPS-PUMS, CPS-telework | **12th** of the reference month |
| `jolts` | **Last business day (Mon–Fri)** of the reference month — *not* the 12th |
| `bed`, `qcew` (quarterly) | **12th** of the quarter's last month (Q1→Mar 12, Q2→Jun 12, Q3→Sep 12, Q4→Dec 12) |
| `oews` (annual) | **May 12** of the reference year |
| `ep` | none (not periodic; stamped only with a download timestamp) |

---

## 5. Release-date tracking

Goal: build and maintain a table of *when BLS publishes each dataset*, so downloads/ingest can be driven by
what's actually out.

**Output table contract** (`data/release_dates.parquet`, plus in-memory DataFrames):
- `program` (short name), `ref_date` (the period the release covers), `release_date` (when BLS published it;
  `null` if a scheduled release was **cancelled**), and — for schedule revisions — `original_release` (the
  pre-revision date; `null` when unchanged).

Two acquisition modes, same output schema:

1. **Scrape** (`scrape_all`) — full history. For each program, scrape its BLS news-release **archive** page
   and current-year **schedule** page, extract each release's embargo/publish date and the reference period
   it covers, then overlay **government-lapse/shutdown revision** pages to correct or cancel affected dates.
   Dedupe on `(program, release_date)`, sort chronologically, and (optionally) write the Parquet.
2. **Feed poll** (`poll_all`) — fast, recent only. For each program, read its BLS **Atom/RSS feed** and take
   the ~dozen most recent entries (publish date + reference period). Same output columns; far cheaper; used
   for routine "what's new" checks.

**Publication registry (contract).** Each program maps to a set of BLS URLs. Verbatim, as currently used:

| Program | Archive page | Schedule page | Atom/RSS feed |
|---|---|---|---|
| `ces` | `https://www.bls.gov/bls/news-release/empsit.htm` | `https://www.bls.gov/schedule/news_release/empsit.htm` | `https://www.bls.gov/feed/empsit.rss` |
| `cps` | `https://www.bls.gov/bls/news-release/empsit.htm` | `https://www.bls.gov/schedule/news_release/empsit.htm` | `https://www.bls.gov/feed/empsit.rss` |
| `sae` | `https://www.bls.gov/bls/news-release/laus.htm` | `https://www.bls.gov/schedule/news_release/laus.htm` | `https://www.bls.gov/feed/laus.rss` |
| `jolts` | `https://www.bls.gov/bls/news-release/jolts.htm` | `https://www.bls.gov/schedule/news_release/jolts.htm` | `https://www.bls.gov/feed/jolts.rss` |
| `qcew` | `https://www.bls.gov/bls/news-release/cewqtr.htm` | `https://www.bls.gov/schedule/news_release/cewqtr.htm` ⚠️ **404** | `https://www.bls.gov/feed/cewqtr.rss` |
| `bed` | `https://www.bls.gov/bls/news-release/cewbd.htm` | `https://www.bls.gov/schedule/news_release/cewbd.htm` | `https://www.bls.gov/feed/cewbd.rss` |
| `oews` | `https://www.bls.gov/oes/release_archive.htm` | *(none)* | `https://www.bls.gov/feed/ocwage.rss` |

Lapse/disruption revision pages (verbatim):
`https://www.bls.gov/bls/2025-lapse-revised-release-dates.htm`,
`https://www.bls.gov/bls/updated_release_schedule.htm`.

> Pointer: BLS feed slugs and page URLs drift over time. Treat this registry as data (a small config table),
> not hard-coded logic, so it can be updated without touching download code. Note that CES and CPS share the
> Employment Situation release (`empsit`).
>
> **URL verification (checked 2026-07-04).** Every source URL in this document was HEAD/GET-checked and
> returns `200` **except** the QCEW schedule page `https://www.bls.gov/schedule/news_release/cewqtr.htm`,
> which returns **`404`** (its sibling `cewbd.htm` works, so this is a genuine gap, not a transient block —
> `scrape_schedule` yields nothing for QCEW). QCEW release dates are still recoverable from its archive page
> and its feed. A recreation should tolerate a missing schedule page per program (skip + warn), not assume
> all three URL types exist for every program. All BLS pages/feeds require a descriptive User-Agent; all
> Census CPS sources verified with a Census-style User-Agent.

**Gap detection** (`find_gaps`): given a release-date table, for each program enumerate the *expected*
`ref_date` sequence between the earliest and latest observed dates (monthly or quarterly cadence, using the
same `ref_date` rules) and report any missing ones. Output: `(program, ref_date)` rows; empty = no gaps.

---

## 6. Ingest (incremental publish→storage pipeline)

Goal: keep object storage up to date with the minimum work, idempotently.

**State contract** (`ingest_state.parquet`, default in CWD): one row per successfully-pushed
`(program, ref_date)`, with columns `program`, `ref_date`, `release_date`, `ingested_at` (UTC), `row_count`.

**Detect:** for each program (or one named program), poll its feed (§5) and **anti-join** against state to
find `(program, ref_date)` pairs not yet ingested; return them oldest-first.

**Pipeline order (per new release):**
1. **Download** that period via the program's downloader (§2).
2. **Push** the resulting DataFrame to storage (§7) — unless `--dry-run`.
3. **Record**: append the row to state and **atomically** rewrite the state Parquet (temp file + rename).

**Ordering invariant (important):** state is written **only after a successful push**, so a crash can never
record a push that didn't happen. Re-running simply re-detects and retries the un-recorded periods.

**Exit codes:** `0` = success or nothing new; `1` = partial failure (some programs failed); `2` = all failed.

---

## 7. Storage / export contract

TBD

---

## 8. CLI surface

TBD

---

## 9. Cross-cutting behavior (contracts, not implementation)

- **HTTP politeness & resilience.** All BLS/Census requests send a descriptive User-Agent that includes a
  contact email (BLS expects this; Census requires a non-bot UA). Requests are resilient to transient
  failures: **4xx fails fast; 5xx / transport errors retry a few times with increasing backoff**; generous
  per-request timeout (large bulk files). Recreations must not hammer servers — throttle multi-request
  scrapes (EP, per-year QCEW).
- **Configuration via environment** (with sane defaults so it runs out of the box):
  | Variable | Default | Controls |
  |---|---|---|
  | `BLS_DATA_DIR` | `data/` | local cache dir for Parquet/raw files |
  | `BLS_CONTACT_EMAIL` | `research@example.com` | contact in the HTTP User-Agent |
  | `SSL_CERT_FILE` | system | custom CA bundle |
  | `HTTPS_PROXY` / `HTTP_PROXY` | none | proxy (checked in both upper/lower case) |
  | `OS_ACCESS_KEY` / `OS_SECRET_KEY` / `OS_ENDPOINT` | none | object-store credentials |
  | `RUN_ENV` | `corp` | toggles the corporate proxy for storage |
- **Logging.** Human-readable logs to stderr; a rotating file log is also fine. Log per-program context,
  counts (rows fetched, periods skipped, new vs already-ingested), and any skipped/failed sub-requests
  (never silently swallow a failure — a skipped month or failed occupation must be logged).
- **Schema stability.** Output column names and dtypes above are a contract; keep `ref_date` as `Date`,
  timestamps as `Datetime`, `area_fips` and `footnote_codes` as strings. Prefer Parquet everywhere.

---

## 10. Recreation success criteria

A reimplementation is faithful if:

1. For each of the ten products, given a date range it fetches from the **exact source URLs** above and
   returns a Polars DataFrame with the **columns and `ref_date` rule** specified — including the tricky ones
   (JOLTS last-business-day; QCEW `area_fips` stays `Utf8`; OEWS May-12; M13 excluded; CPS occupation-column
   normalization; telework dual layout; Census WAF/HTML-retry handling).
2. `reference_periods` + `filter_published` correctly turn date strings into published `(year, period)`
   lists, and never emit an unpublished period.
3. The release-date table can be built by **both** full scrape and feed poll, produces the documented
   schema (including cancelled/revised handling), and gap detection reports missing periods.
4. Ingest is **idempotent and crash-safe**: re-running never double-pushes, and state is only recorded after
   a confirmed push; exit codes reflect partial vs total failure.
5. Exports land at the documented storage paths, and the CLI exposes the commands in §8 with those flags.

> **Verify, don't assume.** BLS/Census reorganize files and feeds periodically. Before trusting any URL in
> this doc, a recreation should confirm it resolves; treat the URL/feed/publication tables as editable
> config, and fail loudly (with the offending URL logged) when a source moves.
