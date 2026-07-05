# CLI reference

All commands run through the `bls-stats` console script (`uv run bls-stats …`). The CLI is a thin
typer adapter — logic lives in [`pipeline`](reference/pipeline.md),
[`releases`](reference/releases.md), and [`storage`](reference/storage.md) — so anything the CLI
does is also scriptable from Python.

| Command | Purpose | Cadence |
|---|---|---|
| [`ingest`](#ingest) | Incremental ingest of newly detected releases | Daily (cron) |
| [`backfill`](#backfill) | Seed history for one program from bulk files | Once per program |
| [`calendar build`](#calendar-build) | Full archive + schedule scrape | Once, then rarely |
| [`calendar refresh`](#calendar-refresh) | Cheap keep-current poll from the feeds | Optional |
| [`calendar show`](#calendar-show) | Print one program's calendar | Ad hoc |
| [`gaps`](#gaps) | Audit ledger against calendar | Ad hoc / monitoring |
| [`store info`](#store-info) | Row counts and vintage range per program | Ad hoc |
| [`store maintain`](#store-maintain) | Delta compaction + vacuum | Weekly (cron) |
| [`store query`](#store-query) | Latest / as-of / all-vintages reads | Ad hoc |
| [`metadata fetch`](#metadata-fetch) | Download CPS dimension tables | Occasional |
| [`metadata export`](#metadata-export) | Snapshot CPS metadata into the store | Occasional |
| [`metadata enrich`](#metadata-enrich) | Spot-check an enriched CPS slice | Ad hoc |
| [`doctor`](#doctor) | Pre-flight environment / store / BLS probes | Before first use; after env changes |

## ingest

```bash
bls-stats ingest [--program ces] [--dry-run]
```

The one daily crontab line. Polls the release feeds, expands each new release into slots, and
ingests them. Without `--program` it covers all feed-driven programs (everything except `ep`).
`--dry-run` walks the full detect/fetch/validate path but commits nothing.

**Exit codes:** `0` success or deferrals-only · `1` partial failure · `2` total failure — see
[the exit-code contract](concepts/pipeline.md#exit-codes).

## backfill

```bash
bls-stats backfill --program jolts --start 2015/01 --end 2026/05 [--dry-run]
```

Seeds history from the program's bulk file, stamped as snapshot vintages
([why](concepts/vintage-model.md#backfill-vintages-vs-live-vintages)). Period grammar follows the
program frequency: `YYYY/MM` (monthly), `YYYY/Q` (quarterly, e.g. `2024/2`), `YYYY` (annual).
Requires a built [calendar](#calendar-build). QCEW automatically splits multi-year ranges into
one-year runs for memory discipline. Re-runs skip already-ingested periods.

**Exit codes:** `0` success or nothing to do · `2` invalid range, missing calendar, or fetch
failure.

## calendar

### calendar build

```bash
bls-stats calendar build
```

Scrapes every program's news-release archive and schedule pages, applies the lapse overlay
(reschedules keep their original date; cancellations get a null release date), and appends the
result to the `release_calendar` state table. Throttled; a page that fails to parse is warned and
skipped.

### calendar refresh

```bash
bls-stats calendar refresh
```

Appends the releases currently visible in the Atom feeds — a cheap way to keep the calendar
current between full builds.

### calendar show

```bash
bls-stats calendar show --program ces
```

Prints one program's calendar, sorted by reference date.

## gaps

```bash
bls-stats gaps [--program ces] [--strict]
```

Audits the slot ledger against the calendar. A calendar entry with no ledger record at all is an
**unexplained gap** — something happened that the pipeline never even saw. Recorded `deferred` and
`missed` slots are *acknowledged* (the pipeline knows about them) and don't fail the audit unless
`--strict`, which also treats `missed` prints as failures.

**Exit codes:** `0` clean · `1` unexplained gaps (or, with `--strict`, missed prints).

## store

### store info

```bash
bls-stats store info [--program ces]
```

Row count plus first/latest vintage per program table; `(empty)` for programs with no data yet.

### store maintain

```bash
bls-stats store maintain
```

The weekly crontab line: Delta `optimize.compact()` and a 7-day-retention `vacuum` on every
non-empty program table.

### store query

```bash
# latest view
bls-stats store query --program ces --ref-date 2026-03-12

# point-in-time view (never returns anything published after --as-of)
bls-stats store query --program ces --ref-date 2026-03-12 --as-of 2026-04-15

# the full print history
bls-stats store query --program ces --ref-date 2026-03-12 --all-vintages
```

The three [canonical reads](concepts/vintage-model.md#reading-it-back) for one reference date,
from the shell.

## metadata

CPS observations are bare `series_id`s; the `ln.*` mapping tables decode them
([`bls_stats.enrich.cps`](reference/enrich.md)).

### metadata fetch

```bash
bls-stats metadata fetch [--refresh]
```

Downloads the CPS series catalog and every mapping table into `data/cps_metadata/`, keeping a
sha256 manifest so unchanged files are skipped (`--refresh` forces re-download).

### metadata export

```bash
bls-stats metadata export
```

Snapshots the fetched metadata into Delta tables in the store (`cps/metadata/…`,
overwrite-on-export).

### metadata enrich

```bash
bls-stats metadata enrich --ref-date 2026-06-12
```

Prints one CPS slice with all dimension text joined on — a spot-check view. Enrichment never
changes the row count.

## doctor

```bash
bls-stats doctor
```

Pre-flight probes, printed as a ✓/✗ checklist:

- environment: `BLS_CONTACT_EMAIL` set, store URI shape, API-key presence (warnings, not failures)
- deltalake: package importable (version reported)
- store: reachability of `BLS_STORE_URI`
- conditional PUT: the [412 probe](concepts/storage.md#concurrent-writers-conditional-put) that
  proves concurrent-writer safety (advises `BLS_S3_UNSAFE_RENAME` if unsupported)
- BLS: endpoint reachability

**Exit codes:** `0` all green · `1` any red line.
