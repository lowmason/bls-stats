# bls-stats

**Vintage-aware ingest for U.S. Bureau of Labor Statistics data.**

`bls-stats` downloads eight BLS products and lands every *print* — not just the latest number —
in a Delta Lake vintage store on an S3-compatible object store (or a local path). That means you
can ask *"what did BLS say on date D?"* and get the answer BLS actually published then. Revisions
and annual benchmarks are recorded as distinct vintages rather than overwriting history, which
makes as-of and point-in-time reads exact.

## Why vintages?

Most BLS series are revised repeatedly after first publication. CES March 2026 employment, for
example, is published four times:

| Published on | What it is | `revision` | `benchmark` |
|---|---|---|---|
| 2026-04-03 | preliminary print | 0 | 0 |
| 2026-05-08 | second print | 1 | 0 |
| 2026-06-05 | third print | 2 | 0 |
| 2027-02-05 | annual benchmark | 2 | 1 |

A store that keeps only the latest value silently rewrites history — any backtest or nowcast
trained on it sees data that did not exist at prediction time. `bls-stats` keeps all four rows,
each stamped with its `release_date`, so [`as_of`][bls_stats.storage.reads.as_of] reads never leak
the future. See [The vintage model](concepts/vintage-model.md) for the full story.

## Programs covered

| Program | What it measures | Frequency |
|---|---|---|
| **CES** | National nonfarm payroll jobs, hours, earnings | Monthly |
| **SAE** | State & metro payroll jobs, hours, earnings | Monthly |
| **JOLTS** | Job openings, hires, separations | Monthly |
| **CPS** | Household employment, unemployment, labor force | Monthly |
| **BED** | Gross job gains/losses, establishment births/deaths | Quarterly |
| **QCEW** | Near-census covered employment and wages | Quarterly |
| **OEWS** | Occupational employment & wages by SOC | Annual (May) |
| **EP** | Employment Projections industry–occupation matrix | Irregular |

!!! note "EP status"
    The EP engine (scrape + cache) is implemented, but EP is not yet wired into the vintage
    store — `ingest`/`backfill` for `ep` exit with code 2 and an explanatory error. The planned
    long-format integration is tracked in the repository's `specs/deferred_items.md`.

## How it works

```text
release feeds ──▶ detect ──▶ expand slots ──▶ fetch ──▶ validate ──▶ commit ──▶ record
 (Atom/RSS)      (poll)     (which prints     (bulk     (schema,     (Delta     (slot
                             this release      files)    row band)    append)    ledger)
                             carries)
```

- **Release detection** polls the BLS Atom feeds and maintains a scraped release-date calendar —
  see [Release detection & deferral](concepts/release-detection.md).
- **Slot expansion** turns one release into the exact set of `(ref_date, revision, benchmark)`
  prints it carries, including annual benchmark windows.
- **Validation** gates every frame on schema, null rate, and a row-count sanity band before commit.
- **Storage** is append-only Delta with crash-safe commit-then-record ordering and
  concurrent-writer-safe conditional PUT — see [Storage & crash safety](concepts/storage.md).
- A **slot ledger** records `ingested` / `deferred` / `missed` per print; missed prints are
  permanent, never silently backfilled — that's what keeps historical reconstructions honest.

## Design principles

- **Polars only** — lazy/streaming scans keep peak memory well under the 8 GB design target;
  QCEW is processed strictly one year at a time.
- **Strings stay strings** — `series_id`, `footnote_codes`, and every code column are `Utf8`
  with leading zeros preserved. `ref_date` is `Date`; timestamps are `Datetime("us", "UTC")`.
- **Injected clocks** — pipeline logic never calls `datetime.now()`; time is a parameter, so the
  entire ingest sequence is replayable in tests.
- **Offline by default** — the standard test run touches no network; live-network and
  live-store tests are opt-in markers.

## Where to next

- [Getting started](getting-started.md) — install, configure, bootstrap, schedule.
- [Concepts](concepts/vintage-model.md) — the vintage model, release detection, storage, pipeline.
- [CLI reference](cli.md) — every command with examples and exit codes.
- [API reference](reference/index.md) — the full module-level API, generated from docstrings.
