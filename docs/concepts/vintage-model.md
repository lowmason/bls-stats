# The vintage model

The store's founding question is: **what did BLS say on date D?** Everything in the data model
serves an exact answer to that question.

## Prints, not values

BLS publishes most series repeatedly — a preliminary print, one or more routine revisions, and
periodic benchmarks that can rewrite years of history at once. Each publication of a value for a
reference period is a **print**. `bls-stats` stores one row per print, never overwriting.

Every observation row carries six vintage columns in addition to the program's own data columns:

| Column | Dtype | Meaning |
|---|---|---|
| `ref_date` | `Date` | The reference period, normalized to a date (e.g. the pay period containing the 12th). |
| `release_date` | `Date` | The day BLS published this print. Partition key. |
| `revision` | `Int16` | Routine revision counter: 0 = preliminary, 1 = second print, … Null for backfill snapshots. |
| `benchmark` | `Int16` | Benchmark counter: increments each time an annual benchmark re-publishes this `ref_date`. Null for backfill snapshots. |
| `source` | `Utf8` | `"increment"` (captured live from a release) or `"backfill"` (historical seed). |
| `downloaded` | `Datetime("us", "UTC")` | When the file was fetched. |

The **candidate key** is `(unit columns, ref_date, release_date)` — for most programs the unit
column is `series_id`; QCEW and OEWS use their natural dimension columns. One release publishes at
most one print per reference period, so the key is unique by construction and the integration
suite asserts it.

## The lifecycle, concretely

CES March 2026 employment accumulates four rows:

| `release_date` | `revision` | `benchmark` | Why |
|---|---|---|---|
| 2026-04-03 | 0 | 0 | first (preliminary) print |
| 2026-05-08 | 1 | 0 | revised alongside April's preliminary |
| 2026-06-05 | 2 | 0 | revised again alongside May's preliminary |
| 2027-02-05 | 2 | 1 | annual benchmark re-publishes it |

A single release therefore carries prints for *several* reference periods — the new month at
revision 0 plus revised prints of prior months. [Slot expansion](release-detection.md#slot-expansion)
computes exactly which `(ref_date, revision, benchmark)` prints each release carries.

## Reading it back

Three canonical reads, in [`bls_stats.storage.reads`](../reference/storage.md):

- [`latest`][bls_stats.storage.reads.latest] — the best-known value per `(unit, ref_date)` today.
- [`as_of`][bls_stats.storage.reads.as_of] — the best-known value *as of a given date*: it filters
  to `release_date <= when` first, so it can never return information published after `when`.
  This is the read that makes backtests honest.
- [`prints`][bls_stats.storage.reads.prints] — filter to a specific print, e.g. every
  first print (`revision=0`) for revision-behavior studies.

"Best-known" is a deterministic four-level tie-break: latest `release_date` first, then live
captures over backfill snapshots (`source="increment"` outranks `"backfill"` from the same day),
then highest `benchmark`, then highest `revision`. Nulls (backfill counters) sort last, so a live
print always beats a snapshot when both exist.

## Backfill vintages vs live vintages

Bulk LABSTAT files only contain the *current* state of history — first prints are gone. A
[`backfill`](../cli.md#backfill) therefore stamps rows with the snapshot date as their
`release_date` and **null** `revision`/`benchmark`: an honest "this is what the file said on that
day", not a fabricated print history. Live [`ingest`](../cli.md#ingest) runs then accumulate true
print-by-print vintages from that point forward.

## Missed prints are permanent

If a print was published but never captured — the scheduler was down, the file never went fresh —
the slot ledger records it as `missed`, forever. The store does **not** substitute a later value
for the missed print: that would be exactly the silent history-rewriting the vintage model exists
to prevent. Deferral, retry, and the `missed` transition are covered in
[Release detection & deferral](release-detection.md#deferral-never-guessing).
