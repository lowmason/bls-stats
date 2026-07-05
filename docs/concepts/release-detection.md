# Release detection & deferral

Knowing *that* BLS released something — and *which prints* that release carries — is half the
system. Detection has three layers: live feeds, a scraped calendar, and per-release slot
expansion.

## Feeds: the live signal

[`releases.feeds.poll`](../reference/releases.md) polls the BLS Atom/RSS feeds for the programs
under ingest. The feeds have quirks the parser is built around:

- **The link href is the only stable date key.** Entry links end in `_(MMDDYYYY).htm`; titles and
  timestamps are unreliable, so the release date is parsed from the href.
- **Titles carry a month but never a year.** The reference year is inferred as the latest
  occurrence of that month strictly *before* the release date — correct across year boundaries
  (a January release of December data).
- **Benchmark detection is structural, not textual.** A release is a benchmark when it
  re-publishes January (or Q1) data per the program's benchmark rule, not because its title says
  "benchmark".
- **One feed can serve two programs.** The Employment Situation feed fans out to both CES and CPS.
- A feed that fails to fetch is logged and skipped — one broken feed never blocks the others.

## Calendar: the persistent record

[`releases.calendar.build`](../reference/releases.md) scrapes each program's news-release archive
and schedule pages into a `release_calendar` state table, with a **lapse overlay** for the
irregularities: a rescheduled release keeps its original date in `original_release`; a cancelled
one gets a null `release_date`. The calendar is what `backfill` filters reference periods against
(never backfill a period whose data was never published) and what [`gaps`](../cli.md#gaps) audits
the ledger against.

## Slot expansion

[`releases.profiles.expand`](../reference/releases.md) turns one detected release into the exact
set of **slots** — `(ref_date, revision, benchmark)` prints — it carries:

- **Routine slots** come from the program's revision profile. CES publishes three prints per
  month, so the June release carries June at revision 0, May at revision 1, and April at
  revision 2.
- **Benchmark windows**: when a release is a benchmark, every reference period in the program's
  benchmark window is re-published at its terminal revision with the benchmark counter
  incremented past the prior maximum for that period.
- **QCEW year-to-date**: a QCEW release for quarter *q* carries all quarters of the year so far,
  each at revision `(q − carried quarter)` — the Q3 release carries Q3 at revision 0, Q2 at
  revision 1, Q1 at revision 2. A quarter reaches its terminal revision `4 − q` only at the
  year's Q4 release; `4 − q` is also the revision used for prior-year quarters re-published in a
  benchmark window.

Slots already recorded as `ingested` or `missed` are filtered out before fetching, which is what
makes re-runs idempotent.

## Deferral: never guessing

BLS pages go live before the underlying flat files are refreshed. Committing a stale file would
stamp yesterday's data with today's `release_date` — a silent corruption of the vintage record.
So before fetching a LABSTAT file, the pipeline checks freshness: the file's `Last-Modified` must
be at or after the release's embargo time (8:30 or 10:00 America/New_York, converted to UTC).

- **Stale file → the whole event is deferred.** Every slot is recorded `deferred`; exit code
  stays 0 (deferral is expected operation, not failure).
- **Deferred slots are retried on every subsequent run** — the next cron tick usually picks
  them up once the file refreshes.
- **Supersession, not timeouts:** a deferred slot becomes `missed` only when a *newer release for
  the same program* successfully ingests — the moment the live print became uncapturable — never
  by wall-clock timeout. An empty slice for one slot defers just that slot the same way.

This is the full slot lifecycle:

```text
                    ┌────────── retried each run ──────────┐
                    ▼                                       │
detected ──▶ deferred (file stale / slice empty) ───────────┤
    │                                                       │
    │               newer release for the program ingested  │
    │                                       │               │
    ▼                                       ▼               │
 ingested ◀─────────────────────────────  missed  ◀─────────┘
 (terminal)                              (terminal)
```
