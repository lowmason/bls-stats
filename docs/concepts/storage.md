# Storage & crash safety

The store is plain Delta Lake via [delta-rs](https://delta-io.github.io/delta-rs/) — no services,
no catalog, just tables on an S3-compatible object store (or a local path for laptop use).

## Layout

```text
{BLS_STORE_URI}/
├── ces/observations/          # one Delta table per program,
├── jolts/observations/        # partitioned by release_date
├── qcew/observations/
│   …
├── cps/metadata/
│   ├── series/                # CPS series catalog (snapshot-replace)
│   └── mappings/{name}/       # ln.* dimension tables
└── state/
    ├── ledger/                # append-only slot ledger
    └── release_calendar/      # scraped release-date calendar
```

Observations are **append-only** — [`VintageStore.append_observations`][bls_stats.storage.delta.VintageStore.append_observations]
enforces the exact vintage schema (dtypes included) before every write, so a malformed frame can
never reach a table. Partitioning by `release_date` makes as-of reads prune to exactly the files
they need.

## The slot ledger

The ledger (in [`bls_stats.vintage.ledger`](../reference/vintage.md)) is the pipeline's memory:
one record per slot per status change, keyed by
`(program, ref_date, release_date, revision, benchmark)` with a status of `ingested`, `deferred`,
or `missed`.

It is itself **append-only**: a status change is a new row, and
[`Ledger.resolved`][bls_stats.vintage.ledger.Ledger.resolved] reduces to the latest record per
slot key by `ingested_at`. Nothing is ever updated in place, which means the ledger inherits the
same time-travel and audit properties as the data it describes.

## Crash safety: commit, then record

Every slot follows a strict ordering:

1. **Append the observations** to the program's Delta table.
2. **Record `ingested`** in the ledger.

If the process dies between the two, the data is present but the ledger doesn't know it. The
repair is automatic on the next run:
[`VintageStore.slot_exists`][bls_stats.storage.delta.VintageStore.slot_exists] checks for the
slot's rows before appending — if they are already there, the pipeline **repairs the ledger
without re-appending**. Duplicates are impossible in the happy path *and* in the crash path.

The inverse ordering (record first, commit second) would be worse than a crash: the ledger would
claim data that doesn't exist, and slot filtering would skip it forever.

## Concurrent writers: conditional PUT

Delta's optimistic concurrency needs an atomic "create this log file only if it doesn't exist".
On S3-compatible stores that is a **conditional PUT** (`If-None-Match`), which delta-rs uses when
configured with `aws_conditional_put=etag` — the default
[`storage_options`][bls_stats.core.config.storage_options] for any `s3://` store URI.

Not every S3 implementation supports it, so [`doctor`](../cli.md#doctor) probes for it directly:
it PUTs a probe object twice with `If-None-Match: *` — a **412 Precondition Failed** on the
second PUT proves the store enforces conditional writes, making concurrent Delta commits safe.
If the probe fails, set `BLS_S3_UNSAFE_RENAME=true` to fall back to single-writer mode — and then
make sure only one writer (one cron line, no overlap) ever touches the store.

## Maintenance

Appending small frames daily accumulates small files. The weekly
[`store maintain`](../cli.md#store-maintain) pass runs Delta `optimize.compact()` plus a
7-day-retention `vacuum` per program table — one cron line, no other upkeep.
