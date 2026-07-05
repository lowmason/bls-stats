# Getting started

## Install

Requires Python ≥ 3.12 and [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/lowmason/bls-stats.git
cd bls-stats
uv sync
```

This installs the `bls-stats` console script into the project environment. Run everything through
`uv run` (e.g. `uv run bls-stats --help`).

## Configure

Settings load from the environment via a gitignored `.project.env` file at the repo root (never
committed). Only the variable *names* below are part of the contract; supply your own values.

| Variable | Default | Controls |
|---|---|---|
| `BLS_STORE_URI` | `./data/store` | Store root. Local path for laptop use; `s3://bucket/store` for an S3-compatible object store. Deployment should use an `s3://` URI — `doctor` warns on a local path. |
| `BLS_CONTACT_EMAIL` | warns if unset | A real contact address; BLS asks for one in the User-Agent of every request. |
| `BLS_API_KEY` | none | Optional. Enables the [API v2 utility engine](reference/engines.md). |
| `BLS_METADATA_CACHE` | `data/cps_metadata` | Local cache dir for CPS dimension tables. |
| `AWS_ENDPOINT_URL` | none | S3-compatible endpoint URL (object store or local MinIO). |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | none | Standard AWS-style credentials for the object store. |
| `BLS_S3_UNSAFE_RENAME` | `false` | Single-writer fallback for object stores without conditional PUT. Leave unset unless `bls-stats doctor` tells you to set it. |
| `BLS_LOG_LEVEL` | `INFO` | stderr verbosity. |

Example `.project.env`:

```dotenv
BLS_STORE_URI=s3://your-bucket/store
BLS_CONTACT_EMAIL=you@example.com
AWS_ENDPOINT_URL=https://your-s3-endpoint.example.com
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
```

!!! danger "Secrets stay out of git"
    `.project.env` is gitignored from the first commit. Never commit credentials, and never echo
    the file's contents into logs or shell history.

## Bootstrap

Run these in order the first time. The backfill step needs the calendar, and everything needs a
reachable store, so `doctor` comes first.

```bash
# 1. Pre-flight: store reachability, commit-safety (conditional-PUT) probe,
#    BLS reachability, and a check that BLS_CONTACT_EMAIL / credentials are set.
#    Fix any red line before continuing.
uv run bls-stats doctor

# 2. Build the release-date calendar (backfill filters periods against it).
uv run bls-stats calendar build

# 3. Seed history for a program (repeat per program; QCEW streams one year at a time).
uv run bls-stats backfill --program jolts --start 2015/01 --end 2026/05
```

Period strings follow the program's frequency: `YYYY/MM` for monthly programs, `YYYY/Q` for
quarterly (e.g. `2024/2`), and `YYYY` for annual.

## Schedule

Two recurring lines keep the store current — a daily incremental ingest and a weekly maintenance
pass:

```cron
# daily incremental ingest (all feed-driven programs)
30 12 * * *  cd /path/to/bls-stats && uv run bls-stats ingest

# weekly compaction + vacuum
0 3 * * 0    cd /path/to/bls-stats && uv run bls-stats store maintain
```

`ingest` is idempotent: a re-run after a crash repairs its ledger without duplicating data, and a
release whose file has not yet been refreshed upstream is *deferred* and retried on the next run.
See [Pipeline, validation & exit codes](concepts/pipeline.md) for what the exit codes mean to your
scheduler.

## First reads

```bash
# What does the store hold?
uv run bls-stats store info

# Latest view of one reference month
uv run bls-stats store query --program ces --ref-date 2026-03-12

# What was known on a given day (no future leakage)
uv run bls-stats store query --program ces --ref-date 2026-03-12 --as-of 2026-04-15

# Every print ever captured for that month
uv run bls-stats store query --program ces --ref-date 2026-03-12 --all-vintages
```

From Python, the same three reads are [`latest`][bls_stats.storage.reads.latest],
[`as_of`][bls_stats.storage.reads.as_of], and [`prints`][bls_stats.storage.reads.prints]:

```python
from datetime import date

from bls_stats.core.config import load_settings, storage_options
from bls_stats.storage.delta import VintageStore
from bls_stats.storage.reads import as_of

settings = load_settings()
store = VintageStore(settings.store_uri, storage_options(settings))

lf = store.scan_observations("ces")
snapshot = as_of(lf, ["series_id"], date(2026, 4, 15)).collect()
```

## Learn more

The full design — program contracts, the vintage schema, release detection, storage layout, CLI
surface, and the testing strategy — lives in the repository at `specs/bls-stats-architecture.md`.
