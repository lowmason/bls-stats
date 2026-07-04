# bls-stats

Vintage-aware ingest for U.S. Bureau of Labor Statistics data. It downloads eight BLS
products (CES, SAE, JOLTS, CPS, BED, QCEW, OEWS, EP) and lands every *print* — not just
the latest number — in a Delta Lake vintage store, so you can ask "what did BLS say on
date D?" and get the answer BLS actually published then. Revisions and annual benchmarks
are recorded as distinct vintages rather than overwriting history, which makes as-of and
point-in-time reads exact.

## Install

Requires Python ≥ 3.12 and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
```

This installs the `bls-stats` console script into the project environment. Run everything
through `uv run` (e.g. `uv run bls-stats --help`).

## Configure

Settings load from the environment via a gitignored `.project.env` file at the repo root
(never committed). Only the variable *names* below are part of the contract; supply your
own values.

| Variable | Default | Controls |
|---|---|---|
| `BLS_STORE_URI` | `./data/store` | Store root. Local path for laptop use; `s3://bucket/store` for an S3-compatible object store. Deployment must use an `s3://` URI — `doctor` warns on a local path. |
| `BLS_CONTACT_EMAIL` | warns if unset | A real contact address; BLS asks for one in the User-Agent of every request. |
| `BLS_API_KEY` | none | Optional. Enables the API v2 utility engine. |
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

## Bootstrap

Run these in order the first time. The backfill step needs the calendar, and everything
needs a reachable store, so `doctor` comes first.

```bash
# 1. Pre-flight: store reachability, commit-safety (conditional-PUT) probe, BLS reachability,
#    and a check that BLS_CONTACT_EMAIL / credentials are set. Fix any red line before continuing.
uv run bls-stats doctor

# 2. Build the release-date calendar (backfill filters periods against it).
uv run bls-stats calendar build

# 3. Seed history for a program (repeat per program; QCEW streams one year at a time).
uv run bls-stats backfill --program jolts --start 2015/01 --end 2026/05
```

Then schedule the two recurring lines — a daily incremental ingest and a weekly store
maintenance pass:

```cron
# daily incremental ingest (all programs)
30 12 * * *  cd /path/to/bls-stats && uv run bls-stats ingest

# weekly compaction + vacuum
0 3 * * 0    cd /path/to/bls-stats && uv run bls-stats store maintain
```

## The vintage model

Every observation is stamped with its `release_date` and a `(revision, benchmark)` pair, so
the same `series_id` + `ref_date` accumulates one row per print — March 2026's employment
number appears at revision 0, then 1, then 2 as CES revises it, then again at the annual
benchmark. Read it back with an **as-of** query (`bls-stats store query --as-of D`, or the
`as_of` / `latest` helpers): as-of never returns a `release_date` after `D`, so you always
see exactly what was published by then. Missed prints are permanent — a print BLS made that
you failed to capture is recorded as `missed` and never silently backfilled with a later
value, which keeps historical reconstructions honest.

## Learn more

The full design — program contracts, the vintage schema, release detection, storage layout,
CLI surface, and the testing strategy — lives in
[`specs/bls-stats-architecture.md`](specs/bls-stats-architecture.md).
