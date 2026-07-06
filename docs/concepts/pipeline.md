# Pipeline, validation & exit codes

[`bls_stats.pipeline`](../reference/pipeline.md) is the orchestrator: it owns the
detect → expand → fetch → validate → commit → record sequence and the exit-code contract your
scheduler sees. Everything else (engines, feeds, storage) is a library it composes.

## Ingest flow

For each release returned by the feed poll:

1. **Expand** the release into slots; drop slots already `ingested` or `missed`.
2. **Freshness gate**: for LABSTAT-backed programs, defer the whole event if the upstream file's
   `Last-Modified` predates the release embargo ([why](release-detection.md#deferral-never-guessing)).
3. **Fetch once per event** — one download covers all the event's slots.
4. Per slot: slice the frame to the slot's `ref_date`, **validate**, **stamp** the vintage
   columns, **commit** (append), **record** (`ingested`).
5. After a successful event, deferred slots from older releases of the same program flip to
   `missed` — their window has closed.

An exception inside one event is isolated: it is logged, the event's outcome is recorded, and the
loop continues with the next event. One broken program never blocks the rest.

## Validation gates

[`validate`][bls_stats.pipeline.validate] runs before every **ingest** commit. `backfill` is the
trusted historical baseline (ARCH §7.3): it skips the row-band/null-rate gates deliberately — the
string locks are enforced at the engine/parse layer and the store's vintage-schema gate still
guards every append — so a backfill commit is not `validate()`-gated.

| Gate | Check | On failure |
|---|---|---|
| Schema | Every unit column present and `Utf8` (leading zeros intact) | event fails |
| Emptiness | Slot's slice has rows | slot deferred (handled by the caller — an empty slice usually means "not published yet") |
| Null rate | `value` null-rate within the program's threshold (default 5%) | event fails |
| Row band | Row count within ±20% (per-program) of the most recent *ingested* comparator for the same slot type | event fails |

The comparator comes from the ledger — only `ingested` records count, so a deferred or failed
event never pollutes the baseline.

## Exit codes

The contract (ARCH §7.4) is designed for cron: **any non-zero exit means a human should look**.

| Code | Meaning |
|---|---|
| `0` | Everything ingested, or the only non-ingests were **deferrals** (expected: files not fresh yet — the next run retries). |
| `1` | **Partial failure.** Some events failed and others succeeded — including the "partial" case where a failed event had already appended some slots' data before erroring (the ledger repair makes the re-run safe). |
| `2` | **Total failure.** Every event failed, or the run couldn't start (missing calendar, invalid period string, unwired program). |

`backfill` uses the same `0`/`2` ends of the scale: `0` on success or nothing-to-do, `2` when the
range is invalid, the calendar is missing, or the fetch fails.

## Backfill vs ingest

[`run_backfill`][bls_stats.pipeline.run_backfill] seeds history from bulk files: it filters the
requested period range against the release calendar (a period whose data was never published is
skipped, and an all-unpublished range is an error), fetches once, and stamps every period with the
**snapshot date** as `release_date` and null `revision`/`benchmark` — see
[backfill vintages](vintage-model.md#backfill-vintages-vs-live-vintages). Re-running the same
backfill is a no-op: slots already `ingested` are filtered out up front.

## Testability: injected seams

[`run_ingest`][bls_stats.pipeline.run_ingest] takes four injectable seams — `clock`, `poll_fn`,
`fetch_fn`, `fresh_fn` — defaulting to the real implementations. The integration suite replays a
five-release CES sequence through the *real* pipeline with only these seams faked, asserting the
full lifecycle table from [the vintage model](vintage-model.md#the-lifecycle-concretely)
end-to-end. No pipeline logic ever calls `datetime.now()` directly.

!!! note "EP is guarded, not wired"
    The Employment Projections engine (scraper + cache) exists, but its wide-matrix output is not
    yet melted into the observations schema. `ingest --program ep` and `backfill --program ep`
    exit with code 2 and an explanatory error rather than silently no-oping.
