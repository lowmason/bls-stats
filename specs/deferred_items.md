# Deferred items

Accepted at plan completion (2026-07-05) from the final whole-branch review's triage —
none block correctness; each has a verified backstop or is polish. Source of record for
the review-by-review detail: the retired plan and the final review output.

## Feature work

- [ ] **EP store wiring** (ARCH §12.6): melt the wide matrix to long format (`series_id`
      from occupation/industry/measure, `value` Float64) so EP shares the observations
      table; replace the interim exit-2 guards in `pipeline.py`; cover
      `fetch_matrix` control flow (cache / refresh / all-fail) in the same task.
- [ ] **QCEW size detail exposure**: `parse_year_zip(by_size_zip=...)` /
      `fetch_year(with_size=True)` are implemented and reviewer-verified but no CLI flag
      requests them — add `backfill --with-size` if size breakdowns become a need.

## Test hardening

- [ ] Offline `MockTransport` test for `releases.feeds.poll` (URL dedup, empsit fan-out,
      failed-feed tolerance, oldest-first sort).
- [ ] Directed tie-break test for `storage.reads.latest` opposing source-rank against
      counters (backfill with high counters vs increment with low).
- [ ] `core.http.download` failure paths (per-attempt truncation, 4xx fast-fail, retry
      exhaustion); consider deduplicating the get/download retry loops.
- [ ] Engine-level assertion that `downloaded` lands as `Datetime("us","UTC")` (the store
      guard catches it, but an engine test fails earlier and clearer).
- [ ] `enrich.cps.fetch_metadata` manifest branches (skip-unchanged / digest-mismatch /
      refresh) via fake client + tmp_path.
- [ ] Config test hygiene: `delenv BLS_S3_UNSAFE_RENAME` in the two storage-options tests;
      decide the `contact_email=""` default-flag edge.
- [ ] Direct unit test of `stamp()` with `ref=None` (typed-null `ref_date` path).

## Robustness / polish

- [ ] `releases.calendar.build`: catch parse-time exceptions (not just `httpx.HTTPError`)
      per source so a relaid-out live page degrades to warn-and-skip; note the dedup key
      can leave two rows per `(program, ref_date)` when archive and schedule disagree
      pre-overlay (downstream consumers are duplicate-safe).
- [ ] `storage.doctor.check_conditional_put`: a cleanup `delete_object` failure in the
      `finally` masks a genuine "supported" verdict as "probe failed".
- [ ] `enrich.cps.export_metadata`: pass `schema_mode="overwrite"` (mapping-schema drift);
      reconcile the ARCH §8 `downloaded` timestamp with the clockless signature.
- [ ] `cli store maintain`: wrap DeltaTable optimize/vacuum per program for clean error
      messages (exit code already correct).
- [ ] README: add proxy/CA env vars (`HTTPS_PROXY`, `SSL_CERT_FILE`); complete the
      `store query` example with `--program`/`--ref-date`.
- [ ] Cosmetics: qcew backfill error double-prefixes the program name; `engines/qcew.py`
      URL constants duplicate registry URLs; `api_v2` docstring should note the deliberate
      no-retry divergence for POSTs.
