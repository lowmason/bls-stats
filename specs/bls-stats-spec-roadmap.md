# bls-stats roadmap

> For agentic workers: REQUIRED SKILL: derive-roadmap — resume via its
> reconcile step; route each unticked stage per its ROUTING line; never plan
> this document wholesale.

**Source spec:** `specs/bls-stats-spec.md` (design proposal, revised 2026-08-10, post-review
synthesis). **Derived:** 2026-08-10.

## Gap analysis

Repo state at derivation: `specs/` only — no `src/`, no `pyproject.toml`, no tests, no CI. Every
requirement is unimplemented; evidence is uniform ("none found — repo contains specs/ only",
verified by directory listing 2026-08-10). No `in-code-but-not-in-spec` rows (there is no code) and
no `out-of-repo` rows — R5's timestamping service and R7's second-copy mechanism are external
*services*, but the repo-side obligations (N10–N12, `doctor` reporting) are in-scope requirements.

| Req | Verdict | Evidence | Note |
|---|---|---|---|
| §4 slot ledger, lifecycle, coverage monitor | missing | none found | |
| §5 program registry, delta rules, `ref_date` | missing | none found | |
| §6 reconciliation, change detection, ladder, feed | missing | none found | |
| §7 transport, 403 handling, archive, `fetch_log`, `file_vintage` | missing | none found | |
| §8 classification, `knowledge_time`, differ, catalog | missing | none found | |
| §9 manifest commit, layout, `observation`, `knowability` | missing | none found | |
| §10 as-of query, temporal invariants, replay, rebuild | missing | none found | |
| §11 backfill B1–B4 | missing | none found | |
| §12 errata, notices, markers, dependency closure | missing | none found | |
| §13 disruption handling, triage | missing | none found | |
| §14 validation engine, invariant map | missing | none found | |
| §15 CLI surface, exit codes, plane split | missing | none found | |
| §16 packaging, object-store adapter | missing | none found | |
| §17 loops, lease, bucket policy, compaction | missing | none found | |
| §18 test layers, golden regressions | missing | none found | |
| §20 probe-resolvable opens (issues 1, 2-confirm, 4-sizes, 14, 15-mechanism) | missing | unanswered | Facts, not code — discharged by Stage 1 |

P1–P9 are cross-cutting principles enforced through the rows above; they carry no separate rows.
§19/§21 are meta (sequencing, synthesis record), not implementable requirements.

## Open questions

Recorded, none blocking:

1. **Program count.** §1.2 enumerates eleven programs; §12.5/§12.10 say "ten". Roadmap assumes
   eleven (JOLTS-STATE separate, per §1.2). Correct the stale count in the spec when convenient.
2. **Invariant 26's SAE half vs. M6→M7 order.** §19 puts benchmark cross-checks (M6) before SAE's
   parser (M7), but B3b (§11.4) reconciles against the store's own SAE vintages. Stage 10 ships the
   machinery + QCEW B3a + fixture-level tests; the first live SAE reconciliation runs after
   Stage 11 and the next benchmark. Recorded on Stages 10–11.
3. **Seed scope per stage.** §11.1 seeds "every program"; parsers arrive in waves. Read here as:
   Stage 6 seeds the Stage-5 programs and builds the machinery; Stages 11–12 seed their own
   programs on arrival. Capture (Stage 2) archives all programs from day one, so no vintage is
   lost in the interim.

## Sequencing

The order **follows the spec's own §19 build order (M0–M7)** — §19 is already sequenced by
dependency and irreversibility, the same criterion this roadmap uses, and that agreement is the
main finding. It is **not** sequenced by the external critique's value ranking: R5 (attestation)
was the review's highest-EV item and lands inside Stage 3, where the commit path is built.

Four divergences, none a reordering:

- **Stage 1 prepended** (investigation). §20 issue 2 says the transport probe comes "before
  anything else is built", and the archive bucket's immutability config is at-creation-or-never
  (§7.3) — so the probes become their own stage whose exit is a written finding.
- **M1 split** into Stages 3–4 (store mechanism / control plane): two subsystems, two cycles.
- **M5 split** into Stages 8–9 (errata / notices): §12's own framing — "two distinct channels,
  with different shapes and different jobs" — and errata arming ships without the classifier.
- **M7 split** into Stages 11–12: the spec's own "EP is the hard one — do it last, deliberately."

Every ROUTING is `writing-plans`: after the critique round-trip, no stage's design space is open —
§20's remaining opens resolve by measurement or observation, not by design.

## Stages

- [x] Stage 1: Ground-truth probes (investigation)
      Objective: Settle the probe-resolvable unknowns that gate irreversible Stage-2 decisions;
        exit artifact is a written finding, not software.
      Spec: §20 issues 1, 2, 4 (sizes), 14, 15 (mechanism); §1.4; §7.1–§7.2; §7.3
        (bucket-creation immutability); §17.4 requirements table.
      Gap closed: §20 probe-resolvable row.
      Consumes: nothing (fresh repo).
      Produces: recorded findings in specs/ — per-host transport posture, endpoint capability
        matrix, container→endpoint reachability, QCEW singlefile sizes, replication options;
        archive-bucket creation parameters stated.
      Exit: each §20-cited probe has a dated result and method; the ingest channel is confirmed
        locked (issue 2); bucket-creation parameters are written down before any bucket exists.
      ROUTING: writing-plans
      Stage 1: COMPLETE WITH ONE OPEN ITEM (2026-08-10) — implemented by plan 1
        (specs/plans/1-bls-stats-stage1-probes.md). Findings:
        specs/bls-stats-stage1-findings.md.
      Open: §20 issue 14 does not close in this stage — the deployment endpoint's address
        and reachability, and container→endpoint reachability, are unprobed, blocked on two
        operator inputs: deployment-endpoint credentials and a deployment-side container
        shell/runtime (findings §5). §17.3's writer-lease contention question is unanswered
        for the same reason. Close before or during Stage 2, which consumes the endpoint
        capability matrix (findings §5) and executes the bucket-creation sheet (findings
        §7) that depends on it. All other Stage-1 exit criteria are met.
      Next: resume the roadmap.

- [ ] Stage 2: Capture plane (M0)
      Objective: Permanent byte capture for every in-scope artifact — the lock-free plane,
        deployed and running.
      Spec: §19 M0; §16 (packaging bootstrap), §16.1; §7.1–§7.3; §6.1; §17.1 (`--fast`/`--sweep`),
        §17.2, §17.4 (bucket creation); §18.1 (transport, objstore layers).
      Gap closed: §7 (capture half), §16.
      Consumes: Stage 1 findings (transport posture, bucket parameters).
      Produces: deployed scheduled sweep; blobs + `fetch_log` accumulating; objstore adapter as
        the sole store interface; transport profiles.
      Exit: every in-scope artifact HEAD-swept on schedule in deployment; a changed artifact
        yields blob + `fetch_log` record with both hashes; `restamp_only` distinguished from
        `new_bytes`; `--real-store` round-trip tests pass.
      ROUTING: writing-plans

- [ ] Stage 3: Store mechanism (M1, writer half)
      Objective: The single-writer plane — lease, manifest commit, generation swap,
        `file_vintage` materialization.
      Spec: §9.1–§9.2; §7.4; §17.3–§17.4 (compaction); §14.2 N7/N8/N9 enforcement points;
        §18.1 (store, compaction, ledger-materialization layers).
      Gap closed: §9 (commit/layout half), §17 (lease/compaction half).
      Consumes: Stage 2 objstore + live `fetch_log` stream.
      Produces: writer lease; manifest-visibility primitive; `_current`/`gen=` indirection on
        every partition from first write; `file_vintage` materialized from `fetch_log`.
      Exit: ledger-level torn-commit invisibility proven; compaction crash tests pass on both
        sides of the `_current` PUT; re-materialization over the same log range yields zero new
        rows (N8); an unleased write raises (N7).
      ROUTING: writing-plans

- [ ] Stage 4: Control plane (M1, slots half)
      Objective: Slots, calendar/feed ingestion, the three-signal loop, coverage monitor.
      Spec: §4; §6.2–§6.4; §5.1 (config split, registry skeleton); §17.1 loop table.
      Gap closed: §4, §6.
      Consumes: Stage 3 writer + materialization; Stage 2 capture.
      Produces: `release_slot`/`slot_history` live; calendar+cadence minting with fan-out;
        ladder arming; slot-transition materialization; `coverage(program)`.
      Exit: `.ics` sync mints and updates slots (fan-out case included); the ladder escalates per
        §6.2 on a due slot; the §6 truth table's pre-parse rows produce captures and flags;
        coverage flags a synthetic staleness.
      ROUTING: writing-plans

- [ ] Stage 5: Data plane — CES-N, JOLTS, QCEW (M2)
      Objective: parse → classify → diff → commit → as-of, with the schema proven against both
        LABSTAT and non-LABSTAT shapes at once.
      Spec: §5.2–§5.4 (three programs); §8; §9.3; §10.1–§10.3; §18.1 (delta-rule, duality, as-of,
        differ, memory-envelope layers); §18.2 case 1 (incl. its (open) date verification, R16);
        §20 issue 4 (sizes by measurement).
      Gap closed: §8, §9.3, §10; §5 (partial: 3 of 11 programs).
      Consumes: Stages 2–4 (capture, writer, slots for §8.2).
      Produces: `observation`/`current_state` for three programs; the single `as_of`
        implementation; `unit_key`/`measure_code` schema proven against QCEW; footprint machinery.
      Exit: golden regression 1 passes (dates verified or fallback fixture per §18.2); the as-of
        property test holds in its max-`knowledge_time` equality form; a QCEW-scale run stays
        inside the RSS budget; the duality property holds.
      ROUTING: writing-plans

- [ ] Stage 6: Backfill — seed + knowability (M3)
      Objective: The provenance split and pre-`T₀` honesty, before the store fills.
      Spec: §3; §11.1–§11.3; §9.4; §14.2 N1; §10.1 strict semantics; §17.3 (backfill lease
        chunking); §18.1 strict-semantics layer.
      Gap closed: §11 (B1/B2 for live programs), §9.4.
      Consumes: Stage 5 commit path; Stage 2 archived history.
      Produces: per-program `T₀`; seed release events for the Stage-5 programs; `knowability`
        rows; N1 enforcement; reusable seed/knowability machinery for Stages 11–12.
      Exit: strict `as_of` at `T₀+ε` returns exactly the seed row count and before `T₀` returns
        empty; N1 blocks a synthetic violation; B2 rows validate against delta-rule cadence with
        confidence recorded.
      ROUTING: writing-plans

- [ ] Stage 7: Validation engine + ops (M4)
      Objective: Findings, triage, incidents, daily report — the operator surface.
      Spec: §14; §13; §15 (ops/findings commands, exit codes, `doctor`); §17.1 `--daily`;
        §17.4 fixity (N11) and replication reporting (N12).
      Gap closed: §13, §14, §15.
      Consumes: Stages 3–6 (the paths that generate findings).
      Produces: rule registry; `finding`/`finding_policy`/`triage_action`/`incident`; daily
        report; ASSERT override path; `doctor` and `verify fixity`.
      Exit: an injected ASSERT blocks a commit, writes a finding, and is releasable via
        `--override`; an incident collapses correlated lateness to one notification; the daily
        report renders; exit codes observable per §15.
      ROUTING: writing-plans

- [ ] Stage 8: Errata automation (M5, errata half)
      Objective: The structured errata channel — ingest, forward-dated arming, mutation matching,
        rate budget.
      Spec: §12.1–§12.4; §6.3 (correction row); §14.2 invariants 24, 25.
      Gap closed: §12 (errata half).
      Consumes: Stage 7 findings plumbing; Stage 4 slots.
      Produces: `errata_row` ingest; armed correction slots; mutation matcher; rate monitor.
      Exit: a forward-dated erratum arms a slot before its date; an unmatched mutation alerts
        `unexplained_mutation`; the rate monitor flags a zero-row scrape.
      ROUTING: writing-plans

- [ ] Stage 9: Notices channel + markers (M5, notices half)
      Objective: Subject-area crawl, classification, routing, markers, and the dependency-closure
        watch list.
      Spec: §12.5–§12.10; §6.3; §8.1 (input 4); §14.2 invariants 24b, 24c; §20 issues 9, 10.
      Gap closed: §12 (notices half).
      Consumes: Stage 7 findings plumbing; Stage 4 slots.
      Produces: hand-enumerated subject-area registry; notice crawl/classify/route; proactive
        reschedule application; methodology and expected-loss markers; tier-1/2 watch surfaces.
      Exit: a reschedule notice moves a slot before lateness; an ambiguous NCS notice attaches to
        both candidates with `inferred` confidence; `unknown` routes to triage and is never
        auto-applied; a precision marker changes differ behavior on a fixture.
      ROUTING: writing-plans

- [ ] Stage 10: Reconstruction + cross-checks (M6)
      Objective: Pre-`T₀` vintage recovery where BLS publishes the diff, and the external
        cross-check machinery.
      Spec: §11.4–§11.5; §14.2 invariants 26, 26b; §15 `verify benchmark-diff`.
      Gap closed: §11 (B3/B4).
      Consumes: Stage 5 QCEW pipeline; Stage 6 provenance machinery; Stage 7 findings.
      Produces: QCEW reconstructed vintages (2017Q1+); B3b reconciliation job (fixture-proven;
        first live SAE run after Stage 11 — open question 2); ALFRED/Phil-Fed cross-check job.
      Exit: reconstructed rows are excluded from strict queries; the B3b job fails loudly on an
        empty or wrong-column artifact (§11.4's trap); a seeded divergence yields a 26b FLAG.
      ROUTING: writing-plans

- [ ] Stage 11: Remaining programs — SAE, CPS-LN, BED, ECI, ECEC, OEWS, JOLTS-STATE (M7, part 1)
      Objective: Extend the proven abstractions across the seven mechanical remaining programs,
        with their seeds, knowability, and program-specific invariants.
      Spec: §5.3 (remaining rows); §9.3 (OEWS row); §11 (per-program seed/knowability); §14.2
        program-specific invariants; §18.2 cases 2–3; §20 issues 6, 11 (stay open — resolved by
        observation post-`T₀`).
      Gap closed: §5 (remainder except EP); completes §11 coverage for these programs.
      Consumes: everything prior; notably Stage 9 (classification for ECI/CPS notice-dependent
        kinds) and Stage 10 (benchmark-diff machinery for the SAE golden test).
      Produces: ten of eleven programs live end-to-end.
      Exit: golden regressions 2 and 3 pass; coverage monitor spans all ten; each program's seed
        and knowability rows committed under Stage-6 rules.
      ROUTING: writing-plans

- [ ] Stage 12: EP (M7, part 2)
      Objective: The one program with a non-static artifact set — DiscoveryRule crawling,
        per-vintage URL persistence, vintage semantics.
      Spec: §5.1 (EP exception), §5.2 (`DiscoveryRule`), §5.3–§5.4 (EP rows); §9.3 (EP row);
        §14.2 invariants 18, 21; §20 issue 7 (stays open — resolved by observation).
      Gap closed: §5 (EP), completing the program set.
      Consumes: Stage 11's proven multi-program store; Stage 2's HTML transport posture.
      Produces: all eleven programs live; EP discovery crawl with per-vintage link sets.
      Exit: a vintage crawl archives and persists its discovered URL set; a link-set diff under
        an unchanged decade label raises the §5.1 correction signal on a fixture; invariant 21
        enforced at parse.
      ROUTING: writing-plans

## Stage-spec stamp

Every stage spec's Rollout note carries this line, which writing-plans copies verbatim into the
stage plan's header:

> Roadmap: specs/bls-stats-spec-roadmap.md, Stage N — on plan completion, tick the stage and
> re-validate later stages against what shipped.

On completion the stamp becomes authoritative:

> Stage N: COMPLETE (YYYY-MM-DD) — implemented by plan <id> (path).
> Next: resume the roadmap.

## Completion

Retirement is gated on a conformance audit of the accumulated system: re-run the gap rubric over
every numbered requirement (§4–§18, plus the §20 rows), with evidence per verdict — implementing
stage/plan, Deviation notes, deferred_items entries. Unmet requirements exit exactly two ways: a
new stage, or conscious deferral to `specs/deferred_items.md` with a written why. §20 issues 6, 7,
and 11 resolve only by post-`T₀` observation and are expected to remain open at retirement; park
them as deferred items, not stages. Optional independent check: re-run describe-critique-methodology
Describe mode on the shipped system and diff against the spec.
