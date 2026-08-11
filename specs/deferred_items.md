# Deferred items

## 1-bls-stats-stage1-probes — 2026-08-11

1. **§20 issue 14 — deployment endpoint capability matrix and container reachability.** Plan
   `specs/plans/completed/1-bls-stats-stage1-probes.md` Task 6, Steps 4–5 (skipped, not run).
   Needs deployment endpoint credentials (`AWS_ENDPOINT_URL`/`AWS_ACCESS_KEY_ID`/
   `AWS_SECRET_ACCESS_KEY`) and a shell on a deployment-side container. Re-run
   `probes/objstore_capabilities.py --context workstation-deploy` and `--context container`.
   This is the last unmet Stage-1 exit criterion, and
   `specs/bls-stats-stage1-findings.md` section 5's deployment columns (`workstation-deploy`,
   `container`) stay blocked until it runs.

2. **Replication mechanism decision.** `specs/bls-stats-stage1-findings.md` section 6, pending.
   Conditional on item 1: pick provider-side replication (option A) if the deployment endpoint
   has a working replication API, otherwise the second-endpoint pull job (option B). Default is
   the pull job (B) until that evidence exists.

3. ~~**Spec amendment: §7.1's `html` profile row and §7.2's mitigation ladder.**~~ → **done
   2026-08-11, commit `9cf6cc6`**, ahead of plan 2 rather than during it: a plan copies its
   Global Constraints verbatim from the spec, so the stale text would have propagated the
   blocked profile into the plan. Original entry, for the record:
   `specs/bls-stats-spec.md` §7.1 (~line 721) still specifies "full browser-shaped headers" for
   `www.bls.gov` — the exact configuration Stage 1 found blocked 6/6
   (`specs/bls-stats-stage1-findings.md` section 3), while the compliant contact profile
   succeeded 6/6. §7.2's mitigation ordering (lines 726-745) ranks browser-shaped `httpx` ahead
   of a headless backend, with no lighter-weight contact-UA rung between them. An implementer
   deriving transport from the spec table alone (without also reading the findings document)
   would build the blocked profile. Fix: update both spec passages to document the contact
   profile as the working `html`-ingest transport, per findings section 3 and section 8's
   "Roadmap re-validation" bullet.

4. **Spec contradiction: `authoritative_scope` vs. the QCEW artifact.** Spec §8.3
   (`specs/bls-stats-spec.md`) and R13 fix `authoritative_scope` as per-quarter, but Stage 1's
   measurement (`probes/qcew_sizes.py`, `probes/results/qcew_sizes-2026-08-10.jsonl`;
   `specs/bls-stats-stage1-findings.md` section 4) found the singlefile artifact
   (`2025.q1-q4.singlefile.csv`) is a whole-*year* artifact, not per-quarter. With a per-year
   artifact and a per-quarter declared scope, the failure direction is *under*-deletion (quarters
   present in the artifact but outside the declared scope silently fail to emit genuine
   deletions) rather than §8.3's stated over-deletion hazard. Needs a Stage-5 decision on which
   frame governs `authoritative_scope` before the differ is hardened.

5. **Interleaved-profile HTML probe.** `specs/bls-stats-stage1-findings.md` section 3 broke the
   profile-vs-request-position confound substantially but not conclusively: no single session in
   Stage 1 interleaved both header profiles (browser-shaped vs. contact) request-by-request
   against the same URLs. A same-session interleaved-profile probe against `www.bls.gov` would
   fully separate "the browser-shaped profile is blocked" from any residual sequence-position
   effect. Low priority — the operational conclusion (build `html` ingest on the contact
   profile) holds either way.

6. **`probes/objstore_capabilities.py`: cleanup outcome is stdout-only.** Whether the throwaway
   `bls-stats-probe-*` buckets were deleted after a run is printed to stdout but never captured
   into the JSON result written to `probes/results/objstore-*.json`, so the committed evidence
   cannot confirm cleanup after the fact (see `specs/bls-stats-stage1-findings.md` section 5,
   "Probe-bucket state" discussion). Fix: capture the cleanup step's per-bucket outcome
   (`cleaned up <bucket>` / `cleanup <bucket>: <error>`) into the `finish()` result structure in
   `probes/objstore_capabilities.py` before the next run.

7. **`probes/objstore_capabilities.py`: delete-deny check omits `VersionId`.** The
   `delete_deny_policy` check's `delete_object` call (function `delete_deny` in
   `probes/objstore_capabilities.py`) does not pass a `VersionId`, and the target bucket
   (`plain`) has versioning enabled earlier in the same run — so an unversioned delete against a
   versioned bucket only exercises `s3:DeleteObject` (the delete-marker path), never
   `s3:DeleteObjectVersion`. The recorded NOT ENFORCED result is negative, so this under-tests
   rather than over-reports enforcement, but coverage of an actual version-delete attempt is
   untested. The Stage-2 re-run against the non-admin runtime credential (see item 8 and
   `specs/bls-stats-stage1-findings.md` section 7's Delete policy row) must cover both a
   delete-marker attempt (no `VersionId`) and a versioned-delete attempt (`VersionId` set to a
   real version) before treating the deny policy as confirmed for that credential.

8. **Delete-deny policy action coverage.** The policy JSON in
   `specs/bls-stats-stage1-findings.md` section 7 denies only `s3:DeleteObject` /
   `s3:DeleteObjectVersion` on `arn:aws:s3:::bls-stats-raw/*` — not
   `s3:PutBucketLifecycleConfiguration` (an expiration rule is itself a path to deletion) nor
   `s3:PutBucketPolicy`/`s3:DeleteBucketPolicy` (a credential that can rewrite or remove the
   policy can self-authorize the deletes it currently blocks). This is decisive in the branch
   where the deployment endpoint turns out not to support object lock and delete-deny becomes
   the *sole* protection layer (section 7's Object lock row fallback). Recorded as a pending
   operator decision — the recommendation is that the runtime credential's own IAM grant should
   explicitly exclude those three actions on the raw bucket — but the policy JSON itself was
   left plan-verbatim and not edited. Needs an operator decision before Stage 2 provisions the
   non-admin runtime credential.

9. **`probes/_lib.py`'s fallback contact address differs from the recorded runs' actual
   address.** `CONTACT_UA` in `probes/_lib.py` falls back to `mason.lowell@mac.com` if
   `BLS_CONTACT_EMAIL` is unset (e.g. an isolated worktree lacking `.project.env`), but the
   Stage-1 runs actually sent whatever `BLS_CONTACT_EMAIL` was set to in the (git-ignored)
   `.project.env`. If those values differ, a re-run without `.project.env` present identifies
   itself to BLS with a different contact address than the one the committed
   `probes/results/*.jsonl` evidence reflects. Low priority; worth a note in `probes/README.md`
   or reconciling the fallback with `.project.env`'s actual value if it ever changes.

10. **Commit `3ef362d` has its `Co-Authored-By` trailer welded into the subject line.** From a
    parallel session (`git log --oneline` shows `3ef362d Ignore __pycache__ Co-Authored-By:
    Claude Fable 5 <noreply@anthropic.com>` as a single line, rather than the trailer on its own
    line in the commit body). Cosmetic; fixing it would require a history rewrite (`git rebase
    -i` / `filter-branch` equivalent), which is disproportionate for a cosmetic issue this deep
    in the log, so it is recorded here rather than fixed.
