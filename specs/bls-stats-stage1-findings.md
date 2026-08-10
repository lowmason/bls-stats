# Stage 1 findings — ground-truth probes

**Roadmap:** specs/bls-stats-spec-roadmap.md, Stage 1. **Spec:** specs/bls-stats-spec.md.
**Method:** each finding cites its probe script (`probes/`), raw output
(`probes/results/`), and run date. Service behavior is point-in-time: every result is
evidence for its date, none is assumed stable (re-verify before hard-coding).

## 1. Flat-file ingest channel — §20 issue 2 (Task 2)

What settles it: "confirm the compliant profile passes and lock the ingest channel
before anything else is built."

*(recorded by Task 2)*

## 2. API surface — §7.1 `api` profile (Task 3)

What settles it: structural success checking demonstrated live — HTTP 200 and
`REQUEST_SUCCEEDED` are not success signals.

*(recorded by Task 3)*

## 3. HTML posture — §20 issue 1 (Task 4)

What settles it: "a live probe across all three transport profiles … it determines the
whole HTML ingest posture" — browser-shaped `httpx` sufficient, or headless mandatory.

*(recorded by Task 4)*

## 4. QCEW singlefile sizes — §20 issue 4, sizes half (Task 5)

What settles it: "actual byte sizes … the review's size figures are third-party."
Diff *cost* is measured at Stage 5 (roadmap).

*(recorded by Task 5)*

## 5. Object-store endpoint capability matrix — §20 issue 14, §1.4, §17.4 (Task 6)

What settles it: "confirming the deployment endpoint's address and reachability from
the container network," plus which §17.4 requirements the endpoint can satisfy.

*(recorded by Task 6)*

## 6. Replication options — §20 issue 15, mechanism (Task 7)

What settles it: "the deployment's replication decision" — options recorded here,
`doctor`-confirmed at Stage 7.

*(recorded by Task 7)*

## 7. Archive-bucket creation parameters — §7.3, §17.4 (Task 7)

Written **before any bucket exists**: object-lock retention is settable only at bucket
creation (§7.3) — "the one storage decision that cannot be corrected later."

*(recorded by Task 7)*

## 8. Consequences for later stages (Task 8)

*(recorded by Task 8)*
