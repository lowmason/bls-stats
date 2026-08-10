# bls-stats

> For agentic workers: REQUIRED NEXT SKILL: derive-roadmap — do not plan
> this spec directly and do not split it into per-subsystem plans.

**Status:** design proposal. 

**Date:** 2026-08-04 · **Revised:** 2026-08-10

**Revision 2026-08-10** — storage plane rewritten against the deployment store's actual capabilities.
Three changes, each with ripples through the document: the object store is **S3-compatible but not
AWS**, so no AWS-only service may be assumed (§1.4, §7.3, §9.2, §17.2, §17.4); **the endpoint does
not support conditional PUT**, which forces **exactly one store writer** (§9.1, §17.1, §17.3) — and,
with concurrency control moot, removes the case for a transactional table format, so the store is
**plain partitioned Parquet with an application-level commit** (§9.1). That in turn moves P2's
durability boundary onto the blob + `fetch_log` pair and replaces §17.3's two leases with a
capture/store **plane split**. The object-store adapter is **purpose-built in-repo** (§16.1).

**Revision 2026-08-10 (b) — external-review synthesis.** A state-of-the-art design review
(`specs/bls-stats-spec-review.md`) was triaged and synthesized into this document. Substantive
additions: manifest statistics (§9.1, R4), manifest attestation (§9.1, R5), full-header capture
(§7.3, R10), archive fixity and replication requirements (§17.4, R6/R7), invariants 26b and
N10–N12 (§14.2), and a prior-art record (§2, R1). The §9.1 storage argument is re-stated on honest
grounds (R2). §20 issues 2, 4, 5, 12, 13, 15 updated; one new (open) on §18.2's golden-test dates
(R16). Full disposition record: §21.

**Design provenance.** Synthesized from this document's pre-review text (commit `c93f5e7`,
2026-08-10) and the external critique `specs/bls-stats-spec-review.md` (commit `a8e5dd6`), per the
describe-critique-methodology round-trip. Locators of the form **(R-n)** cite row *n* of §21's
synthesis record, which anchors each point to the review's own sections. Citations of the form
`review §…` continue to name the companion domain-evidence document, as before — the two are
distinct sources.

---

## 0. Notation and reading order

The word "T" is overloaded in the source material — it names both the cutover instant and, in
some phrasings, a wall-clock offset. This document fixes the vocabulary:

| Symbol | Meaning |
|---|---|
| `now` | The present instant. **The only time the system ever acts at.** |
| `T₀` | **Cutover.** The instant the capture path first ran. Before it, no vintages were observed; after it, every vintage is observed. |
| `t` | A **reference period** — the month, quarter, or year a value describes. |
| `t_max` | The **newest reference period carried by a given release**, read from the artifact. |
| `Y` | A calendar year, used in benchmark arithmetic (`Apr(Y−2)`, etc.). |

**The system looks backward, never forward.** It does not schedule work for "`t` + 1 month," and
it does not predict what a future release will contain. It wakes at `now`, asks what has changed,
reads `t_max` out of whatever actually arrived, and applies the program's delta rule *backward*
from `t_max` to determine which prior reference periods that release revises.

This is not merely a simplification. Forward prediction of release *content* is exactly what a
shutdown or delay breaks: on resumption BLS may skip a reference period, combine two, or publish
out of order (§13.2). A design that materializes "reference period 2026-07 will receive its second
print on 2026-09-04" has to invalidate and rebuild that claim on every disruption. A design that
reads `t_max` from the artifact never had the claim to invalidate.

The split that follows from this:

- **The calendar and feed predict dates.** "A release is expected on date `D`." No claim about content.
- **The artifact declares content.** `t_max` is read, never assumed.
- **The delta rule looks back** from the observed `t_max`.

**Reading order.** §1–§3 are the governing principles and are load-bearing for everything after.
§4–§6 are the control plane (slots, program registry, the three-signal reconciliation). §7–§10 are
the data plane (capture, interpretation, storage, as-of semantics). §11–§14 are backfill, the errata
and notices channels, disruption handling, and validation. §15–§20 are the buildable surface: CLI,
packaging, deployment, testing, sequencing. §21 is the external-review synthesis record, where
every (R-n) locator resolves.

---

## 1. Purpose, scope, non-goals

### 1.1 Purpose

Maintain a **point-in-time (vintage-aware) store of BLS statistical output** such that, for any
instant `D ≥ T₀` and any series, the system can answer:

> What did BLS publish for this series and reference period, as it stood at `D`?

and, distinctly:

> Was this reference period published at all as of `D`, and if so, at what publication stage?

Correctness means *historical* correctness: an as-of query at `D` must never return bytes, values,
footnotes, or series that were not publicly retrievable at `D`. The dominant failure mode of a
naive store is not an error — it is silently returning today's revised values under yesterday's
date, which corrupts every downstream backtest without ever raising.

The premise is no longer hypothetical (R17). Within a year of this document's drafting: the CES
preliminary benchmark of 2025-09-09 announced a −911,000 revision (USDL-25-1352), and the
2026-02-11 final put the March-2025 NSA miss at −862,000, with 2025 job growth revised from
+584,000 to +181,000 SA (USDL-26-0169); the BLS commissioner was dismissed hours after the
2025-08-01 jobs report; and the 43-day appropriations lapse cancelled the October 2025 CPI release
outright — the October unemployment rate can never be computed (Reuters, 2025-11-21). Vintages are
now politically contested objects, and a window not captured is gone.

### 1.2 In scope

**Eleven programs**, per the review document's §14 comparative table plus JOLTS-STATE:

`CES-N` (national CES), `SAE` (CES State & Area), `JOLTS` (national), `JOLTS-STATE`,
`CPS-LN` (aggregate labor-force flat file only), `BED`, `QCEW`, `OEWS`, `ECI`, `ECEC`, `EP`.

`JOLTS-STATE` is modelled as a **separate program**, not a variant of `JOLTS` — it has its own
release and its own benchmark, anchored on Q4(`Y−1`) alignment to JOLTS national, CES, and QCEW
(review §6.3). Its routine `N` is **not documented**; see §5.3 and §20 issue 11.

### 1.3 Non-goals

Stated explicitly, because each is a plausible-sounding scope creep that would damage the design:

1. **No full-universe ingest via the BLS v2 API.** The quota (500 queries/day × 50 series) caps at
   25,000 series/day against programs that publish millions of cells. The API is for interactive
   spot checks only. Flat files are the ingest channel.
2. **No reconstruction of pre-`T₀` *values*** beyond the QCEW revisions CSV (back to 2017Q1). The
   SAE `bmrk{Y−1}-revisions.zip` is used for **validation only**, not vintage recovery — see §11.4
   for why its two-column shape makes committing it unsafe. Pre-`T₀`
   vintages are, in general, permanently unobservable — BLS keeps no archive and overwrites in
   place. The system must be honest about this rather than papering over it (§11).
3. **No back-filling of the CPS population-control discontinuity.** Review §2.D5 / §17 gap 4: BLS
   deliberately does not revise history for population controls. Inferring and applying an offset
   fabricates values BLS never published. This is a correctness bug wearing a helpfulness costume.
4. **No serving layer** beyond the CLI and the object store. Downstream consumers read Parquet
   from the object store or shell out to the CLI. No HTTP API, no database server.
5. **No modelling, nowcasting, or analysis.** This is an acquisition and custody system.
6. **No CPS microdata (PUMS) or supplements** (review §3.1) — different file-replacement and
   weighting semantics; they must not inherit `ln`'s revision classification.

### 1.4 Fixed constraints

- Python ≥ 3.12; `uv` + `hatchling`; `typer` CLI; **`polars` for all tabular data** (no pandas);
  `httpx` for all HTTP.
- **An S3-compatible object store is the only durable storage**, reached at a configurable endpoint
  (`AWS_ENDPOINT_URL`). The `s3://` URI scheme and the core S3 verbs are used unchanged. Three
  things are **not** assumed, and each one costs the design something specific:
  | Not assumed | Consequence |
  |---|---|
  | Any AWS-only *service* — DynamoDB, IAM roles, storage classes | No external lock provider (§9.1); access control and lifecycle are stated as requirements, not as AWS policy documents (§17.4) |
  | **Conditional PUT (`If-None-Match`)** | The compare-and-swap that every transactional table format relies on is unavailable, forcing single-writer mode — see below |
  | A stable intraday endpoint identity | Every option is configuration; no endpoint, region, or credential is ever a constant |
- **Exactly one process writes the store at a time.** This follows from the missing conditional PUT,
  not from preference: with no compare-and-swap, a single writer is the only safe commit discipline.
  It is a hard constraint on the deployment, and §17 is designed around it rather than merely
  tolerating it. §9.1 then takes the consequence seriously — a table format whose central feature is
  multi-writer concurrency control earns nothing here, so the store is plain Parquet.
- **The only S3 operations relied on are `PUT`, `GET`, `HEAD`, `LIST`, and `DELETE`** on individual
  objects, with **single-object PUT as the sole atomicity primitive**. Everything transactional in
  this design is built from that one guarantee.
- Compute is ephemeral containers with no durable local disk; scratch is deleted after parse.

⚠ **A local S3-compatible endpoint on a workstation is a development convenience only.** It has
different capabilities from the deployment store in both directions — it may *support* conditional
PUT, and it may *lack* object-lock or lifecycle features. The design targets the intersection, and
`doctor` (§15) reports what the endpoint in front of it actually offers. Nothing in this document
may be predicated on a capability only one of the two provides.
- Memory envelope: assume ~4 cores / 25 GB RAM. Target **peak RSS < 8 GB**. Sources include
  hundred-megabyte flat files and multi-gigabyte archives — streaming and lazy scans are mandatory.

---

## 2. Architectural principles

These are the decisions everything else follows from. Each is stated with the failure it prevents.

### P1 — The raw byte archive is the only irreplaceable artifact

Every distinct byte sequence ever retrieved is stored, content-addressed by SHA-256, immutably,
forever. **Everything else in the system is derived and rebuildable by deterministic replay from
the archive.** The change log, the current-state snapshot, the series catalog, the as-of views — all
of it can be dropped and regenerated.

*Prevents:* a parser bug, a schema mistake, or a bad deploy from destroying history. Since BLS
overwrites flat files in place and keeps no archive, a vintage not captured at release time is
permanently unobservable. The archive is the one thing that cannot be recomputed.

*Consequence:* the capture path and the interpretation path are **separate processes with different
reliability requirements**. Capture must be simple, dumb, and nearly impossible to break. Parsing
can fail and be retried offline against archived bytes.

### P2 — Ordering: archive before parse, parse before commit

```
HEAD (detect change) → GET (stream bytes to archive) → append fetch_log record   ← DURABILITY BOUNDARY
                     → materialize file_vintage → parse → validate
                     → commit release_event + observation deltas
```

**The durability boundary is the blob PUT plus the `fetch_log` append — neither of which is a store
write.** Both are plain object PUTs to immutable keys: the blob to its content-addressed path
(§7.3), the log record to a uniquely-named JSONL object in that day's partition (§9.2). Nothing
serializes them, nothing can block them, and together they carry every field `file_vintage` holds.

`file_vintage` is therefore a **derived row, materialized by the single writer** (§17.3) from
the `fetch_log` records that have no row yet. Everything from that point on is retryable. A
validation `[ASSERT]` failure aborts the commit; it never aborts the archive.

*Why the boundary sits here rather than at `file_vintage`:* §1.4 admits exactly one store writer, so
placing the boundary at a store table would make the capture path wait on that writer — and P1 ranks
a missed capture as the one unrecoverable failure in the system. Moving the boundary one step
earlier costs nothing, because `file_vintage` is fully reconstructible from the two artifacts that
precede it. A materialization lag delays ledger state; it never loses bytes.

*Prevents:* the class of bug where a release arrives, the parser chokes on an unexpected column,
the job exits non-zero, and by the time a human looks the file has been overwritten.

### P3 — The data artifact is the release, not the news release

Review invariant 23, and two dated cases prove both directions: CPS revised January 2026 in the
database on 2026-03-06 **without reissuing the news release** (§2.5.3f); QCEW before 2025-06-04
published a news release ~14 days *before* the full data file existed (§2.B1). ECEC's database was
updated during the 2025 appropriations lapse with the news release explicitly forgone (§12.5).

`knowledge_time` binds to the data artifact. News releases, feeds, and calendar entries are
**corroborating metadata**, never the primary signal.

### P4 — Delta rules read backward from the observed `t_max`

One authored rule per program describes, for a release whose newest reference period is `t_max`,
which prior reference periods it carries and at what publication stage — separately for SA and NSA.
The "when will period `t` be reprinted" view is a *derived query* over that same rule, never a
second authored artifact.

*Prevents:* drift between a forward schedule and a backward footprint, and the shutdown-invalidation
problem described in §0.

### P5 — Predicted footprints are hypotheses, not assertions

Review §2.D8 and §16's classification: every reach bound is `[FLAG]`, never `[ASSERT]`, because
every program documents reconstructions exceeding its routine window (CES back to 1990; JOLTS full
reconstruction to December 2000). The delta rule generates an *expectation*; the differ reports what
actually changed; divergence is recorded as a finding, not an error.

*Prevents:* invariants that fire false positives, get disabled in CI, and then catch nothing.

### P6 — Metadata is vintage data

Series additions, discontinuations, title changes, area-code renumbering, seasonal-status changes,
footnote codes, and suppression flags are effective-dated with the **same** file vintage as numeric
values (invariant 6). The 2026 SAE benchmark discontinued ~900 series (§5.8); a store that models
only numbers reads that as data loss.

### P7 — Append-only; never mutate a stored vintage

A changed hash under the same URL creates a **new** `file_vintage`. Corrections append. Suppression
appends a null-valued row; it never nulls the earlier row (invariant 15). No `UPDATE` semantics
anywhere in the observation store.

### P8 — Provenance is a first-class column, and honesty beats coverage

Every observation row carries `provenance ∈ {observed, seed, reconstructed, external}`. A strict
as-of query before `T₀` returns **nothing** from the observed store, and that is the correct answer.
See §11.2 — this is the single most likely thing to implement wrong.

### P9 — The HTML surfaces live in the metadata plane only

`www.bls.gov` returns HTTP 403 to ordinary programmatic fetchers (review §0.3). Every HTML-derived
input — schedule pages, the errata table, news-release archives, **program notices pages**, EP
matrix tables — must be degradable: if HTML fetching fails entirely, ingestion continues from
artifact polling alone, losing scheduled times, errata pre-arming, and proactive reschedule
detection, but not losing data. Only EP is genuinely HTML-native, and it is the lowest-frequency
program in scope.

### Prior art — convergence, recorded (R1)

The external review's central finding is that this design *converges* with established systems far
more than it deviates. Worth recording, because each analogue is both a citation and a place to
look when its mechanism here needs hardening:

| Mechanism here | Established analogue |
|---|---|
| Plane split: lock-free capture + one leased writer (§17.3) | Datomic's transactor — immutable data, a single writing process (Hickey 2012) |
| Manifest-then-atomic-pointer commit (§9.1) | Iceberg's atomic metadata swap; crash orphans are invisible, not corrupt |
| `_current` generation pointer (§17.4) | Iceberg/Delta root-pointer versioning — a catalog pointer |
| Change log + as-of query (§9.3, §10.1) | ALFRED's `realtime_start`/`realtime_end`; the Croushore–Stark real-time datasets |
| `wire_sha256` / `content_sha256` + `restamp_only` (§6.1, §7.3) | WARC block- vs payload-digest and `revisit` records (ISO 28500) |
| `current_state` projection (§8.3) | CQRS read model folded from an event log |
| The clairvoyance ban (§1.1, N1–N3) | Feature-store point-in-time correctness |

One deviation is deliberate, and the review endorsed it: ALFRED records a vintage only on revision,
while `restamp_only` sides with WARC's `revisit` in recording *proof of continuity* — that BLS
republished a file unchanged is a real fact, and only it proves a value was still current.

---

## 3. The two regimes

The system has a hard boundary at `T₀` and the two sides are epistemically different, not merely
different in age.

| | **Before `T₀`** (backfill) | **From `T₀`** (live) |
|---|---|---|
| Reference periods | Given — enumerable from the flat files | Discovered — read from `t_max` |
| First-publication dates | **Reconstructed** by scraping news-release archives | Observed |
| Values by vintage | **Unavailable.** One seed snapshot only (+ two BLS-published diffs) | Fully observed, every distinct byte sequence |
| Can answer "what did it say on `D`?" | **No** | Yes |
| Can answer "was it published by `D`?" | Yes (knowability) | Yes |
| Driver | One-time batch | Calendar + feed + artifact reconciliation |

The most important sentence in this document:

> **A backfilled reference period has a knowability date but not a value vintage.**

The reconstructed first-publication date, scraped from `bls.gov/bls/newsrels.htm` and
`bls.gov/bls/news-release/home.htm`, answers *when the period became public*. It must never be
written to an observation row's `knowledge_time`. If it is, an as-of query at 2019 returns today's
post-benchmark values as if they had been public in 2019 — the exact clairvoyance bug the store
exists to prevent (§11.2, invariant N1).

---

## 4. Control plane: the slot ledger

### 4.1 What a slot is — and is not

A **release slot** is an expectation that *a release occurs*, for a program, on a date. It carries
**no claim about content**. `t_max` is null until observed.

```
release_slot
  slot_id                 str    # sha256(program | slot_kind | occurrence_ordinal) — see below
  program                 str
  slot_kind               str    # routine | qcew_news_release | qcew_full_data
                                 # | analytical_notice | errata | out_of_cycle
  occurrence_ordinal      str    # the cadence-implied occurrence this slot fills, e.g. "2026-M08"
                                 # for errata/out_of_cycle: the errata row_key or notice_id
  scheduled_at            datetime(UTC), nullable   # MUTABLE attribute
  scheduled_at_source     enum   # ics | schedule_page | feed | notice | cadence_estimate
  scheduled_tz_local      str    # America/New_York — times are ET, DST matters
  predicted_from          enum   # calendar | cadence | feed | notice
  expects_artifact_change bool   # false for qcew_news_release, analytical_notice — see §6
  state                   enum   # see 4.3
  supersedes_slot_id      str, nullable   # genuine re-issue only, NOT a date move
  incident_id             str, nullable
  created_at, updated_at

slot_history                     # append-only audit of every mutation
  slot_id, changed_at, field, old_value, new_value, actor, reason, evidence_url
```

**Slot identity is keyed on the occurrence, not on the date.** This is the correction that matters:
`scheduled_at` is exactly the field the design mutates most — a notice reschedules it (§12.6), a
calendar edit moves it, an incident delays it — so hashing it would orphan every foreign key in
`file_vintage`, `release_event`, `finding`, and `triage_action` on each move. The occurrence ordinal
("the August 2026 CES release") is stable across every reschedule of that release.

`supersedes_slot_id` is therefore reserved for a genuine **re-issue** — an errata correction of an
already-committed release — not for a date change. A date change is a mutation of one slot,
recorded in `slot_history`.

`expects_artifact_change = false` marks the slots that legitimately produce no data-file change:
QCEW news releases before the 2025-06-04 merge, and the CES preliminary benchmark announcement
(review §4.1, which "does not produce a completed January-release flat-file snapshot"). Without this
flag they age into false lateness every cycle. See §6, row 2.

### 4.2 Where slots come from

Two sources, in priority order:

1. **Calendar (authoritative for dates and times).** The iCalendar feed
   `https://www.bls.gov/schedule/news_release/bls.ics` (review §2.5.3d) carries scheduled dates
   *and* times and is updated at least a week ahead. Per-program schedule HTML pages are the
   fallback. **Never hardcode a release time** (invariant 27): ECI is 08:30 and ECEC is 10:00, on
   different days, from the same survey.
2. **Cadence estimate (fallback, so silence is still detectable).** Each program's registry entry
   carries a nominal periodicity and lag. If the calendar names no release for a period where the
   cadence implies one, a slot is minted with `predicted_from = cadence` and a wide tolerance.

**Both sources may mint.** Cadence mints so that silence is detectable; the calendar mints when it
names a release the cadence did not predict, because otherwise truth-table rows 5–6 (§6) have no
slot and therefore never arm the §6.2 poll ladder. Matching rule:

1. A calendar entry whose `(program, date)` falls within tolerance of an open cadence slot **updates**
   that slot's `scheduled_at` and source.
2. Otherwise it **mints** a scheduled slot.
3. An entry whose title cannot be mapped to a program mints a **program-unassigned** slot flagged for
   triage. It is never discarded.

The earlier concern — that title-parsing must not be load-bearing — is satisfied by §4.1's rule that
a slot **carries no claim about content**. A mis-parsed title costs one wasted poll window, not a
data error. Review §2.5's warnings still apply to *parsing*: feed titles carry no year, feeds are
hand-edited and hold ~12 entries, and ECEC labels reference periods as months while ECI labels them
as quarters.

When a matched calendar entry later moves or disappears: a moved date mutates `scheduled_at` (logged
to `slot_history`); a removed entry drops the slot back to `predicted` with its cadence estimate,
rather than leaving it pinned to a date BLS has withdrawn. Without this, a silent `.ics` edit after a
shutdown ages a batch of slots into spurious lateness.

**One calendar entry can fan out to several slots.** "Employment Situation" is simultaneously the
CES-N release and the CPS-LN release. The registry declares this fan-out explicitly.

### 4.3 Slot lifecycle

```
                 ┌─────────────┐
   cadence ─────►│  predicted  │
                 └──────┬──────┘
   calendar ────────────┼────────────► scheduled
                        │                 │  now ≥ scheduled_at − lead
                        │                 ▼
                        │             watching ──── artifact hash change ───► captured
                        │                 │                                     │
                        │                 │ past scheduled_at + grace           │ parse+validate ok
                        │                 ▼                                     ▼
                        └──────────►    late                                committed
                                          │
                              human triage│
                    ┌─────────────────────┼─────────────────────┐
                    ▼                     ▼                     ▼
                delayed              rescheduled            cancelled
             (incident-linked)     (new slot linked)     (human-only action)
```

Two additional terminal-ish states:

- `news_only` — reached when a slot with `expects_artifact_change = false` is satisfied: the news
  release or notice was observed and the data artifact correctly did not change. **Terminal, no
  triage item, no contribution to exit code 20.** Normal for QCEW news-release slots before
  2025-06-04 and for the CES preliminary benchmark. The *converse* — such a slot whose artifact
  **does** change — is a warning.
- `unscheduled` — an artifact changed with no due slot. An `out_of_cycle` slot is minted
  retroactively and the errata/notice matcher runs (§12.3).

A **reschedule is not a state transition**. It mutates `scheduled_at` on the existing slot (from a
notice, §12.6, or a calendar edit, §4.2), returning a `late` slot to `scheduled` if the new date is
still ahead. This is why §4.1 keys `slot_id` on the occurrence: the slot survives its own reschedule.

**The system never auto-cancels a slot.** Only a human cancels (§13.3).

### 4.4 Coverage and staleness monitoring

Because the system looks backward, "reference period `t` was never published" is not a slot state.
It is a derived monitor:

```
coverage(program) = {
  newest_observed_t_max,
  cadence_implied_t_max(now),
  staleness_periods = difference,
  last_committed_release_at,
}
```

Flag when `staleness_periods` exceeds a per-program tolerance (default: 1 period + grace). This
catches the shutdown case, the "we published but our parser silently dropped everything" case, and
the "cadence rule is wrong" case with one monitor — and it does so without materializing per-period
future expectations that a disruption would invalidate.

---

## 5. Program registry

### 5.1 The split: config vs. code

Program facts divide cleanly by volatility, and the split should follow that:

| Kind of fact | Where it lives | Why |
|---|---|---|
| URLs, artifact filenames, calendar titles, release times, poll tolerances, grace windows | **TOML config**, versioned in-repo | Changes without a release; operators must be able to edit under time pressure |
| Reference-period grammar, `ref_date` mapping, delta rules, benchmark predicates, cascade edges | **Typed Python** (`ProgramSpec`) with unit tests | Date arithmetic in a config DSL is how you get an untestable, unreadable rules engine |

A TOML DSL that can express "NSA from April of two years before the year of `t_max` through
December of the prior year" is a programming language with worse tooling. Write it in Python; test
it exhaustively; keep the volatile strings in TOML.

⚠ **EP does not fit this split**, and it is the only program that does not. Its artifact set is not
a stable list: each vintage links a different set of workbooks and matrix-detail URLs, and table
numbering moves between vintages (review §13.4). So `ArtifactSpec.locator` accepts a
**`DiscoveryRule`** as well as a URL: crawl the vintage landing page, extract every linked workbook
and matrix-detail URL, archive each under P1, and **persist the discovered URL set per vintage**.
A link-set diff under an unchanged decade label is then itself a correction signal, feeding
invariant 21.

### 5.2 `ProgramSpec` shape

```python
@dataclass(frozen=True)
class ArtifactSpec:
    key: str
    locator: str | DiscoveryRule              # a URL, or a crawl rule (EP — see §5.5)
    parser: str
    unit_key_columns: tuple[str, ...]         # series_id, or QCEW's code tuple — see §9.3
    measure_columns: tuple[str, ...] | None    # wide-form measures; None if long-form
    roster_artifact: str | None               # e.g. "ce.series"; None where BLS ships none
    authoritative_scope: ScopePredicate | None # what this artifact is authoritative FOR (§8.3)

@dataclass(frozen=True)
class ProgramSpec:
    key: str                                  # "ces-n"
    period_grammar: PeriodGrammar             # monthly | quarterly | annual | mixed (QCEW)
    ref_date: Callable[[int, str], date]      # (year, period_code) -> canonical date
    artifacts: tuple[ArtifactSpec, ...]       # what to fetch, in what order, how to parse
    cadence: Cadence                          # periodicity + nominal lag, for slot fallback
    notices_subject_area: str | None          # BLS subject area, NOT 1:1 with program — see §12.5
    classify: Callable[[ReleaseContext], ReleaseKind]
    footprint: Callable[[ReleaseKind, Period], Footprint]
    cascades_to: tuple[str, ...]              # outbound: validation hints only — never a write path
    depends_on: tuple[Dependency, ...]        # inbound: early-warning surfaces, incl. non-BLS (§12.10)
```

`authoritative_scope` is load-bearing and easy to overlook. An artifact is authoritative for a
**bounded slice** — a series roster crossed with a period set — and the differ may only emit
`deleted` rows inside that slice (§8.3). QCEW's quarterly singlefile is authoritative for one
quarter of one year, not for the program × `ref_year` partition; treating it as the latter
mass-deletes every other quarter on every release. Where an artifact declares neither a roster nor a
scope, the differ **emits zero `deleted` rows and raises a FLAG** rather than defaulting to
whole-partition authority.

`Footprint` is seasonality-aware and carries a semantics tag:

```python
@dataclass(frozen=True)
class Footprint:
    nsa: PeriodRange | None
    sa:  PeriodRange | None
    stages: Mapping[Period, PublicationStage]   # t_max -> first_prelim, t_max-1 -> second_prelim, ...
    semantics: Literal["exact", "lower_bound"]  # benchmarks are always lower_bound (P5)
```

### 5.3 The delta rules, read backward from `t_max`

This is the operative table. Every entry is "given a release whose newest reference period is
`t_max`, what does it touch."

**Three conventions, stated once, because getting any of them wrong silently corrupts the table:**

1. **Ranges are inclusive of `t_max`.** "Backward from `t_max`" includes `t_max` itself.
2. **Routine rows are unconditional; annual-event rows state only the *additional* reach.** Every
   annual event rides on a release that *also* carries its routine print — the February CES release
   is the January first preliminary **and** the benchmark. `classify()` therefore returns a
   **primary kind plus an ordered set of secondary kinds**, and `footprint()` returns the union of
   the components. A design that treats `benchmark` and `routine` as mutually exclusive predicates
   loses `t_max`'s own publication stage on every benchmark release.
3. **Reaches are written arithmetically, never as bare counts** (review §2.C1: only the arithmetic
   form is safe). `Y` is the calendar year of `t_max`.

⚠ **Benchmark naming is off by one from `t_max`** (review §2.C6). A benchmark carried with
`t_max = M01` of `Y` is named by the QCEW year it consumes: `benchmark_year = Y − 1`. Its artifact is
`bmrk{Y−1}-revisions.zip` with columns `AS_PREVIOUSLY_PUBLISHED_BMRK{Y−2}` / `AS_REVISED_BMRK{Y−1}`.
**Never derive the label from the reference month.**

| Program | Release kind | Predicate on `t_max` | Reference periods touched (backward from `t_max`) |
|---|---|---|---|
| **CES-N** | routine | *always* | `t_max` = 1st prelim · `t_max−1` = 2nd prelim · `t_max−2` = 3rd/final |
| **CES-N** | benchmark | month = M01 of `Y` | NSA `Apr(Y−2) … Dec(Y−1)` (21 mo) · SA `Jan(Y−5) … Dec(Y−1)` (60 mo) · **lower bound** · 21-mo span confirmed for national CES by the CES FAQ (R14) |
| **CES-N** | prelim benchmark | separate notice, Aug/Sep | analytical announcement; **may not touch the flat file at all** (§4.1) |
| **SAE** | routine | *always* | `t_max` = prelim · `t_max−1` = final. **N=2, not 3 — do not port CES's rule** |
| **SAE** | benchmark | month = M01 of `Y` | NSA `Apr(Y−2) … Dec(Y−1)` (QCEW replacement runs to `Sep(Y−1)`, then re-estimation) · SA `Jan(Y−5) … Dec(Y−1)` · **lower bound; publication month drifts Feb–Apr — do not predicate on it** |
| **JOLTS** | routine | *always* | `t_max` = prelim · `t_max−1` = final · **plus concurrent SA may move older SA values every month** |
| **JOLTS** | benchmark | month = M01 | SA **and** NSA `Jan(Y−5) … Dec(Y−1)` — NSA moves because alignment couples it to the SA process |
| **JOLTS-STATE** | routine | *always* | `t_max`; **routine `N` undocumented** — FLAG any prior-period change (§20 issue 11) |
| **JOLTS-STATE** | benchmark | month = M01 | Aligned to JOLTS national, CES, and QCEW **through Q4(`Y−1`)** (review §6.3, `1/3 · quoted`) |
| **CPS-LN** | routine | *always* | `t_max` only. No prior period reprinted |
| **CPS-LN** | annual SA revision | month = M12 | SA `Jan(Y−4) … Nov(Y)` relative to the December `t_max`; **NSA untouched** |
| **CPS-LN** | population control | month = M01 | **zero back-revision** — a Dec→Jan discontinuity, not a revision. Necessary but *not sufficient* predicate (§13.4) |
| **BED** | routine | *always* | `t_max` only |
| **BED** | annual revision | quarter = Q1 | NSA `Q1(Y−1) … Q4(Y−1)` · SA `Q1(Y−5) … Q4(Y−1)` |
| **QCEW** | full data | *always* | active snapshot: Q1 → 5 quarters (own + 4 finalizing prior-year); Q2 → 2; Q3 → 3; Q4 → 4. **Plus annual:** a Q4 release prints `annual(Y)` **preliminary**; a Q1 release prints `annual(Y−1)` **final** |
| **QCEW** | news release | any quarter | **no data-artifact change** — `expects_artifact_change = false` (pre-2025-06-04 era) |
| **OEWS** | annual vintage | new May reference year | new vintage; **does not revise prior vintages** |
| **ECI** | routine | *always* | `t_max` only; NSA final on publication |
| **ECI** | SA readjustment | quarter = Q1 (M03 reference) | SA `Q1(Y−5) … Q4(Y−1)` plus `t_max` |
| **ECI** | reweight/rebase | **announced only** — not derivable from `t_max` | detect from notices; base-period label change is the structural marker |
| **ECEC** | routine | *always* | `t_max` only; constant-dollar (real) series additionally re-based annually — a deflator change, **not** a revision of nominal levels |
| **EP** | new vintage | base–target decade label changes | new vintage; not a revision |
| **EP** | wage refresh | `ep` flat file, ~April | semiannual refresh of wages under an unchanged decade label — **not** a correction (review §13.1, `1/3 · quoted`) |
| **ALL** | **correction** | matched errata/notice row **OR** changed hash under an unchanged vintage key | **unbounded, `lower_bound`.** Applies to every program as a *secondary* kind — a correction bundled into a scheduled release is otherwise never classified |

Two structural notes carried from the review:

- **The forward view is derived.** "How many times will 2026Q1 QCEW be printed, and when?" is
  answered by inverting this table over the calendar's known release dates. It is a query, not a
  stored expectation.
- **Consistency property, property-tested:** `t ∈ footprint(release) ⟺ release ∈ prints_of(t)`.
  A property test over random programs and periods keeps the two views from diverging.

### 5.4 `ref_date` canonicalization

Every reference period maps to a single canonical `ref_date` = first day of the period, plus a
retained `period_code` and a `period_kind`:

| Program shape | Period codes | `ref_date` | `period_kind` |
|---|---|---|---|
| Monthly | `M01`–`M12` | `Y-MM-01` | `period` |
| Monthly annual average | `M13` | `Y-01-01` | `annual_average` |
| Quarterly | `Q01`–`Q04` | `Y-{01,04,07,10}-01` | `period` |
| Quarterly annual average | `Q05` | `Y-01-01` | `annual_average` |
| QCEW annual (avg annual pay) | `A01` | `Y-01-01` | `annual_average` |
| OEWS vintage | May-`Y` | `Y-05-01` | `vintage` |
| EP vintage | base–target | `base_year-01-01` | `vintage` |

**Annual averages are kept, not dropped**, but flagged — they are genuinely published values, and
silently discarding them makes the store non-reconstructible against the raw file. They are excluded
from time-series views by the `period_kind` filter, never by absence.

`period_code` stays in the primary key: `M01` and `M13` both canonicalize to `Y-01-01`, and QCEW
alone carries monthly employment, quarterly wages, and annual pay in one program.

---

## 6. The three-signal reconciliation

The user requirement — operations driven by *calendar* and *feed* **and** *the actual existence of a
release* — is implemented as an explicit truth table over three independent signals. This is the
core control loop, and writing it out as a table (rather than as nested conditionals discovered
incrementally) is what keeps the disruption cases from being afterthoughts.

| Cadence expects | Calendar says | Artifact changed | Interpretation | Action |
|:-:|:-:|:-:|---|---|
| ✓ | ✓ | ✓ | Normal release | Capture → parse → commit |
| ✓ | ✓ | ✗ | **Late** — *only if* the slot has `expects_artifact_change = true` | Continue polling to grace deadline → triage as delay |
| ✓ | ✓ | ✗ | Slot has `expects_artifact_change = false` (QCEW news release pre-merge; CES preliminary benchmark) | → `news_only`. **Terminal. Not a delay, not an alert.** |
| ✓ | ✗ | ✓ | **Unannounced publication** — the ECEC-during-shutdown case (§12.5) | Capture → commit → flag `unannounced` |
| ✓ | ✗ | ✗ | Not yet scheduled | Hold `predicted` slot; triage if past cadence tolerance |
| ✗ | ✓ | ✓ | Release we don't model, or cadence rule wrong | Capture → commit → flag `cadence_gap` → triage |
| ✗ | ✓ | ✗ | Calendar entry for an unmodelled product | Triage: registry gap |
| ✗ | ✗ | ✓ | **Silent mutation** | Capture → match against errata rows → if no match, alert `unexplained_mutation` |
| ✗ | ✗ | ✗ | Nothing | — |

Two rows deserve emphasis. Row 3 (`unannounced publication`) is why **artifact polling never stops
during a shutdown** (§13.2). Row 7 (`silent mutation`) is the QCEW 2025-12-19 → 2026-01-07 case,
which is the system's canonical regression test (§18.2) — those dates carry an (open) verification
marker there (R16), with the verified 2019-09-09 QCEW reload as the documented fallback instance.

### 6.1 Change detection

Poll with `HEAD`, escalate to `GET` only on a signal:

```
HEAD → (Last-Modified, ETag, Content-Length)
  unchanged             → fetch_log{outcome: unchanged}, done. Cost: one request.
  Last-Modified or ETag changed → GET → stream to archive → SHA-256
      hash changed      → fetch_log{outcome: new_bytes}    → file_vintage → release_event
      hash unchanged    → fetch_log{outcome: restamp_only} → NO file_vintage
```

The arrows after `fetch_log` cross the P2 durability boundary: everything left of it is written by
the lock-free capture plane, everything right of it is materialized by the single writer (§17.3).
`outcome` is what carries the decision across, which is why it is a `fetch_log` column rather than
an in-process branch.

`restamp_only` is worth capturing explicitly. Review §15 notes ALFRED's semantic gap: "BLS
republished this series unchanged on date `D`" is a real and different fact from "BLS did not
publish on `D`," and only the former proves the value was still current. A re-stamped
`Last-Modified` with an identical hash is exactly that proof, at file granularity.

For surfaces where `HEAD` is unreliable (HTML pages behind the WAF), use conditional `GET` with
`If-Modified-Since` / `If-None-Match`.

### 6.2 Polling ladder

Per slot, from `scheduled_at`:

| Window | Interval |
|---|---|
| `scheduled_at − 5m` → `+30m` | 60 s |
| `+30m` → `+4h` | 5 min |
| `+4h` → end of day | 30 min |
| next day → `+grace_days` (default 3) | 6 h |
| beyond grace | slot → `late` |

Baseline sweep independent of slots: every in-scope artifact gets a `HEAD` at least daily, so row 7
of the truth table is detectable even when nothing is due.

### 6.3 Notices as an overlay on the truth table

A fourth signal — **program notices** (§12.5) — is deliberately *not* a fourth column in the truth
table. Adding it would double the table to sixteen rows, most of them meaningless. Notices are a
**prior** that changes the *action* in three specific rows without changing the interpretation:

| Truth-table row | Without a matching notice | With a matching notice |
|---|---|---|
| 2 — late | Poll to grace deadline → human triage | **Reschedule proactively**, before lateness is ever reached (§12.6) |
| 5 — unexpected scheduled release | Flag `cadence_gap` → triage | Classified from the notice (methodology change, new product, resumption) |
| 7 — silent mutation | Alert `unexplained_mutation` | Classified `correction`; alert suppressed, event explained |

Row 2 is the valuable one, and it inverts the delay workflow: BLS usually *announces* a reschedule
before the original date passes. A system that only reacts to lateness discovers on Friday morning
what BLS published on Tuesday.

### 6.4 The feed as a poll trigger

`bls.gov/feed/*.rss` is the third named driver, and it earns a decision rule of its own rather than
sitting in the truth table:

> **A new or changed feed entry for an in-scope program arms the §6.2 60-second ladder on that
> program's artifacts immediately, regardless of slot state** — minting an `out_of_cycle` slot if
> none is due.

The feed's value is *latency*, not authority. It frequently surfaces before a poll window would have
opened, and it is the cheapest signal that something is happening off-schedule.

Two hard limits, both from the domain reference:

- **The feed never sources `knowledge_time`.** It says *when to look*, never *what time to record*
  (review §17 gap 5: BLS never defines which timestamp is authoritative, and an Atom `<updated>`
  field may reflect a later page edit rather than first availability). §8.2 is unaffected by feed
  state.
- **Key entries by the archive-link href** (`.../archives/{slug}_MMDDYYYY.htm`), never by Atom `id`
  or title — ids are edited in place after publication and titles carry no year. Feeds retain only
  ~12 entries, so an outage longer than the feed window loses events unrecoverably; the daily
  baseline `HEAD` sweep (§6.2) is the backstop that makes that survivable.

---

## 7. Data plane: capture

### 7.1 Transport profiles

Three surfaces with genuinely different behavior; one client abstraction with three configured
profiles:

| Profile | Hosts | Notes |
|---|---|---|
| `flatfile` | `download.bls.gov`, `data.bls.gov` | Primary ingest. HTTP/2, connection reuse, descriptive contact User-Agent — **mandatory from the first request, not a courtesy: the flat-file host 403s bare user agents, and BLS policy permits real-time blocking of non-compliant robots (R12)**. Conservative concurrency (≤4). Streaming downloads. |
| `api` | `api.bls.gov` | Spot checks only. **Never trust HTTP 200 or `REQUEST_SUCCEEDED`** — inspect the `message` array and per-series data presence. Key expires annually; alert on auth messages. |
| `html` | `www.bls.gov` | Known to 403 ordinary fetchers. Full browser-shaped headers, HTTP/2, low rate. **Pluggable backend** (§7.2). |

All profiles: `tenacity` retry with jitter on 5xx/429/timeout, no retry on 4xx except 403/429,
per-host token bucket, and every request — including failures — appended to `fetch_log`.

### 7.2 The 403 problem

`www.bls.gov` returns 403 to plain `httpx` even with a contact User-Agent per BLS's stated policy
(review §0.3). The design does not pretend otherwise. Mitigations, in order, and the last one is the
real one:

1. **Prefer non-HTML surfaces.** The `.ics` schedule feed and the Atom release feeds cover most of
   what the schedule pages carry.
2. **Browser-shaped `httpx`** — realistic UA, `Accept`, `Accept-Language`, `Accept-Encoding`,
   HTTP/2 enabled, keep-alive. This defeats naive filters; it may not defeat TLS-fingerprint ones.
3. **Pluggable `HtmlFetcher` backend.** If (2) fails, a headless-browser fetcher runs as a separate,
   low-frequency worker. Results are cached to the object store keyed by `(url, date)`, so the hot path reads
   the cache, not the live site. Errata and schedule pages change daily at most; a browser fetch a few
   times a day is entirely adequate.
4. **Wayback Machine** as a secondary source for the *archived* news-release indexes — genuinely
   useful for §11.3 backfill regardless of the 403 question.
5. **Degradation is designed in (P9).** If all HTML fetching fails, the system loses scheduled
   times, errata pre-arming, and EP. It does not lose flat-file ingest. Every HTML-derived field is
   nullable and every code path that reads one has a defined behavior when it is null.

### 7.3 The archive

```
s3://<raw-bucket>/raw/blob/sha256=<hh>/<hh>/<full-wire-sha256>
```

Two-level hex fan-out. Bytes stored **exactly as received**, including the original
`Content-Encoding`.

**Two hashes, not one.** A single hash cannot serve as both the archive key and the change signal:

| Hash | Computed over | Used for |
|---|---|---|
| `wire_sha256` | Bytes as they arrived off the socket | The blob key; the forensic record |
| `content_sha256` | After transfer-encoding decode | **Change detection** (§6.1) |

If change detection keyed on the wire hash, a server-side compression change — a CDN switching from
gzip to brotli, or re-compressing at a different level — would manufacture a `file_vintage` and a
`release_event` for a file whose contents never moved. In an append-only store (P7) that fabricated
vintage is permanent. Conversely, keying the archive path on the decoded hash would make the object
key a function of the decompressor's version.

Consequently §7.1's `flatfile` profile **pins its `Accept-Encoding`**, so archived bytes are
reproducible across implementations and across time.

Archive protection is stated as requirements on the endpoint in §17.4, not as one vendor's settings.
The one that constrains **setup** rather than operation: object-lock retention is commonly settable
only at bucket creation, so the archive bucket must be created with immutability configured before
the first byte lands. Verify this against the specific endpoint before capture begins — it is the
one storage decision that cannot be corrected later.

Sidecar metadata goes to `fetch_log`, not to object tags:

```
fetch_log (append-only JSONL, partitioned dt=YYYY-MM-DD)
  request_id, url, method, profile, requested_at, status_code,
  http_last_modified, etag, content_length, content_encoding,
  request_headers, response_headers,   # complete header blocks, verbatim (R10)
  wire_sha256, content_sha256 (null on HEAD), blob_key (null on HEAD), byte_size,
  outcome enum: unchanged | restamp_only | new_bytes | error | blocked
  error_class, duration_ms, slot_id
```

This log is the forensic record. When a vintage looks wrong six months later, the question "what
exactly did we ask for and what exactly came back" must be answerable — and R10 makes the answer
complete: both header blocks are stored verbatim rather than as a curated field subset, which the
external review's WARC comparison identified as the one forensic loss of the blob + JSONL split.
(Adopting WARC itself as the capture container was considered and **rejected** — R9, §21: its
replay tooling serves a §1.3 non-goal, content-addressed blob keys are load-bearing for P2's
lock-free capture idempotency, and with full headers kept, a later lossless WARC wrap stays open.)

It is also, per P2, **half the durability boundary** — which fixes two properties that would
otherwise be implementation detail:

- **One object per process-run, never an append in place.** Object storage has no append; the
  partition holds `dt=<date>/<run_id>.jsonl`, so concurrent lock-free capture processes never
  contend. This is already the §9.2 layout; P2 makes it load-bearing.
- **It carries every field `file_vintage` needs** — both hashes, `blob_key`, `byte_size`,
  `http_last_modified`, `etag`, `slot_id` — because the writer materializes that row from this log
  and nothing else.

### 7.4 `file_vintage`

Materialized by the single store writer (§17.3) from every `fetch_log` record with
`outcome = new_bytes` that does not yet have a row — i.e. only on a `content_sha256` change. It is a
derived table (P2), so a rebuild from `log/fetch/` is always available and `sequence` is assigned at
materialization, in `(requested_at, request_id)` order, by the one process allowed to assign it.

```
file_vintage
  file_vintage_id      str    # sha256(url|sha256|first_observed_at)
  program, artifact_key, url
  wire_sha256, content_sha256, blob_key, byte_size
  http_last_modified, etag
  sequence            int    # monotonic; the deterministic tiebreaker for §10.1
  first_observed_at    datetime(UTC)   # := fetch_log.requested_at — NEVER the materialization time
  slot_id              str, nullable
  release_event_id     str, nullable   # set at commit
  parse_state          enum   # pending | parsed | failed | skipped
  parse_error          str, nullable
```

⚠ **`first_observed_at` is `fetch_log.requested_at` of the originating record, copied verbatim.**
Stamping it at materialization instead is the obvious implementation and it breaks three things
simultaneously, none of them loudly:

1. `file_vintage_id` hashes it, so a rebuild from `log/fetch/` would mint *different* ids — which
   breaks §8.3's `file_vintage_id` idempotency token (retries stop being no-ops) and falsifies P1's
   claim that everything derived is rebuildable by deterministic replay.
2. §8.2's `early_artifact` / `late_artifact` branches compare it against the scheduled instant. A
   writer backlog would manufacture `late_artifact` flags on releases that in fact arrived on time.
3. N2 (`knowledge_time ≤ first_observed_at + poll_tolerance`) would loosen by exactly the
   materialization lag, quietly widening the one assert that catches clairvoyance.

The same rule governs every other `file_vintage` column: all of them are copied from the log record,
none are computed at materialization. `sequence` is the sole exception — it is assigned by the
writer, in `(requested_at, request_id)` order, because a monotonic counter is precisely the thing
only a single writer can produce. That ordering is deterministic over the log, so `sequence` is
rebuildable too.

`parse_state = pending` is the retry queue. A parse failure leaves the bytes archived and the row
flagged; it never blocks the next capture.

---

## 8. Data plane: interpretation

### 8.1 Release classification

A `release_event` is created when a `file_vintage` is parsed. Classification is **structural**, per
review §17 gap 2 — never from headline text, which changes yearly and carries no benchmark marker.

Inputs to `classify()`:

1. `t_max` read from the parsed file.
2. The observed revision footprint (from the differ).
3. The series-catalog diff (additions, discontinuations, title changes).
4. Any matched BLS **notice** (§12.5) or **errata row** (§12.1).

Input 4 is not optional garnish. Review §17 gap 2 states the problem plainly: **there is no
universal machine-readable release-kind field** — nothing in any feed or file says "this is a
benchmark." Three release kinds in the §5.3 table are flatly *underivable* from `t_max` and can only
be classified from a notice: CES out-of-cycle corrections (§4.4 — "requires a CES notice **plus** a
checksum change"), ECI reweight/rebase (§11.3 — "announced per event", no fixed cadence), and the
delayed CPS population control (§7.4 — off-cycle introduction **plus** a historical rewrite **plus**
a population-control notice). Without a notices channel, those three degrade to
`unexplained_mutation`.

```
release_event
  release_event_id, program, slot_id
  release_kind          enum    # PRIMARY kind: routine | new_vintage | seed
                                # | unchanged_republication | out_of_cycle
  secondary_kinds       list[enum]  # benchmark | annual_revision | population_control
                                # | sa_revision | correction | reweight | wage_refresh
                                # — an annual event ALWAYS rides on a routine release (§5.3)
  t_max_period_code, t_max_ref_date
  knowledge_time        datetime(UTC)          # see 8.2
  knowledge_time_source enum
  bls_scheduled_at, bls_notice_date, first_observed_at, http_last_modified
  news_release_observed_at   datetime, nullable
  expected_footprint    json    # from the delta rule
  observed_footprint    json    # from the differ
  footprint_divergence  json    # findings, not errors
  provenance            enum
```

Keeping `expected_footprint` and `observed_footprint` side by side in the same row is what makes P5
operational: the divergence is data you can query and trend, not a log line.

### 8.2 Resolving `knowledge_time`

Invariant 4 makes `knowledge_time` part of the primary key; review §17 gap 15 names the scheduled
time as canonical. But that gap scopes the scheduled time's authority to skew of *seconds to
minutes*, and several paths in this design can populate a "scheduled" date that is off by weeks. A
naive `coalesce` therefore back-dates knowledge and produces exactly the clairvoyance §1.1 exists to
prevent. The rule, in full:

**Step 1 — only a calendar-sourced time is eligible.**

```
eligible_scheduled = bls_scheduled_at WHERE scheduled_at_source ∈ {ics, schedule_page}
```

A slot's `scheduled_at` and a release event's `bls_scheduled_at` are **different fields**. Three
sources populate the former but must never reach the latter:

- `cadence_estimate` — a periodicity guess (§4.2). OEWS is the standing case: review §2.A5 records
  the May 2025 vintage published 15 May 2026 against an expected late-March window, and §14 records
  that OEWS has *no documented intraday publication time* at all.
- An errata `correction_due_date` (§12.2 rule 2) — a forward-looking, date-typed *prediction*.
- A `delayed` slot's stale original date — `--recheck-at` (§13.3) deliberately does not replace it.

These arm the poll ladder and nothing else.

**Step 2 — resolve, with both skew directions pinned.**

```
if eligible_scheduled exists:
    if first_observed_at <  eligible_scheduled − tolerance:   # early web post
        knowledge_time = eligible_scheduled;  flag early_artifact
    elif first_observed_at <= eligible_scheduled + tolerance: # normal
        knowledge_time = eligible_scheduled
    else:                                                     # late / delayed / resumed
        fall through;  flag late_artifact
knowledge_time = coalesce(<from above>, bls_notice_date_as_instant, first_observed_at)
knowledge_time_source ∈ {scheduled, notice, observed}
```

`tolerance` is the §6.2 poll resolution plus the program's grace. Beyond it, a scheduled instant is
no longer evidence of anything — a release delayed by a shutdown and published three weeks late must
not inherit the date on which BLS *did not* publish.

**Step 3 — never widen a date into an instant in the earlier direction.** `bls_notice_date` is
date-typed; naively promoting it to `00:00Z` places it *before* the same day's 10:00 ET release and
permanently masks the correction in as-of ordering. So:

```
bls_notice_date_as_instant = max(end_of_day(bls_notice_date, America/New_York), first_observed_at)
```

**Step 4 — assert the result.**

```
[ASSERT] knowledge_time <= first_observed_at + poll_tolerance
         — except the explicitly flagged `early_artifact` branch.
```

A `knowledge_time` preceding observed availability by more than the polling resolution is
clairvoyance, and it is the one arithmetic error in this section that corrupts the store rather than
merely annoying an operator.

⚠ **Citation correction.** The 2024 CES benchmark incident (review §4.5) was a **late** web post —
the file appeared roughly 30 minutes *after* the scheduled time, while some financial firms obtained
it early through another channel. It is evidence for the `late_artifact` branch, not the early one.
The early branch is retained because BLS *can* post before the embargo minute, but it is not what
that incident demonstrates.

`bls_scheduled_at` is stored as an absolute UTC instant *and* with its source IANA zone
(`America/New_York`), because BLS release times are ET and the DST boundary falls inside the
release calendar.

### 8.3 The differ and `current_state`

The observation store is a **change log**: a row is written only when something about an
observation differs from what was previously effective. Determining that requires knowing the
currently-effective row for every `(unit_key, measure_code, ref_date, period_code)`. For QCEW that is millions of
cells; an anti-join against all history on every release does not fit in 8 GB.

The standard fix, and it is required for this design to be implementable on the stated hardware:
maintain a derived **`current_state`** table — one row per key, latest effective value — partitioned
identically to the change log (`program` × `ref_year`) so every join is partition-local.

```
parse(file) [lazy]
  → strip padding, cast code columns to str
  → for each ref_year partition touched:
        join current_state[program, ref_year] on unit_key   IN THE ARTIFACT'S NATIVE WIDE FORM
        compare measure columns column-wise
        MELT TO LONG AFTER THE DIFF — only changed cells are ever materialized
        emit `deleted` rows ONLY for keys inside artifact.authoritative_scope
  → sink_parquet (streaming)
```

**Diff wide, melt after.** Melting to long form *before* the join multiplies the frame by the
measure count — roughly 8× for QCEW — and puts both sides of the join outside the memory envelope.
The comparison is naturally column-wise; the long form is a storage shape, not a comparison shape.

**`deleted` is scoped by the artifact, never by the partition.** This is the correction that matters
most here. An artifact is authoritative for a bounded `(roster × period set)` slice
(`authoritative_scope`, §5.2). QCEW's quarterly singlefile covers *one quarter*; if the differ
treated absence-from-this-file as deletion across the whole `program × ref_year` partition, **every
non-Q1 release would mass-delete the other three quarters.** Where an artifact declares neither a
roster nor a scope, the differ emits **zero** `deleted` rows and raises a FLAG. Silence is the safe
default; whole-partition authority is not.

**Commit ordering, because a cross-table transaction does not exist.** Nothing in §9.1 spans two
tables atomically, so "append change rows and upsert `current_state` atomically" is not
implementable. Single-writer mode (§17.3) does not help: it removes *concurrent-writer* races, not
*crash-mid-sequence* races, and a commit that dies between step 1 and step 3 below leaves exactly
the same inconsistency whether or not another writer existed. The protocol is therefore required in
full even with one writer:

1. **`observation` is the sole authority and commits first.** `current_state`,
   `file_vintage.release_event_id`, and the slot transition are downstream and are permitted to lag.
   ⚠ Those last two are **in-place field updates**, which an append-only Parquet store has no verb
   for. They are not appends and must not be modelled as such — see §9.1's mutable-table rule.
2. The append is made **idempotent by construction**: every object is named from the
   `release_event_id` that produced it (§9.1), so a retried commit rewrites identical keys with
   identical bytes. There is no token to reconcile and no duplicate to detect — the retry is a
   no-op because the write is addressed, not appended.
3. `current_state` records the newest `release_event_id` it was built from. On startup, if it
   lags, it is **rolled forward from the change log before any diffing occurs**.
4. A cheap window-function invariant catches both retry duplication and `current_state` divergence:
   for every `revised` row, `prev_value` must equal the `value` of the immediately preceding row for
   that key.

`current_state` is fully rebuildable from the change log, so it does not weaken P1 — and step 3 is
what makes that rebuildability operationally load-bearing rather than theoretical.

**Precision handling.** ECI moved to three decimal places with the June 2025 release (invariant 22);
a naive numeric diff reports the entire file as revised. The registry carries per-program
`precision_change` events; the differ rounds both sides to the coarser precision at such an event
and emits `change_kind = precision_only` where they then match. Comparison is always on parsed
numerics, never on raw strings.

### 8.4 The series catalog

BLS ships `<prefix>.series` alongside the data file. Capture and diff it per vintage:

```
series_catalog_vintage
  program, series_id, file_vintage_id, knowledge_time
  title, seasonality, begin_period, end_period,
  dimension_codes json,        # decoded via the program's mapping files
  metadata_hash str,
  status enum                  # present | added | discontinued | retitled | recoded
```

This delivers P6 and answers ALFRED's semantic gap at series granularity: the roster proves a series
was *present and unchanged* in a release, which the change log alone cannot.

---

## 9. Storage layout

### 9.1 Table format: partitioned Parquet, committed by a manifest

**Plain Hive-partitioned Parquet for every ledger and store table. No transactional table format.**

**Chosen on honest grounds, not forced (R2).** The missing conditional PUT forces *single-writer
mode* (§1.4); the external review corrected this section's earlier claim that it also forces plain
Parquet. Transactional formats can commit without object-store compare-and-swap — Iceberg through a
catalog transaction, DuckLake through a SQL metadata database, delta-rs in explicit single-writer
mode. Under §1.4 the catalog variants remain unavailable for a *structural* reason — a catalog is a
second durable system, and the object store is the only durable storage this deployment has — but a
single-writer table format is genuinely possible. Plain Parquet is therefore a **choice**, made on
one ground: zero external dependencies in the custody path. Once a single writer is declared, the
format's central feature — concurrency control — is inert, while its costs remain: a commit log to
maintain, a version history to vacuum, and a safety property resting on our lease either way. What
is bought is operational simplicity; what is paid is listed below, honestly.

What a table format would still have given, and where each is answered instead:

| Would have given | Answered by |
|---|---|
| Atomic multi-file commit | **The manifest protocol below** — one atomic single-object PUT, which §1.4 guarantees |
| Idempotent retry | **Deterministic file naming** — a retry rewrites the identical key with identical bytes. Strictly simpler than a transaction token |
| Schema enforcement on write | An explicit declared schema per table, asserted at the commit boundary by the validation engine (§14) — which already runs there |
| File-level column statistics / scan pruning | **Manifest statistics (R4), below** — recorded at commit, used by the as-of query and the differ |
| Snapshot isolation for naive external readers | **Partly conceded.** The supported consumer interface is the CLI (§1.3); a direct-Parquet consumer must resolve `_current` and the §9.1 semi-join, which a table format would have handled for any reader |
| Compaction | §17.4's generation pointer, below |
| Time travel over the store itself | **Genuinely lost.** Accepted: P1 makes the byte archive the audit surface, and §17.4 already argued store-version retention buys nothing because replay selects on `knowledge_time`, a data predicate |

#### The commit protocol

The design already has a manifest and did not notice: **a row is visible when a `release_event`
points at it.** That makes commit an application-level property, and single-object PUT atomicity is
the only primitive needed.

```
1. write observation Parquet, one object per touched partition, named <release_event_id>.parquet
2. write series_catalog / findings rows for the release, same naming rule
3. PUT the release_event record  ← THE COMMIT POINT. One object. Atomic.
```

A crash before step 3 leaves data files that **no committed event references**, so no reader can see
them; they are orphans, not corruption. A retry rewrites the same keys with the same bytes and
proceeds. There is no partially-visible state at any instant.

Visibility is therefore *defined*, not merely observed:

> A row in `observation` is visible **iff** its `release_event_id` appears in a committed
> `release_event` object.

**Every table written during a commit resolves visibility the same way**, not `observation` alone.
`series_catalog_vintage` rows carry `release_event_id` and are semi-joined identically — otherwise an
interrupted commit leaves orphan catalog rows that the next release's catalog diff (§8.4) reads as
real, reporting a series as present that was never committed. That is invariant 6 failing silently,
one table over from where §9.1 was aimed.

⚠ **`finding` is the one deliberate exemption.** A finding written by a commit that then aborted is
still a true diagnostic — often the very reason it aborted (§14.1's `[ASSERT]` path writes a finding
*and* blocks the commit). Gating findings on the manifest would hide exactly the records an operator
needs. Findings therefore reference `release_event_id` for correlation but are visible on write.

#### Manifest statistics (R4)

The `release_event` manifest additionally records, **per object it commits**, the row count and the
min/max of `knowledge_time` and of the unit-key columns — the hand-rolled equivalent of a table
format's file statistics, which is the one concrete loss the external review identified in
rejecting Iceberg. Two consumers: the as-of query (§10.1) drops objects whose `knowledge_time`
minimum exceeds `D` without opening them, and the differ (§8.3) skips objects outside the
artifact's key range. Compaction preserves the mechanism: `gen=<n+1>/` carries a `_stats.json`
recording the same statistics for the compacted objects, written **before** the `_current` swap
(§17.4), so a reader always finds statistics beside whichever generation the pointer names.

#### Manifest attestation (R5)

The store's central claim — *these bytes were observed at this instant* — rests, as specified so
far, entirely on self-reported logs; an operator who back-dated `knowledge_time` would leave no
inconsistency for `verify` to find. The external review ranked third-party attestation its
highest-EV improvement, and it has the same at-creation-or-never shape as §7.3's object-lock rule:
a proof over today's manifest can only be made today.

The lightweight form is adopted (adjudicated — §21): after the `release_event` PUT, the writer
submits the manifest object's SHA-256 to a trusted timestamping service — OpenTimestamps, or an
RFC 3161 TSA — and stores the returned proof under the **archive bucket**
(`attest/<release_event_id>`, §9.2), because a proof is the one artifact class besides the archive
itself that cannot be regenerated after the fact.

Three rules keep it lightweight:

- **Attestation never blocks a commit.** The stamp is fired after the commit point and retried by
  subsequent `tick --write` runs; an unreachable service degrades to a backlog, never an abort.
- **The outage fallback is a batch root.** Pending manifest hashes fold into a Merkle root stamped
  as one proof when the service returns; per-event proofs derive from the batch.
- **Backlog is monitored, not silent.** Invariant N10 flags any committed event lacking a proof
  beyond tolerance; `doctor` reports attestation state; `verify attest` (§15) re-verifies stored
  proofs against the archived manifests.

Loss of a proof degrades *evidence*, never *data*: the store remains fully rebuildable (P1) — what
is lost is only third-party demonstrability of *when*. This is why proofs live under the immutable
bucket's protections but do not join `raw/` + `log/fetch/` as rebuild inputs; §10.4 is unchanged.

#### Mutable tables

The manifest governs **vintage rows** — things that are appended and never change. Three tables are
not of that kind, and an append-only store has no verb for them:

| Table | What mutates | Why it cannot be an append |
|---|---|---|
| `release_slot` | `scheduled_at`, `state` (§4.1) | A reschedule mutates the existing slot precisely so foreign keys survive it (§4.1); appending a second row would reintroduce the orphaning that keying on the occurrence was designed to prevent |
| `file_vintage` | `release_event_id`, `parse_state` (§7.4) | Set after the fact, on a row written earlier |
| `current_state` | everything, per partition (§8.3) | Derived fold, rewritten whole |

All three use the **generation swap** of §17.4 — the same `_current` pointer primitive as compaction,
which is a whole-partition atomic replace and is therefore exactly the right verb. They are small
(slots and vintages number in the thousands, not millions), so rewriting a partition is cheap.

Two properties follow, and both matter:

- **They are governed by the pointer, not the manifest.** There is no `release_event` semi-join on a
  slot; the swap itself is the atomicity. Applying the manifest rule here would be a category error.
- **P7 is not violated.** P7 forbids mutating a stored *vintage* — an observation. A slot's scheduled
  date is an expectation, not an observation, and §4.1 already keeps its full mutation history in the
  append-only `slot_history`. `file_vintage`'s mutable fields are bookkeeping; its identity, hashes,
  and `first_observed_at` are immutable by §7.4's purity rule.

The as-of query (§10.1) enforces this with a semi-join against `release_event`, which is small
(thousands of rows, one per release ever) and pushes down cleanly. Orphan sweeping is hygiene run by
the writer at startup — **not** a correctness dependency, because visibility never depended on the
orphans being gone.

⚠ Orphan deletion does **not** violate P7. P7 forbids mutating a *stored vintage*; an orphan was
never committed and never visible. The bytes it was derived from remain in the archive regardless.

#### Storage options

Every read and write path takes the same options, resolved once:

```python
storage_options = {
    "aws_endpoint_url":      AWS_ENDPOINT_URL,     # never a constant, never defaulted to AWS
    "aws_access_key_id":     ...,
    "aws_secret_access_key": ...,
    "aws_region":            AWS_REGION,           # object_store requires one even where ignored
    "aws_allow_http":        "true",               # iff the endpoint scheme is http
}
```

⚠ **`aws_allow_http` is not optional against a plain-`http` endpoint**, and omitting it is the
standard first-run failure. ⚠ These are `object_store` key names, which differ from boto3's
(`aws_endpoint_url`, not `endpoint_url`); the overlapping subset makes a wrong mapping *look*
correct while the client silently falls back to ambient AWS credential resolution. §16.1 makes
producing this dict a single function with a live round-trip test for exactly this reason.

Dropping the format dependency is **reversible**: the change log is derived (P1), so adopting a
table format later is a rebuild, not a migration. §20 issue 3 records what would justify it.

### 9.2 Layout

Two buckets, addressed as `s3://…` against `AWS_ENDPOINT_URL`. The archive is separated so that
delete-deny and retention are whole-bucket statements, clear of the tables compaction rewrites —
see §17.4.

```
s3://<raw-bucket>/                              # immutable; no delete permission (§17.4)
  raw/blob/sha256=<hh>/<hh>/<sha256>            # content-addressed, never expires
  attest/<release_event_id>                     # timestamp proofs (§9.1, R5) — evidence-class, not a rebuild input

s3://<bucket>/                                  # versioned, mutable
  log/fetch/dt=<YYYY-MM-DD>/<run_id>.jsonl      # every HTTP interaction; one object per run (§7.3)
  ledger/                                        # Parquet, partitioned as shown
    release_slot/            program=<p>/{_current,gen=<n>/}   # mutable — generation swap
    release_event/           program=<p>/       # THE MANIFEST — commit point for §9.1
    file_vintage/            program=<p>/{_current,gen=<n>/}   # mutable — generation swap
    slot_history/            program=<p>/       # append-only audit of every slot mutation
    series_catalog_vintage/  program=<p>/
    errata_row/
    incident/
    finding/                 program=<p>/
    triage_action/
    knowability/             program=<p>/
  store/                                         # Parquet, partitioned as shown
    observation/             program=<p>/ref_year=<yyyy>/_current
                             program=<p>/ref_year=<yyyy>/gen=<n>/<release_event_id>.parquet
    current_state/           program=<p>/ref_year=<yyyy>/_current
                             program=<p>/ref_year=<yyyy>/gen=<n>/state.parquet
  ops/
    lease/writer.json
    cache/html/<url-hash>/<date>.html
    report/<date>/triage.md
```

Partitioning by `program` × `ref_year` keeps every differ join partition-local and every as-of query
prunable. Seasonality is a column, not a partition — SA/NSA are always queried together for
footprint validation.

Two details carry weight:

- **`_current` and `gen=<n>/` are the compaction primitive** (§17.4), present on every partition from
  the first write — `gen=0` with no compaction yet. Retrofitting the indirection later would mean
  rewriting every reader.
- **`finding` is partitioned by `program`, not by date.** A `dt=` partition plus one write per hourly
  tick is the small-file worst case in the whole layout, and the date is available as a column for
  filtering.

### 9.3 `observation` — the change log

```
program              str
unit_key             str          # registry-composed identity of the measured unit — see below
measure_code         str          # which measure of that unit this row carries
ref_date             date
period_code          str          # M07, Q02, A01, M13
period_kind          enum         # period | annual_average | vintage
seasonality          enum         # S | U | NA
value                f64          # NULL if suppressed or withheld — never 0, never absent
value_precision      i8           # decimals as published
footnote_codes       str
publication_stage    str          # first_prelim | second_prelim | third_final
                                  # | preliminary | final | vintage | unknown
suppressed           bool
change_kind          enum         # first_print | revised | footnote_only | precision_only
                                  # | suppressed | unsuppressed | deleted | reinstated
prev_value           f64          # NULL on first_print; carried for cheap revision analytics
knowledge_time       datetime(UTC)
knowledge_time_source enum
release_event_id     str
file_vintage_id      str
provenance           enum         # observed | seed | reconstructed | external
```

**Primary key:** `(program, unit_key, measure_code, ref_date, period_code, knowledge_time)` —
invariant 4.

**Unit identity is registry data, not a universal `series_id`.** Keying the store on `series_id`
works for the LABSTAT programs and fails for the ones that have no series ID at all:

| Program family | `unit_key` composed from | `measure_code` |
|---|---|---|
| LABSTAT (CES-N, SAE, JOLTS, JOLTS-STATE, CPS-LN, BED, ECI, ECEC) | `series_id` | `value` (single measure per series) |
| QCEW | `area_fips` + `own_code` + `industry_code` + `agglvl_code` + `size_code` | `month1_emplvl`, `qtrly_estabs`, `total_qtrly_wages`, `avg_wkly_wage`, … |
| OEWS | `area` + `naics` + `occ_code` | `tot_emp`, `a_mean`, `h_median`, … |
| EP | matrix cell identity per vintage | matrix measure |

`ArtifactSpec.unit_key_columns` (§5.2) declares the composition. This is why at least one
non-LABSTAT program must enter at **M2** alongside CES-N and JOLTS (§19) — a validation set of two
LABSTAT programs cannot detect that the abstraction does not generalize, and discovering it at M7
means reworking the primary key after the store is full.

Dtype locks, from the domain reference and non-negotiable:

- **All code columns are `str`**, with leading zeros preserved (`state_code`, `industry_code`,
  `area_fips`, size classes). A cast to integer silently destroys `01` → `1` and every subsequent
  join. These columns are carried alongside `unit_key` for query ergonomics, not merely at the parse
  layer.
- **`value` is `Float64` and nullable.** A suppressed QCEW cell is `NULL` with `suppressed = True`,
  never `0` and never a missing row (invariant 15).
- **Keys are whitespace-stripped** — LABSTAT files space-pad both headers and cell values, and an
  unstripped join key matches nothing, emptying the frame without erroring.

### 9.4 `knowability` — pre-`T₀` publication dates only

```
program, ref_date, period_code
news_release_date        date, nullable   # when BLS announced it
data_available_date      date, nullable   # when the DATA ARTIFACT became retrievable — canonical
source                   enum   # newsrels_index | archive_index | qcew_dual_schedule
                                # | wayback | calendar | derived
source_url, evidence_title
confidence               enum   # exact | inferred | uncertain
```

**Two dates, not one**, because P3 makes the *artifact* canonical and the two genuinely differ.
Before 2025-06-04, QCEW's news release preceded the full-data update by ~14 days (review §2.B1: Q1
2024 → news Aug 21, full data Sep 4). A single `first_publication_date` sourced from the
news-release archive would answer "was this public on Aug 25?" with *yes* for data that did not exist
until Sep 4 — and invariant 17 explicitly requires both dates be recorded for that era.

`data_available_date` is what §11.2's "was it published as of `D`" query reads. For pre-cutover QCEW
it comes from BLS's two-column **"Schedule of News Releases and Full Data Availability"** page
(review §9.5) or its Wayback captures — **not** from the generic news-release-archive method, which
is forbidden from writing QCEW knowability rows for that era. Where only a news-release date is
recoverable, `data_available_date` is left null and the news date recorded as a lower bound with
`confidence = uncertain`; it is never promoted to an equality.

Deliberately a separate table with no join path into `observation.knowledge_time`. See §11.2.

---

## 10. Bitemporal semantics

### 10.1 The as-of query

```python
KEY = ["unit_key", "measure_code", "ref_date", "period_code"]

def as_of(D, program, *, ref_years=None, strict=True):
    if strict and D < T0[program]:
        return EMPTY            # the honest answer — see below
    provenances = ["observed", "seed"] if strict else ALL_PROVENANCES
    committed = (pl.scan_parquet(f"{LEDGER}/release_event/**/*.parquet", hive_partitioning=True)
                   .select("release_event_id"))      # the manifest — §9.1
    q = (pl.scan_parquet(f"{STORE}/observation/**/*.parquet", hive_partitioning=True)
           .filter(pl.col("program") == program)
           .filter(pl.col("provenance").is_in(provenances))
           .filter(pl.col("knowledge_time") <= D)
           .join(committed, on="release_event_id", how="semi"))   # visibility, not a filter
    if ref_years:                                    # partition pruning
        q = q.filter(pl.col("ref_year").is_in(ref_years))
    return (q.sort(["knowledge_time", "file_vintage_sequence"])
             .unique(subset=KEY, keep="last")        # explicit argmax, not group_by order
             .filter(pl.col("change_kind") != "deleted"))
```

Pure append-only + "latest row at or before `D`" avoids `valid_to` maintenance entirely. Closing
intervals would require mutating prior rows, which contradicts P7 and is expensive on object storage.

**The semi-join is the commit boundary, not an optimization.** §9.1 defines a row as visible iff a
committed `release_event` references it, so removing this join would expose orphan rows from an
interrupted commit — the one way this store can return values that were never published. It belongs
in the single `as_of` implementation that every consumer goes through, never re-derived by callers.
`release_event` is small (one row per release ever observed) and the join key is already a column,
so the cost is a broadcast semi-join, not a shuffle.

The manifest statistics (§9.1, R4) act before any of this: objects whose recorded `knowledge_time`
minimum exceeds `D`, or whose unit-key range misses the query's, are dropped unopened — the pruning
a table format would have supplied from file metadata.

**`strict` is a constraint on `D`, not a filter on rows.** This distinction is the difference
between a working store and a silently empty one, and it is worth stating why. §8.3 makes
`observation` a **sparse change log**: a key that has not been revised since `T₀` has exactly *one*
row, and that row is the seed row. Had `strict` been implemented as
`provenance == "observed"`, it would have removed that row at **every** `D`, not merely
`D < T₀` — so the production default would answer "CES has no data before the last benchmark
window," with no error and no log line. CES NSA reach is 21 months, so June 2015 is never rewritten
after `T₀` and would have been invisible forever.

The correct semantics:

| `D` | Admitted provenances under `strict=True` |
|---|---|
| `D < T₀` | none — returns empty |
| `D ≥ T₀` | `observed`, `seed` |
| any `D`, `strict=False` | all, including `reconstructed` and `external` |

A seed row *is* a genuine observation — of the file BLS was serving at `T₀` — which §11.1 argues in
its own terms. What `strict` excludes is `reconstructed` and `external` (§11.4, §11.5), never seed.

`T₀` is stored explicitly per program as the seed `release_event`'s `knowledge_time`, rather than
left implicit in the provenance column.

**Ordering is explicit.** `unique(subset=KEY, keep="last")` over a sort on
`(knowledge_time, file_vintage_sequence)` is an argmax that does not depend on within-group input
order surviving a lazy or streaming `group_by`. `file_vintage_sequence` (§7.4) is the deterministic
tiebreaker for two vintages resolving to the same instant — same-day errata being the realistic case.

### 10.2 The four invariants of the temporal model

1. **Monotonic visibility** (invariant 5). A query as of `D` returns exactly the vintages committed
   with `knowledge_time ≤ D`. Later corrections are invisible. This constrains *visibility*, not
   *mutability*: a later release legitimately revising an earlier reference period — the March 2026
   CPS case — is a new vintage, correctly stored and correctly invisible before its commit.
2. **Temporal order** (invariant 1). `ref_period_end ≤ first_observed_at`, with EP projections
   exempt by `release_kind`.
3. **Separate clocks** (invariant 3). `bls_scheduled_at`, `bls_notice_date`, `first_observed_at`,
   and `http_last_modified` are four distinct columns and are never collapsed.
4. **Provenance gating** (P8). Mixed-provenance results are opt-in and always labelled.

### 10.3 Reconstruction as a correctness test

Because the change log is derived, the store can prove itself: pick an archived `file_vintage`,
replay the change log up to its `knowledge_time`, materialize the file's logical contents, and
compare against a fresh parse of the archived bytes. Any mismatch is a real defect in the differ,
the commit path, or the ordering.

Three things this requires that are easy to leave unsaid:

1. **Replay runs non-strict, with seed rows in scope.** A vintage's logical contents include every
   value carried forward from the seed. The check therefore covers the differ *and* the seed
   snapshot — but it means §10.1's `strict=True` default is the wrong mode for this job.
2. **The comparison is two-stage, not two materialized frames.** A full-universe file does not fit
   twice in memory. Aggregate a hash per `(ref_year, key-bucket)` on both sides as a streaming
   aggregate, compare the hashes, and descend to row-level diff only for buckets that mismatch. This
   keeps coverage at 100% of rows while bounding memory to the bucket count.
3. **The 100%-on-parser-change pass needs a budget and a checkpoint.** "It costs nothing but compute"
   is false at QCEW scale; it is a resumable batch job with a stated cost model, not something to run
   inside a deploy.

Weekly it runs on a random sample. On a parser change it runs across that program's full vintage
history, resumably.

### 10.4 The rebuild order

P1 claims everything derived is rebuildable by deterministic replay from the archive. That claim is
now load-bearing in a way it was not when a table format's log recorded commit order: `release_event`
is the manifest (§9.1), and it is *itself* derived. "Just rebuild it" is not executable without a
stated sequence, and the moment it is needed is the worst moment to be deriving one.

```
raw/blob + log/fetch          the two irreplaceable artifacts (P2)
  → file_vintage              §7.4: copied from fetch_log, sequence assigned in log order
  → calendar / errata / notice ledger
                              re-parsed from the ARCHIVED HTML and .ics blobs, not refetched —
                              the live surfaces may 403 (§7.2) or have changed
  → release_slot              slot state replayed forward over the reconstructed calendar
  → release_event             §8.2 resolves knowledge_time; needs bls_scheduled_at from the slots
                              above, which is why the ledger precedes it
  → observation               re-diffed per vintage in `sequence` order (§8.3)
  → current_state             folded from the rebuilt change log
```

Two properties make this sound rather than aspirational:

1. **Every input is archived.** §7.3 archives HTML and `.ics` surfaces as blobs on the same terms as
   data files, precisely so that step 2 never depends on a live fetch.
2. **Every step is a pure function of its predecessors**, given `first_observed_at`'s purity rule
   (§7.4). A rebuild reproduces byte-identical `file_vintage_id`s, so it is idempotent against a
   partially-rebuilt store.

The `verify replay` job (§15) exercises the tail of this chain continuously. The head — rebuilding
the ledger from archived HTML — is exercised only on demand, and that asymmetry is itself a risk
worth naming: **it is the least-tested path in the design and the one that runs during a disaster.**

---

## 11. Backfill (everything before `T₀`)

Four phases, in dependency order, with sharply different epistemic status.

### 11.1 B1 — Seed vintage

Download every program's full published history at `T₀`. Commit as one `release_event` per program
with `release_kind = seed`, `provenance = seed`, `knowledge_time = T₀`.

**Every seed row carries `knowledge_time = T₀`.** Not the reference period's date, not its
reconstructed first-publication date. `T₀` is genuinely when these bytes were first observed, and
`T₀` is genuinely the earliest instant at which the store can vouch for these values.

### 11.2 The seed-honesty rule — invariant N1

> **`[ASSERT]` A row with `provenance ∈ {seed, reconstructed}` may never take its `knowledge_time`
> from `knowability.data_available_date` or `knowability.news_release_date`.**

This is the single most likely thing to implement wrong, because writing the reconstructed date onto
the observation row *feels* like an improvement — suddenly the store "has" fifteen years of history
with plausible dates. What it actually produces is a store that answers an as-of query for 2019 with
today's post-benchmark values, presented as if they had been public in 2019. Every backtest built on
it is silently wrong, and nothing in the output looks unusual.

The correct division:

| Question | Answered by | Available pre-`T₀`? |
|---|---|---|
| Was reference period `t`'s **data** retrievable as of `D`? | `knowability.data_available_date` | **Yes** |
| Was it **announced** as of `D`? | `knowability.news_release_date` | Yes — and it is a *different* date (§9.4) |
| At what stage was it, as of `D`? | `knowability` + the delta rules | Approximately |
| What *value* did BLS publish for `t` as of `D`? | `observation` | **No** — except QCEW 2017Q1+ (§11.4 B3a) |

Enforced structurally, not by convention: the loader that writes `observation` has no read access to
`knowability`, and a rule in the validation engine asserts it at commit.

### 11.3 B2 — Knowability reconstruction

Recover first-publication dates by scraping the news-release indexes:

- Current: `https://www.bls.gov/bls/newsrels.htm`
- Archived: `https://www.bls.gov/bls/news-release/home.htm`
- Per-release archive links: `.../archives/{slug}_MMDDYYYY.htm`

Method:

1. Enumerate archive links. **The `MMDDYYYY` in the href is the release date** — it is the only
   stable key. Atom ids are edited in place; titles carry the reference month but never the year.
2. Map `slug` → program via the registry.
3. Parse the reference period from the title, then **derive the year from the URL date** and the
   program's cadence lag, not from the title.
4. **Validate against the delta rule.** The reconstructed `(program, t, news_release_date)` must
   satisfy the cadence: `news_release_date ≈ end(t) + nominal_lag`, within tolerance. For QCEW
   before 2025-06-04, `data_available_date` comes from the dual-column schedule page instead (§9.4).

Step 4 is where this design earns something: the same rule that mints forward slots validates the
backward reconstruction. A mismatch is a triage item — it means either the scrape mis-parsed, or
BLS genuinely published off-cadence (a shutdown in the historical record, which is itself worth
knowing and worth recording as a historical `incident`).

Confidence is recorded per row (`exact` where the archive link is unambiguous, `inferred` where the
year came from cadence, `uncertain` where the title parse was ambiguous). Uncertain rows are
surfaced for human confirmation rather than silently trusted.

Where the current index is 403-blocked, the Wayback Machine carries these index pages and is a
legitimate primary source for a historical index — this is one place where the fallback is not a
degradation.

### 11.4 B3 — Reconstructed vintages, where BLS publishes the diff

Two programs let you genuinely recover a pre-`T₀` vintage (review §2.5.3a):

- **QCEW**: a downloadable CSV of national and state revision data back to **2017 Q1**, updated
  after each news release.
- **SAE**: `bmrk{year}-revisions.zip`, with columns `AS_PREVIOUSLY_PUBLISHED_BMRK{yyyy-1}` and
  `AS_REVISED_BMRK{yyyy}` — BLS handing you a complete two-vintage diff.

These two artifacts have **different shapes and must be handled differently.** Treating them alike
produces a primary-key collision or a fabricated date.

**B3a — QCEW revisions CSV: genuine vintage recovery.** The CSV carries per-quarter revision records,
each attributable to the release that produced it. Each row is committed with
`provenance = reconstructed` and the `knowledge_time` of *that* release, recovered from the QCEW
release calendar. This genuinely recovers pre-`T₀` vintages back to 2017 Q1.

**B3b — SAE `bmrk{Y−1}-revisions.zip`: validation only. Not committed to `observation`.** The file
is a *two-column diff* — `AS_PREVIOUSLY_PUBLISHED_BMRK{Y−2}` and `AS_REVISED_BMRK{Y−1}` — with **one
release date attached to both columns**. Committing both under that single `knowledge_time` violates
the primary key; committing only one silently discards half the artifact; and assigning
`AS_PREVIOUSLY_PUBLISHED` a correct per-reference-month date would require inverting the SAE N=2
delta rule across a 21-month span to find when each month last received a pre-benchmark print — an
inference too speculative to write into a value store.

So B3b is used **only** as the invariant-26 cross-check, which is what review §2.5.3a actually
recommends ("use it to *validate* the store's own captured pre-benchmark vintage"). No fabricated
date, no PK collision, and the artifact's real value is preserved.

That value is considerable: after `T₀`, at each benchmark, reconcile the store's own captured
pre-benchmark vintage against `AS_PREVIOUSLY_PUBLISHED`. A mismatch means the store missed or
corrupted a vintage. It is the cheapest end-to-end as-of correctness check in the design, and it
should run at every benchmark forever.

⚠ The job must **ASSERT that the fetched artifact is non-empty and carries the expected
`AS_REVISED_BMRK*` column.** Given the `Y` vs `Y−1` naming hazard (§5.3), a wrong filename yields a
404 or an empty frame — which, without this assert, reports "no findings" and looks exactly like a
clean reconciliation.

### 11.5 B4 — External cross-checks (optional, validation only)

ALFRED (St. Louis Fed) and the Philadelphia Fed Real-Time Data Set carry independent vintage history
for headline CES and CPS series, with release dates back to 1966. Ingest with
`provenance = external`, never into `strict` queries, and use only to sanity-check the seed and
knowability layers. Formalized as **invariant 26b** (R11): at backfill completion and at each
benchmark job, divergence between the store's `seed`/`reconstructed` layers and the external
record — a differing vintage value, or a first-publication date off by more than tolerance —
raises a FLAG finding with **both-may-be-wrong semantics**: ALFRED itself reconstructs dates where
sources kept none, so a divergence is a lead to investigate, never an auto-correction. Both have documented limitations (ALFRED omits unchanged republications;
Philadelphia's month-end cadence misses intramonth corrections) that make them cross-checks rather
than sources.

---

## 12. Errata and notices automation

Two distinct HTML-derived channels, with different shapes and different jobs. **Errata** (§12.1–12.4)
are a single structured table covering all programs, and they predict *file mutations*. **Notices**
(§12.5–12.8) are per-program prose pages, and they announce *everything else* — reschedules,
methodology changes, reweights, suspensions, and the corrections that no reference-period rule can
infer.

### Errata

Errata are the automatable half of the user's split. The review's §2.5.3b finding makes this
tractable: `bls.gov/errata/` is not prose to monitor — it is a structured, sortable table rendered
from static HTML (client-side DataTables, no XHR, so a plain HTML parse gets every row) with columns
**Date Added · Program · Product Type · Title · Description · Correction Status**.

### 12.1 Ingest

```
errata_row
  row_key            str    # sha256(date_added | program_raw | title) — the table has no id
  row_hash           str    # sha256 of all fields — detects in-place edits
  date_added         date
  program_raw        str
  program_key        str, nullable      # mapped via registry; null → triage
  product_type       str                # "Database" | "News release" | ...
  title, description str
  correction_status  str
  correction_due_date date, nullable    # parsed from "Corrections to be made on MM/DD/YYYY"
  first_seen_at, last_seen_at
  state              enum   # new | armed | matched | closed | unmappable
```

### 12.2 Automation rules

1. `product_type` contains **`Database`** and `program_key` is in scope → this **predicts a
   flat-file mutation**. Mint an `errata` slot for that program.
2. `correction_status` is **forward-dated** → set the slot's `scheduled_at` to `correction_due_date`.
   The watcher is armed *before* the mutation happens rather than detecting drift afterward. This is
   the single highest-leverage automation in the design.
3. `product_type` is **`News release`** only → record; create no data slot.
4. `row_hash` changed → the row was edited; re-evaluate and re-arm.
5. `program_key` unmappable → triage (registry gap), do not drop.

### 12.3 Matching mutations to errata

When row 7 of the truth table fires (artifact changed, nothing expected), search `errata_row` for a
`Database` row for that program within ±14 days. On match: attach, set `release_kind = correction`,
close the errata row. On no match: **alert `unexplained_mutation`** — this is the highest-severity
routine alert in the system, because it means a file changed for a reason BLS has not published.

### 12.4 Rate monitoring

The review quantifies the expected rate: ~**12 in-scope errata per year, ~1 in 3 touching the
database**, concentrated in SAE (21 of 73 database corrections over 6.5 years). Invariant 25 makes
this a monitor rather than a curiosity:

> A trailing-90-day errata rate outside `[0.3×, 3×]` the historical rate is a signal that **the
> monitor is broken**, not that BLS's behavior changed.

A silent errata scraper that returns zero rows looks exactly like a quiet quarter. This monitor is
what distinguishes them.

### Notices

### 12.5 The notices channel

BLS publishes dated notices pages announcing things the errata table does not carry — reschedules,
methodology changes, reweights, suspensions, and corrections. The canonical shape is a dated path
under a subject-area directory, e.g.
`https://www.bls.gov/cex/notices/2025/ce-2024-reschedule.htm` — a **reschedule announcement
published before the affected release date**.

⚠ **Notices are organized by *subject area*, not by program, and the two are not 1:1.** This is a
registry-modelling fact, not a detail:

- **One subject area can feed several in-scope programs.** `ncs` carries notices for **both ECI and
  ECEC** — two programs the review is emphatic must not be conflated (§12.7: an ECI reweight
  announcement must **not** be applied to ECEC unless an ECEC notice or file change confirms it).
  Routing an NCS notice to the wrong one directly violates that rule.
- **"Out of scope" is not the same as "irrelevant."** The example above is `cex` — Consumer
  Expenditure Survey — which is not one of the ten stored programs. But CEX supplies the expenditure
  weights for **CPI**, and CPI deflates the **constant-dollar ECEC** and real-ECI series (review
  §12.2: "Each year the constant dollar (real) estimates are updated to reflect the most recent
  reference period"). A subject area two hops upstream of a stored program still carries notices
  that matter. The watch list is therefore the **inbound dependency closure** of the in-scope
  programs, not the programs themselves — see §12.10.

So the **crawl unit is the subject area** and the **routing target is the program**:

```python
@dataclass(frozen=True)
class SubjectArea:
    key: str                      # "cew", "ces", "sae", "cps", "jlt", "bdm", "oes", "ncs", "emp"
    notices_index_url: str
    programs: tuple[str, ...]     # fan-out; e.g. "ncs" -> ("eci", "ecec")
    resolve: Callable[[Notice], tuple[str, ...]]   # notice -> program(s), when the area is shared
```

`ProgramSpec.notices_subject_area` points back, so the registry has one source of truth for the
mapping. This is the same fan-out shape as §4.2's calendar entries (one "Employment Situation" entry
→ CES-N and CPS-LN slots), and it should reuse the same mechanism rather than growing a parallel one.

**Ambiguous routing never drops a notice.** If `resolve()` cannot confidently assign an NCS notice
to ECI or ECEC from its title and slug, the notice is attached to **both** with
`confidence = inferred`, and any action more consequential than "surface it in the report" requires
confirmation. Dropping is the only unacceptable outcome.

⚠ **The index URLs are not uniformly patterned and must be enumerated by hand at M1.** Do not assume
`bls.gov/<area>/notices/<year>/` generalizes — populating this table is a research task, not an
inference.

```
notice
  notice_id          str    # the notice URL — stable and unique
  program            str
  url, title
  published_date     date, nullable      # from the page or the URL path
  first_seen_at      datetime(UTC)
  content_sha256     str                 # notices are edited in place; hash to detect it
  blob_key           str                 # the page is archived like any other artifact (P1)
  classification     enum                # see 12.6
  extracted          json                # parsed dates, affected periods, series counts
  confidence         enum                # exact | inferred | uncertain
  state              enum                # new | applied | proposed | matched | ignored
```

Discovery: poll each `notices_index_url` daily, diff the link set, fetch and archive each new
notice. Re-hash known notices — a notice edited after publication is itself an event.

### 12.6 Classification and routing

| Class | Trigger | Automated action | Human required? |
|---|---|---|---|
| `reschedule` | A new date is stated for a scheduled release | Update the slot's `scheduled_at`, link the notice, mint a `rescheduled` slot | **No** — but it appears in the daily report |
| `delay` | A release is postponed without a firm new date | Extend the slot's grace window, attach the notice, suppress the lateness alert | **No** for the suppression; **yes** to close the slot |
| `correction` | An announced correction to published data | Arm the checksum watcher for the named date; classify the resulting mutation | No |
| `reweight` / `rebase` | ECI fixed-weight update, base-period change | Write a **methodology boundary marker** (§12.7) | Review only |
| `precision_change` | Display precision changes | Write a precision marker consumed by the differ (§8.3) | Review only |
| `population_control` | CPS annual population-control adjustment | Write a **`population_control_break` marker** carrying BLS's own published adjustment magnitudes (§12.7) | Review only |
| `discontinuation` / `suspension` | Series dropped, or a state's data suspended | Write an **expected-loss marker** (§12.8) | Review only |
| `resumption` | Suspended data returns | Clear the expected-loss marker | Review only |
| `methodology` | Sample redesign, NAICS/SOC transition, definitional change | Methodology boundary marker | Review only |
| `unknown` | Classifier has no confident match | — | **Yes** — triage |

**The reschedule case is the one that changes operator experience most**, and it is worth being
precise about why it does not contradict the user's judgment that delays need human intervention.
Applying a reschedule writes **no data** — it moves a date on a slot. It is fully reversible, it
cannot corrupt the store, and getting it wrong costs one spurious poll window. *Closing* a slot,
*cancelling* it, or deciding a release will never arrive remains human-only (§13.2). Automate the
cheap reversible half; escalate the judgment.

Classification is rule-based (URL slug patterns, title keywords, structured date extraction) with an
explicit `unknown` bucket. It is **never** allowed to guess: a notice that does not match a rule
confidently goes to triage. A misclassified `discontinuation` would suppress a real data-loss alert.

### 12.7 Methodology boundary markers

Several review facts need a place to live that is neither a value nor a schedule. Notices are where
they come from, and a marker table is where they land:

```
methodology_marker
  program, effective_ref_period, marker_kind, notice_id, detail json
```

Consumers:

- **The differ** reads `precision_change` markers to round both sides before comparing (§8.3 —
  this is what ECI's June 2025 move to three decimals requires, and it is *announced in a notice*,
  so hardcoding it is both fragile and unnecessary).
- **The classifier** reads `reweight` / `rebase` markers to resolve the ECI events that no `t_max`
  predicate can reach.
- **Invariant 20** (an ECI base-period label change must accompany any NSA discontinuity) checks the
  marker against the observed catalog change.
- **Query consumers** get an effective-dated record of every comparability break — the NAICS 2022
  transition, the 2020 SOC introduction, the December 2026 workers'-compensation removal — which is
  what stops someone downstream from differencing across a definitional boundary.
- **The CPS population-control break** is the most important single consumer, and it is what makes
  the design's refusal to back-fill (§1.3 non-goal 3) *usable* rather than merely correct. Review
  §17 gap 4's smallest safe inference has two halves: keep the official history byte-identical
  **and** "record the break as metadata using BLS's own published adjustment magnitudes." The second
  half lives here. `detail` carries the figures BLS publishes for each adjustment — for January 2025:
  civilian noninstitutional population +2.9 million (+1.1%), civilian labor force +2.1 million,
  employment +2.0 million, unemployment +105,000, not-in-labor-force +765,000. A consumer that needs
  a continuous series can then apply BLS's *own published* offsets explicitly and visibly, rather
  than the store silently inventing them.

### 12.8 Expected-loss markers

Review §5.8: the 2026 SAE benchmark **discontinued roughly 900 series**. Review §8.8: BED data for
**Colorado** were suspended and later resumed — "a missing state observation may be a
publication-status change, not a numeric zero."

Without a marker, the catalog diff (§8.4) reports 900 discontinuations as a mass data-loss alert,
and the operator either investigates a non-event or, worse, learns to dismiss the alert. An
`expected_loss` marker derived from the notice scopes the expectation, so the alert fires on the
*unexpected* residual only.

This is the same design instinct as the errata rate budget (§12.4): the goal is not to detect
everything, it is to make the alerts that fire mean something.

### 12.9 Degradation

Notices are HTML and therefore subject to §7.2's 403 problem. If the notices channel is unavailable:

- Reschedules are not detected proactively → slots go `late` → human triage. **This is exactly the
  pre-notices behavior**, so the failure mode is a return to baseline, not a new defect.
- CES corrections, ECI reweights, and delayed CPS population controls degrade to
  `unexplained_mutation` and reach a human that way.
- Methodology and expected-loss markers stop being written; the catalog diff falls back to unscoped
  alerting.

⚠ **Precision is the one exception, and it breaks the "noisier, not wrong" claim.** An *undeclared*
precision change — ECI's June 2025 move to three decimals, had its notice been missed — makes the
differ read every value in the file as revised, and P7 makes those fabricated `revised` rows
**permanent**. Noise in an append-only store is not recoverable.

So the precision path gets a defense that does not depend on the notices channel: an **ASSERT-severity
mass-change circuit breaker** at commit. A release classified `routine` that revises more than a
per-program threshold of that program's live series **aborts the commit** and opens a triage item.
P2 makes this nearly free — the bytes are already archived, so a false positive costs one human look
while a false negative is unrecoverable. The threshold now has an evidence-based
anchor (R15): BLS's own QCEW revision statistics — establishment counts rarely move more than ±1%,
employment and wage levels more than ±0.1%, initial to final (bls.gov/cew/revisions) — so QCEW's
breaker trips when a `routine` release moves national or state aggregates beyond that published
envelope. The same method sets the other programs' initial values — anchor to published revision
statistics where the program has them, start conservative where it does not — and the first year
of observed footprints then tunes them (§20 issue 12).

With that breaker in place: the rest is noisier, none of it is wrong, and no data is lost. That is
the P9 contract.

### 12.10 Watch-list scope: the inbound dependency closure

The naive scoping rule — "watch the subject areas of the ten stored programs" — is wrong, and CEX is
the counterexample that shows why. The correct rule:

> **Watch the transitive closure of everything the in-scope programs depend on. Store only the
> in-scope programs.**

Watching and storing are different decisions, and conflating them is what makes an upstream
disruption arrive as a surprise. Three tiers, with sharply different automated authority:

| Tier | Contents | Automated action |
|---|---|---|
| **0 — stored** | The ten in-scope programs | Full: slots, captures, corrections, markers, commits |
| **1 — direct upstream** | QCEW → CES/SAE/BED/OEWS · CES → JOLTS · NCS → ECI+ECEC · OEWS/QCEW → ECI weights · **CPI → ECEC/ECI constant-dollar** · **Census population estimates → CPS** | Annotate the downstream program, arm its watcher, raise a `upstream_event` finding. **Never writes a value.** |
| **2 — transitive upstream** | **CEX → CPI** · BEA input-output → EP · RRB / Census CBP / ASPEP → CES benchmark non-covered employment | Advisory only: logged and surfaced in the daily report. No automated action. |

Tier 1 and 2 authority is bounded by invariant 10 (§14.3): an upstream event may **schedule a
recrawl or annotate**; it may never **rewrite** a downstream vintage. The tiering does not create a
cascade write path — it creates an early-warning path.

**Not every upstream source is BLS**, and this is the part a bls.gov-only watch list misses
entirely. The review's own §2.B2 case is exactly this failure: the 2025–26 appropriations lapse
delayed **Census** population-control production, which delayed the **CPS** population-control
introduction from January to March 2026 and forced a revision of already-published January
estimates. The leading indicator was on a Census surface, not a BLS one, and it was available weeks
before the BLS slot went late.

So `ProgramSpec` carries the inbound edge explicitly, mirroring the outbound `cascades_to`:

```python
    depends_on: tuple[Dependency, ...]   # (source_key, tier, surface_url, relationship)
```

with `source_key` free to name a non-BLS producer (`census-popest`, `bea-io`, `rrb`). Tier-1 and
tier-2 sources get the same daily index-diff crawl as BLS notices; only their routing authority
differs.

⚠ **This is a scope-widening decision and should be taken deliberately.** It adds crawl surfaces
that never produce a stored row. The justification is narrow and specific: it converts a class of
delay from *reactive triage after a missed release* into *advance warning*, which is precisely the
half of the problem the user identified as needing human intervention. Build it at M5, after the
in-scope notices channel works — and if it proves noisy, tier 2 is the part to cut.

---

## 13. Disruption: delays, shutdowns, human triage

The user's split is correct and the design encodes it directly: **errata automate, delays do not.**
A delay is an open-ended fact about the world that the system cannot resolve — only observe, bound,
and escalate.

### 13.1 What the system does automatically

- **Detect an announced reschedule *before* the release date passes**, from the program notices
  channel (§12.6), and move the slot. This is the cheapest possible resolution of a delay: it costs
  no human attention and the delay never becomes an alert.
- **Surface upstream early warnings** from tier-1 and tier-2 dependency surfaces (§12.10), including
  non-BLS ones. A Census population-estimate production delay is visible before the CPS slot it will
  delay goes late.
- Detect lateness (slot past `scheduled_at` + grace with no artifact change).
- Keep polling. Lateness never stops the watcher.
- Gather context and attach it to the slot: the calendar entry, feed state, the last N `HEAD`
  observations with their `Last-Modified` values, any matched errata rows, any BLS notice text
  retrievable, and the status of sibling programs (a lapse hits many programs at once, and that
  correlation is the strongest available signal for "shutdown" vs. "our poller is broken").
- Group correlated lateness into a single `incident` rather than N independent alerts.
- Continue ingesting every other program. A delayed slot never blocks an unrelated one.

### 13.2 Shutdowns

```
incident
  incident_id, kind          # shutdown | bls_delay | infrastructure | unknown
  opened_at, closed_at
  reason, source_url
  affected_programs   list[str]
  opened_by, notes
```

`bls-stats ops incident open --kind shutdown --programs all --reason "appropriations lapse"`
attaches every late slot in the window to the incident, moves them to `delayed`, and collapses
alerting to one incident-level notification.

**Three rules that are easy to get wrong and expensive to get wrong:**

1. **Polling does not stop.** During the 2025 appropriations lapse, BLS updated the ECEC *database*
   through September 2025 while explicitly forgoing the news release (review §12.5). A
   news-release-driven detector would have missed a real data update. Any artifact change during an
   incident is captured, committed, and flagged `unannounced` — and it is high-value signal, not
   noise. The 43-day lapse of 2025-10-01 → 11-12 then showed the stakes in the other direction
   (R17): the October CPI release was cancelled outright and the October household-survey
   unemployment rate is permanently uncomputable — a shutdown does not merely delay data; it can
   destroy it.
2. **`t_max` on resumption is read, never assumed.** BLS may skip a reference period, combine two,
   or publish out of order. This is the whole reason §0's backward-looking design exists.
3. **Nothing is auto-cancelled.** A slot that will never be filled is closed by a human, with a
   recorded reason. The system's default posture toward an unexplained absence is to keep asking.

### 13.3 Triage interface

```
bls-stats slots ls --state late --program sae
bls-stats slots show <slot_id>            # prints the full context bundle
bls-stats slots triage <slot_id> --delay --reason shutdown --recheck-at 2026-11-01
bls-stats slots triage <slot_id> --reschedule --to 2026-11-20T08:30 --tz America/New_York
bls-stats slots triage <slot_id> --cancel --reason "program suspended per BLS notice <url>"
bls-stats slots triage <slot_id> --force-capture
```

Every action appends to `triage_action` (who, when, what, why, evidence URL). A daily triage report
renders open items to `ops/report/<date>/triage.md` and notifies. The report is the operator's whole
interface on a normal day — if it is empty, nothing needs a human.

### 13.4 The CPS delayed-population-control case

Worth calling out because it defeats the obvious rule. In March 2026, population controls were
introduced with the **February** estimates and **all January 2026 estimates were revised** — a prior
published reference month changed outside any documented annual event (review §2.B2). An invariant
asserting "a CPS January commit never alters a prior month" would have hard-failed on real BLS
behavior.

Hence invariant 14 is `[FLAG]`, not `[ASSERT]`, and the population-control detector requires
**three** signals, not one: reference month `M01` is *necessary but not sufficient*; an off-cycle
introduction accompanied by a one-month historical rewrite plus a CPS population-control notice is
the delayed variant.

---

## 14. Validation engine

### 14.1 Design

Invariants are pure functions over a commit context, registered with metadata:

```python
@rule(id="INV-02", severity=ASSERT, scope=COMMIT, source="review §16.2")
def never_overwrite_vintage(ctx: CommitContext) -> Iterable[Finding]: ...
```

- **`[ASSERT]`** — aborts the commit. The bytes are already archived (P2), a `finding` is written,
  and a triage item opens. Reserved for properties that *cannot* legitimately be violated.
- **`[FLAG]`** — writes a `finding`, commit proceeds.

Findings are **data, not exceptions**:

```
finding
  finding_id, rule_id, severity, program, slot_id, release_event_id
  subject json          # series_id / ref_date / range
  detail json
  created_at
  state enum            # open | acknowledged | resolved | expected
```

A `finding_policy` table lets an operator mark a recurring flag as `expected` for a program and
window — scoped suppression, so that the alternative to alert fatigue is not disabling the rule
wholesale. The review is explicit that an invariant firing false positives gets disabled in CI,
which is worse than never writing it.

### 14.2 Invariant map

All 27 invariants from review §16, plus N1–N8 added by this document, with their enforcement point:

| # | Invariant | Sev | Enforced at |
|---|---|---|---|
| 1 | Temporal order (`ref_period_end ≤ first_observed_at`); EP exempt | ASSERT | commit |
| 2 | Never overwrite a vintage; new hash → new `file_vintage` | ASSERT | capture |
| 3 | Declared and observed times stay distinct columns | ASSERT | schema |
| 4 | PK `(program, unit_key, measure_code, ref_date, period_code, knowledge_time)` | ASSERT | schema + commit |
| 5 | Monotonic knowledge window | ASSERT | query layer + property test |
| 6 | Metadata is vintage data | ASSERT | commit (catalog diff required) |
| 7 | Checksum every ingest | ASSERT | capture |
| 8 | Schedule/observation divergence beyond tolerance | FLAG | classify |
| 9 | Footnote flip is a revision event | FLAG | differ |
| 10 | No inferred cascade substitution | ASSERT | commit (write path has no cross-program edge) |
| 11a | Stage labels present are a subset of the program's declared stage vocabulary, scoped per program (`{ces-n: 3, sae: 2, jolts: 2}`; JOLTS-STATE unscoped) | ASSERT | classify |
| 11a′ | **Cardinality** — exactly one new period and exactly *N−1* priors | **FLAG** | classify — §13.2 rule 2 declares skipped, combined, and out-of-order periods legitimate on shutdown resumption, so an ASSERT here hard-fails on expected BLS behavior |
| 11b | Value-change footprint outside the stage window | FLAG | differ (concurrent SA moves it legitimately) |
| 12 | Benchmark reference-month rule (`M01`); **not** the publication month | ASSERT | classify |
| 13 | Reach-by-seasonality as **lower bounds** | FLAG | differ |
| 14 | CPS January immutability | FLAG | differ (§13.4) |
| 15 | Suppression preserves history | ASSERT | differ |
| 16 | QCEW year-to-date carry (Q1≤5, Q2≤4, Q3≤3, Q4≤2 prints) | ASSERT | classify |
| 17 | QCEW dual-date era before 2025-06-04 | ASSERT | slot generation |
| 18 | Immutable-vintage programs (OEWS, EP): changed hash under an unchanged key | FLAG | classify |
| 19 | ECEC nominal stability | FLAG | differ |
| 20 | ECI rebase marker accompanies any NSA discontinuity | FLAG | differ |
| 21 | EP year-pair integrity | ASSERT | parse |
| 22 | Compare parsed numerics, not raw strings | ASSERT | differ (§8.3) |
| 23 | Key vintages to the data artifact, not the news release | ASSERT | classify (P3) |
| 24 | Errata table ingested as a structured feed | FLAG | scheduled job |
| 24b | Subject-area notices crawled, classified, and routed; `unknown` never auto-applied | FLAG | scheduled job (§12.6) |
| 24c | An NCS notice is never applied to ECEC on ECI evidence alone (review §12.7) | ASSERT | notice routing |
| 25 | Errata rate budget (~12/yr, ~⅓ database) | FLAG | monitor |
| 26 | Cross-validate against BLS's own revision diffs | FLAG | benchmark job |
| **26b** | **Cross-validate `seed`/`reconstructed` layers against external vintage databases (ALFRED / Phil-Fed); divergence is a lead — both may be wrong (§11.5, R11)** | FLAG | backfill + benchmark job |
| 27 | Scheduled times from the calendar, never a constant | ASSERT | registry lint |
| **N1** | **Seed/reconstructed rows never take `knowledge_time` from `knowability`** | **ASSERT** | **commit (§11.2)** |
| **N2** | `knowledge_time ≤ first_observed_at + poll_tolerance`, except the flagged `early_artifact` branch | ASSERT | classify (§8.2) |
| **N3** | No committed row derives `knowledge_time` from a slot with `predicted_from = cadence` or an errata due date | ASSERT | classify (§8.2) |
| **N4** | `deleted` rows only inside `artifact.authoritative_scope`; absent a declared scope, zero deleted rows + FLAG | ASSERT | differ (§8.3) |
| **N5** | Mass-change circuit breaker: a `routine` release revising more than the per-program threshold aborts | ASSERT | commit (§12.9) |
| **N6** | For every `revised` row, `prev_value` equals the preceding row's `value` for that key | ASSERT | post-commit window check (§8.3) |
| **N7** | **No store write occurs without the writer lease held and unexpired** (§17.3). With no compare-and-swap in the endpoint, an unguarded write is silently corrupting rather than merely racy, so this is checked in the write helper itself rather than trusted to deployment discipline | ASSERT | every `store/` and `ledger/` write |
| **N9** | **No `observation` row is readable without a committed `release_event`** (§9.1). The as-of query's semi-join is the enforcement; a property test asserts an interrupted commit's rows are invisible | ASSERT | query layer + property test |
| **N8** | Ledger materialization is idempotent: re-running it over the same `fetch_log` range produces zero new rows (§17.1) | ASSERT | `tick --write`, post-materialization |
| **N10** | Every committed `release_event` acquires a timestamp proof within tolerance; a backlog beyond it is a finding (§9.1, R5) | FLAG | daily monitor |
| **N11** | Archive fixity: a sampled re-hash of `raw/` blobs matches their content-addressed keys (§17.4, R6) | FLAG | fixity job |
| **N12** | An independent second copy of `raw/` + `log/fetch/` exists; any single-copy period longer than one release cycle is a standing finding (§17.4, R7) | FLAG | doctor + daily monitor |

**Operator override.** Because an `[ASSERT]` blocks a commit while the bytes are already archived
(P2), there must be a documented path from a blocked commit to a stored observation — otherwise a
single over-strict rule strands a vintage indefinitely. `bls-stats commit <file_vintage_id>
--override <rule_id> --reason "…"` records the override in `triage_action` and stamps the resulting
`release_event`. `finding_policy` (§14.1) scopes **FLAGs only**; an ASSERT is overridden per-commit
by a named human, never suppressed by policy.

### 14.3 Cascades are validation, never a write path

Invariant 10 is the review's sharpest correction (§2.D4). An upstream event may **schedule a
recrawl**; it may never **rewrite** a downstream vintage. The window between the CES February
benchmark and the JOLTS January-data release in early March is a **legitimate public vintage** in
which CES and JOLTS sit on different CES vintages. Collapsing it fabricates a state that was never
public.

Concretely: when a CES benchmark commits, the system writes a `cascade_expectation(JOLTS, next M01
release, expect ≥60-month footprint in both seasonalities)` used purely as a validation hint at the
JOLTS release. It touches no JOLTS row. The registry's `cascades_to` field feeds this and nothing
else — enforced by the loader having no cross-program write capability at all.

---

## 15. CLI surface

Typer, with command groups mirroring the planes.

```
# expectation
bls-stats calendar sync                       # .ics + schedule pages → slots
bls-stats feed poll                           # Atom feeds → corroboration
bls-stats errata sync                         # errata table → armed correction slots
bls-stats notices sync [--area ncs]           # subject-area notices → reschedules, markers
bls-stats notices ls [--state new] [--class unknown]
bls-stats notices apply <notice_id> --program eci   # confirm an ambiguous or low-confidence route

# capture
bls-stats watch [--program P] [--due-only]    # HEAD sweep; the fast loop
bls-stats capture <slot_id|--url U>           # force a GET + archive

# interpretation
bls-stats parse [--pending] [--file-vintage V]
bls-stats commit <file_vintage_id>            # parse → validate → append
bls-stats commit <file_vintage_id> --override <rule_id> --reason "..."   # named human unblocks an ASSERT

# composite (what cron runs) — --fast/--sweep take no lease; --write/--daily take the writer lease
bls-stats tick --fast | --sweep | --write | --daily

# backfill
bls-stats backfill seed --program P
bls-stats backfill knowability --program P [--from 2005]
bls-stats backfill reconstruct --program qcew|sae

# operations
bls-stats slots ls|show|triage ...
bls-stats ops incident open|close|ls
bls-stats ops report [--date D]
bls-stats findings ls [--severity assert] [--state open]

# verification
bls-stats verify replay [--sample 50] [--program P]
bls-stats verify benchmark-diff --program sae --benchmark 2025
bls-stats verify coverage
bls-stats verify attest [--since D]           # re-verify timestamp proofs against archived manifests (R5)
bls-stats verify fixity [--sample N|--full]   # re-hash raw/ blobs against their keys (R6); --full is resumable

# query
bls-stats query --series CEU0000000001 --as-of 2026-03-01 [--strict/--no-strict]
bls-stats query revisions --series ... --ref 2025-03
bls-stats doctor
```

**Exit codes**, because cron is a consumer:

| Code | Meaning |
|---|---|
| 0 | Success, nothing needing attention |
| 1 | Unexpected error |
| 10 | `[ASSERT]` failure — a commit was blocked |
| 20 | Late slots or open triage items exist |
| 30 | Degraded — an HTML surface is unreachable, ingest continuing |
| 40 | Writer lease unavailable — timed out waiting; nothing was written (§17.3) |

Every command is **idempotent**. Re-running `tick` after a crash is always safe; this is what makes
ephemeral containers viable.

**Every command belongs to exactly one plane** (§17.3), and the grouping is not cosmetic — it
determines whether the command can run while a QCEW parse is in flight:

| Plane | Commands | Behavior under contention |
|---|---|---|
| **Lock-free** (no store write) | `watch`, `capture`, `query`, `verify *`, `slots ls|show`, `findings ls`, `notices ls`, `ops report`, `doctor`, `tick --fast|--sweep` | Always runs. Never waits. |
| **Writer** (takes the lease) | `calendar sync`, `feed poll`, `errata sync`, `notices sync`, `notices apply`, `parse`, `commit`, `slots triage`, `ops incident open|close`, `backfill *`, `tick --write|--daily` | Blocks with a timeout, naming the holder; exit code 40 on timeout. `--no-wait` exits 0 immediately instead. |

Note `capture` is lock-free while `parse` and `commit` are not — the same split as P2's durability
boundary, so a forced capture during triage always succeeds even mid-parse.

---

## 16. Package layout and dependencies

```
src/bls_stats/
  cli/          typer app; one module per command group
  config/       pydantic-settings; TOML program overlays
  registry/     ProgramSpec objects, cadence, delta rules, artifact specs
  transport/    httpx clients, profiles, retry, rate limiting, HtmlFetcher backends
  objstore/     object-store adapter: storage options, blob PUT/GET, fetch-log writer (§16.1)
  capture/      HEAD watcher, streaming GET, blob archive, fetch log
  calendar/     .ics parse, schedule-page parse, Atom feed parse, entry→slot matching
  errata/       errata table parse, slot arming, mutation matching
  notices/      subject-area crawl, classification, program routing, markers
  parse/        labstat_tsv, qcew_csv_zip, oews_xlsx, ep_html, series_catalog
  diff/         differ, current_state maintenance, precision handling
  store/        Parquet layout, declared schemas, manifest commit, as-of query, replay
  ledger/       slot lifecycle, release events, incidents, coverage monitor
  validate/     rule registry, findings, policies
  ops/          triage, reports, notifications, single-writer lease
  backfill/     seed, knowability, reconstruction, external
```

| Dependency | Role |
|---|---|
| `httpx[http2]` | All HTTP. HTTP/2 matters for the WAF-shaped requests. |
| `polars` | All tabular data **and the store itself** — `scan_parquet` / `sink_parquet` for streaming, Hive partitioning for pruning (§9.1). **Pinned; bumps are deliberate** (R18): streaming-engine memory regressions between releases are documented, so a version bump must pass §18.1's memory-envelope gate before adoption. |
| `typer` | CLI. |
| `pydantic` + `pydantic-settings` | Config and schema validation at boundaries. |
| `tenacity` | Retry with jitter. |
| `icalendar` | `.ics` schedule parsing. |
| `lxml` or `selectolax` | HTML (errata table, news-release indexes, EP matrix) and Atom. |
| `fastexcel` | OEWS `.xlsx` via `pl.read_excel`. |
| `structlog` | Structured logs correlated by `request_id` / `slot_id`. |
| `boto3` + `s3fs` | S3 API for the blob archive, the manifest PUT, and streaming transfers, via `objstore` (§16.1). |

Dev: `pytest`, `respx` (httpx mocking), `hypothesis`, `ruff`, `mypy --strict`.

Packaging: `uv` + `hatchling`, `uv.lock` committed, `uv sync --frozen` in the image.

### 16.1 The object-store adapter

**No external storage library.** The adapter is purpose-built in-repo, because the surface this
design needs is narrow and every general-purpose S3 helper carries paths that are wrong here —
whole-object buffering reads (against §1.4's streaming mandate), unpaginated listings (silently
truncating an archive of millions of objects), and log-and-return-falsy error handling (a write that
fails without raising loses a vintage, P1's top-ranked unrecoverable failure).

It is the **only** module that talks to the object store. Everything else — capture, store, ledger,
ops — goes through it, so the endpoint's identity and capabilities are known in exactly one place.

| Capability | Contract |
|---|---|
| `storage_options()` | Produces the §9.1 dict, and is the single source of it. Emits `object_store` key names, adds `aws_allow_http` iff the endpoint scheme is `http`, and never falls back to ambient AWS credential resolution — a missing credential raises rather than silently resolving to something else. |
| Streaming `get` / `put` | Chunked in both directions, never `.read()` into memory. Sized for hundred-megabyte flat files and multi-gigabyte archives under §1.4's 8 GB peak-RSS budget. |
| `put_atomic(key, bytes)` | Single-object PUT, used for the §9.1 manifest commit and the §17.4 generation pointer. Returns only on confirmed durability; **raises on any failure**, never returns a falsy sentinel. |
| `append_jsonl(records)` | One uniquely-named object per run under `log/fetch/dt=…/`. Half of P2's durability boundary. |
| `list(prefix)` | **Fully paginated**, continuation-token driven, returning a lazy iterator. A capped or truncating listing is a defect, not a tuning parameter: `verify replay` and the ledger rebuild both enumerate the whole archive, and a silent truncation there reports a clean reconciliation over data it never looked at. |
| `head(key)` / `exists(key)` | Metadata only; distinguishes "absent" from "error", never collapsing the two. |

Two error-handling rules, stated because the temptation runs the other way:

1. **Every write failure raises.** There is no falsy return path on the write side. An unchecked
   `False` in the capture path is indistinguishable from success and loses bytes permanently.
2. **A read may return `None` only for a genuinely absent object.** Absence is a real answer that
   callers act on (a first-run empty table, an unwritten partition). Any other failure raises.

Deliberately **not** provided, because nothing in this design uses them and each is a hazard:
presigned URLs (§1.3 rules out a serving layer), local-directory mirroring (§1.4 has no durable
local disk), and unrestricted delete (§17.4 denies it on the archive; the writer's orphan sweep is
scoped to uncommitted keys under `store/`).

**Test it against a live endpoint, not a mock** — see §18.1. A mocked object store accepts whatever
option keys it is handed, so it cannot catch the failure this module exists to prevent.

---

## 17. Deployment and scheduling

### 17.1 Loops

The loops divide on one line — **does this write the store?** — because §1.4 admits exactly one
store writer. Everything above the line runs freely and concurrently; everything below it is one loop.

| Loop | Cadence | Writes store? | Work |
|---|---|:-:|---|
| `tick --fast` | every 2 min | **no** | HEAD-poll slots in a watch window; on a change, GET → blob → `fetch_log`. No-op when nothing is due. |
| `tick --sweep` | hourly | **no** | Baseline HEAD sweep over every in-scope artifact (§6.2), plus the *fetch* half of `calendar sync`, `feed poll`, `errata sync`, `notices sync` — each retrieved surface archived as a blob with a `fetch_log` record, exactly like a data artifact |
| `tick --write` | every 5 min | **yes** | The single writer. In order: materialize `file_vintage` + slot transitions from `fetch_log`; ingest the fetched calendar / feed / errata / notice blobs into the ledger; parse pending captures; validate; commit; fire or retry manifest attestation (§9.1, R5) |
| `tick --daily` | daily | **yes** | Triage report, coverage/staleness monitor, errata rate monitor, notifications |
| `compact` | daily | **yes** | Generation-swap compaction (§17.4) on hot `observation` partitions and on `finding` |
| `verify` | weekly | no (read-only) | Replay verification on a sample; `benchmark-diff` when a benchmark landed; fixity sample over `raw/` (R6) |

**The three `yes` rows serialize behind the one lease (§17.3)** — `tick --write` skips its run if the
lease is held, and `--daily` / `compact` are scheduled into a quiet window. They may make each other
wait; none of them can make *capture* wait.

**Nothing above the line is a compromise forced by single-writer mode.** The fast loop is now
strictly simpler than in the two-lease design — it takes no lock, touches no transactional table,
and its only outputs are two immutable object PUTs. It is the piece that must never break, because
it is the piece whose failure loses data permanently, and it is now nearly impossible to break.

⚠ **`tick --sweep` moving its metadata *parsing* to `--write` is a real change and costs one thing:**
a `.ics` edit or an errata row is now visible to the slot ledger up to one `--write` cycle late
rather than immediately. Five minutes against the §6.2 ladder's 60-second resolution is inside
tolerance. What it must not do is delay the *fetch* — the sweep still retrieves and archives on its
own cadence, so the bytes are captured on time even when the writer is mid-QCEW-parse.

### 17.2 Containers

Stateless. All state in the object store. Scratch under `$TMPDIR`, deleted after parse. Peak RSS bounded by
streaming: `scan_csv` / `scan_parquet` → `sink_parquet`, never `read_*` on a full-universe file.
Config via environment (`AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`); secrets via the
platform's secret store.

⚠ **The endpoint must be reachable from wherever the containers run.** A loopback endpoint on a
workstation is a development convenience, not a deployment topology — see §20 issue 14.

### 17.3 The single writer

Overlapping cron invocations are a matter of when, not if, and §1.4 permits exactly one store writer
at a time. The obvious reading of that constraint — one global lease over everything — is still
wrong, for the reason the two-lease design gave: it would let a long QCEW parse block the capture
path, and P1 ranks a missed capture as the design's top unrecoverable failure.

The resolution is not a second lease but a **plane split**, and it is what P2's durability boundary
buys:

| Plane | Writes | Concurrency control |
|---|---|---|
| **Capture** | Blobs under `raw/`, JSONL under `log/fetch/` — both immutable objects at unique keys | **None needed.** Content-addressed blobs make a duplicate PUT a no-op; one JSONL object per run means no two processes ever touch the same key. Unlimited concurrent capture processes. |
| **Ledger + store** | Every Parquet table — `file_vintage`, `release_slot`, `release_event`, `observation`, `current_state`, `series_catalog_vintage`, `finding`, … | **One lease, `ops/lease/writer.json`.** Held for the duration of a `tick --write`, `--daily`, or `compact` run. A cron process that cannot take it exits 0 immediately; it never queues. |

**`tick --fast` and `tick --sweep` take no lease and write no store table.** Every ledger effect they
would have had — a new `file_vintage`, a slot moving `watching → captured` — is *derived* from the
`fetch_log` records they wrote, and materialized by the next `tick --write`. The fast loop cannot
wait on a lock because it never asks for one.

The lease is a lease, not a lock: it carries a holder id and an expiry, and a run that dies without
releasing it is reclaimed after the expiry rather than deadlocking the store forever. Expiry must
exceed the longest expected parse (QCEW), and a reclaim is a `finding`, not a silent event.

**Interactive commands contend differently from cron, and must behave differently.** "Exits 0
immediately, never queues" is right for a loop that will run again in five minutes; it is wrong for
an operator at a terminal, for whom a silent success while triaging a late slot is a defect. §15's
write commands — `slots triage`, `ops incident open|close`, `notices apply`, `commit`, `parse`,
`capture`, and `backfill *` — therefore **block on the lease with a stated timeout, print the
current holder and its expiry, and exit non-zero on timeout**. `--no-wait` restores the cron
behavior for scripted use.

⚠ **`backfill seed` is the one command that does not fit either shape.** It downloads and commits
every program's full published history; under one writer it would hold the lease for hours, during
which no ledger materializes and slot state goes stale across the whole system. It splits along the
same plane boundary as the loops: the download half is lock-free (blobs + `fetch_log`, and can run
long), and the commit half proceeds in **bounded chunks that take and release the lease per chunk**,
so `tick --write` interleaves and the ledger never falls hours behind. `backfill knowability` and
`backfill reconstruct` follow the same rule.

⚠ **Single-writer mode removes concurrent-writer races. It does not remove crash-mid-sequence
races, and it gives no cross-table consistency.** §9.1's manifest commit is atomic *per release*,
not across tables, so a `tick --write` killed between the `observation` write and the
`current_state` rewrite leaves precisely the inconsistency §8.3 describes. Cross-table consistency
comes entirely from §8.3's observation-first ordering, §9.1's addressed-write idempotency, and the
roll-forward on startup — **none of which is made redundant by having one writer.**

Where single-writer mode genuinely *helps* is compaction. §17.4's generation swap would be unsound
under concurrent appends — a writer adding a file to generation `N` while the compactor is sealing
it would lose that file. With one writer, appends and compaction are serialized by construction, so
the swap needs no coordination beyond the lease it already holds. The constraint that made a table
format pointless is the same constraint that makes its replacement simple.

### 17.4 Bucket policy and compaction

Stated as **requirements on the endpoint**, not as a vendor's feature names. Which of them a given
endpoint can satisfy — and by what mechanism — is reported by `doctor` (§15) rather than assumed
here, because the development and deployment endpoints differ in both directions (§1.4).

| Requirement | Why | If unavailable |
|---|---|---|
| **Versioning on** | An overwrite of a committed object is recoverable | `doctor` warns; the archive's content-addressing means an overwrite is a no-op anyway |
| **Immutability on the archive bucket** — object-lock retention, or equivalent | P1: a captured vintage is unrecoverable if lost | ⚠ Degrade to the delete-deny policy alone and record the gap. Note that where object lock exists it is commonly settable **only at bucket creation** and needs a *duration*, not just a mode — so it must be configured before the first capture or not at all |
| **Delete denied to the runtime credential on the archive bucket** | There is no operational reason to delete an archived vintage, and every reason to be unable to | Hard requirement. Do not run without it |
| **`raw/` never expires** | Same | Hard requirement |
| **An independent second copy of `raw/` + `log/fetch/`** (R7; adjudicated — §21) | Versioning and delete-deny protect against *logical* loss, not media or provider failure; P1 makes the archive irreplaceable, and 2025 proved a missed window permanent | Run, but `doctor` reports replication state and any single-copy period longer than one release cycle is a standing finding (N12). The mechanism — provider replication, a second endpoint, a pull job — is the deployment's choice; the requirement is that the copy exist and be independent |

**Two buckets** — `s3://<raw-bucket>/raw/` and `s3://<bucket>/{log,ledger,store,ops}/` — because it
makes delete-deny and retention whole-bucket statements rather than prefix conditions, and keeps
compaction's rewrites unambiguously clear of immutable objects. Per-object retention would make a
single shared bucket workable; the separation is chosen because it is cheap and the blast radius of
a policy mistake on `raw/` is P1's unrecoverable failure.

**Fixity is checked, not assumed (R6).** Content-addressing makes integrity verification trivial —
a re-hashed blob must equal its key — and the design now requires that it be *done*: `verify
fixity` (§15) re-hashes a weekly sample and completes a full, resumable pass quarterly; any
mismatch is a FLAG finding (N11), treated as a candidate media failure and cross-checked against
the replica above before any conclusion is drawn.

**No lifecycle tiering is assumed.** The former "`log/fetch/` transitions to infrequent access at 90
days" rule is **dropped, not translated** — storage classes are an AWS-only feature (§1.4). The
fetch log stays hot and is budgeted as such.

#### Compaction: the generation swap

Without a table format there is no `optimize.compact` and no `VACUUM`, so compaction is specified
here. Small-file proliferation is real: one object per release per partition, plus one per tick for
`finding`.

Per partition, compaction is an **atomic pointer swap** built on §1.4's single-object PUT:

```
store/observation/program=<p>/ref_year=<y>/
  _current                  → one small object naming the live generation   ← THE SWAP POINT
  gen=<n>/<release_event_id>.parquet, …
  gen=<n+1>/compacted.parquet
```

1. Read every object in the generation named by `_current`.
2. Write the compacted result under `gen=<n+1>/`.
3. **PUT `_current` naming `n+1`.** One object. Atomic. This is the instant compaction takes effect.
4. Delete `gen=<n>/` — garbage collection, not part of the commit.

A crash before step 3 leaves an unreferenced generation; a crash after leaves a stale one. **Neither
is ever double-counted**, because readers resolve `_current` first and read exactly one generation.
Both failure modes are garbage, cleaned on the next run.

**Reader contract, because the edge cases are where this breaks:**

- **`_current` absent → the partition is empty.** Return no rows. Never an error: this is the state
  of every partition before its first write, and §10.1's `as_of` is required to return empty rather
  than fail for a never-written program or `ref_year`.
- **Never glob `gen=*/` as a fallback.** A reader that enumerates generations instead of resolving
  the pointer double-counts across the compaction window — precisely the failure the pointer exists
  to prevent, and it appears only under load.
- **Resolve `_current` once per query**, not per file, so a swap mid-query cannot split a read
  across two generations.

This is sound only because §17.3 guarantees a single writer: concurrent appends into `gen=<n>` while
it is being sealed would be lost. Appends and compaction serialize behind the same lease, so no
further coordination is needed.

| Table | Compaction |
|---|---|
| `observation` | Generation swap, daily, on hot `ref_year` partitions. Replay (§10.3) selects on `knowledge_time`, a data predicate, so no version history is needed to support it |
| `current_state` | Already a whole-partition rewrite — it *is* a generation swap, with no separate job |
| `release_slot`, `file_vintage` | Same: every mutation is a generation swap (§9.1), so compaction is implicit. Small enough that a whole-partition rewrite per `tick --write` is affordable — measure it if slot counts grow |
| `finding` | **Needs it most**: one write per hourly tick. Drop the `dt=` daily partition — partition by `program`, or leave unpartitioned and rely on Parquet file statistics |

---

## 18. Testing strategy

### 18.1 Layers

| Layer | Approach |
|---|---|
| Parsers | Recorded fixtures — real (trimmed) LABSTAT files, QCEW CSVs, an OEWS sheet, an EP page. No network. |
| Transport | `respx`-mocked httpx: 403 handling, `Last-Modified` semantics, restamp-only, retry/backoff. |
| Delta rules | Table-driven tests over every program × release kind, asserting exact footprints for dated instances from the review. |
| Duality | `hypothesis`: `t ∈ footprint(r) ⟺ r ∈ prints_of(t)` over random programs and periods. |
| As-of | `hypothesis`: for random `D` and each key, the returned row **equals the row with the maximum `knowledge_time ≤ D`**, computed independently of the query under test. The weaker "no row has `knowledge_time > D`" is satisfied by returning nothing, and so cannot detect the §18.2 QCEW regression it is paired with. This is the feature-store point-in-time-join correctness property (R19). |
| Strict semantics | `as_of(T₀ + ε, program, strict=True)` returns exactly the seed release's row count — the check that catches §10.1's sparse-log trap. |
| Differ | Synthetic vintage pairs covering every `change_kind`, including `deleted`, `precision_only`, and suppression flips. |
| Store | Round-trip commit → replay → compare, on a temporary store root. Plus **the torn-commit test**: write a release's `observation` *and* `series_catalog_vintage` objects, omit the `release_event` PUT, assert both are invisible — then PUT the event and assert both appear. Catalog rows are included deliberately: gating them is what stops an aborted commit from being read as a series addition (§9.1). This is the one test that proves the manifest protocol, and it has no equivalent in a table-format design because the format would have provided it. |
| Compaction | Generation swap under simulated crashes: kill before and after the `_current` PUT, assert the reader sees exactly one generation and identical row counts either way. Plus the empty case — **a partition with no `_current` reads as empty, never as an error** (§17.4), which is the state every test fixture starts in. |
| Memory envelope | A QCEW-scale synthetic fixture runs the differ and the as-of query under a hard peak-RSS budget and fails on regression (R18) — the gate that catches a silent fallback from the streaming engine to in-memory, which no unit test sees, and the gate a `polars` version bump must pass (§16). |
| Rebuild | §10.4's chain end to end on a small fixture: drop `ledger/` and `store/` entirely, rebuild from `raw/` + `log/fetch/`, and assert byte-identical `file_vintage_id`s and an identical `as_of` result at several `D`. This is the least-exercised path in the design and the one that runs during a disaster. |
| `objstore` | Local-filesystem backend for logic; a `--real-store` marker for a live round-trip asserting `storage_options()` produces keys the reader/writer actually honour, that `list()` paginates past one page, and that a failed write raises (§16.1). A mocked object store cannot catch a wrong option key — it accepts whatever it is handed. |
| Ledger materialization | `fetch_log` JSONL → `file_vintage` + slot transitions is a pure function of the log; test it by generating a log, materializing twice, and asserting the second run is a no-op. Idempotency here is what makes the §17.3 plane split safe. |
| Integration | `--network` marker, excluded from CI, run on demand against live BLS. |

### 18.2 Golden regressions

Three cases from the review, encoded as permanent tests:

1. **QCEW December 2025.** Files released 2025-12-19 omitted expected Q1 2025 revisions and were
   replaced 2026-01-07. The test asserts two distinct `file_vintage` rows, two `release_event` rows
   (the second `release_kind = correction`), and that `as_of("2025-12-20")` returns the **defective**
   values. This is the canonical worked example for the entire design; if it ever breaks, the store
   is no longer point-in-time correct.
   ⚠ **(open — resolved by verification, not argument):** the 2025-12-19 / 2026-01-07 dates come
   from the domain review and did not verify against primary sources in the external review (R16);
   its cadence objection does not refute a *correction* — corrections are off-cadence by nature —
   but unverified is unverified. Discharge: confirm against `bls.gov/cew/notices` (or its Wayback
   capture) when this fixture is built at M2. The **verified** instance of the same shape — the
   2019-09-09 reload replacing defective 2019-09-04 QCEW files (bls.gov/cew/notices/2019) — is the
   fallback fixture; the test's substance (two `file_vintage` rows, second event `correction`,
   as-of returning the defective values) is date-independent.
2. **CPS March 2026.** Population controls introduced with February estimates; January 2026 revised;
   news release not reissued. The test asserts the revision is captured (P3), invariant 14 flags
   without blocking, and the release is classified `population_control` via the three-signal
   detector rather than the reference-month rule alone.
3. **SAE benchmark cross-check.** Given a `bmrk{year}-revisions.zip` fixture, the store's own
   pre-benchmark vintage matches the `AS_PREVIOUSLY_PUBLISHED` column exactly (invariant 26).

### 18.3 What must not be tested with mocks

The 403 behavior of `www.bls.gov` is a live, changeable property of a WAF. Mock it for logic tests,
but keep a `--network` smoke test that runs on demand and reports which transport profile currently
works. A design decision built on a mocked assumption about a third party's edge network is a
decision built on nothing.

---

## 19. Build order

Sequenced by *what is irreversible if delayed*, which is not the same as what is architecturally
foundational.

| Milestone | Deliverable | Why here |
|---|---|---|
| **M0** | **Capture-only.** `objstore` (§16.1) + transport + blob archive + `fetch_log` + a crude HEAD sweep over every in-scope artifact. **No store writes at all** — no `file_vintage`, no parsing, no ledger, no schema. | **Ship this first, before the rest of the design is finished.** Every day without capture is a day of permanently unobservable vintages. P2's durability boundary makes M0 exactly the lock-free plane of §17.3, so M0 is now *smaller* than in the two-lease design and ships sooner. `file_vintage` is derived and arrives at M1 with the writer. |
| **M1** | Slot ledger, calendar `.ics` sync, feed poll, coverage monitor — **plus the whole store mechanism**: writer lease (§17.3), ledger materialization from `fetch_log`, the `_current`/generation indirection (§9.2), and the manifest commit (§9.1) | Turns the crude sweep into targeted watching; makes absence detectable. M0 shrank to the lock-free plane, so all of this landed here. ⚠ The `_current` indirection must be present on **every partition from its first write** — §9.2 — so it is un-retrofittable and belongs in the first milestone that writes the store at all |
| **M2** | Parse + differ + `current_state` + change log for CES-N, JOLTS, **and QCEW** | Two LABSTAT programs cannot validate the abstractions. QCEW is the one that proves `unit_key`/`measure_code` (§9.3), `authoritative_scope` (§8.3), and the memory envelope. Deferring every non-LABSTAT program to M7 as "mechanical" means discovering the primary key is wrong after the store is full |
| **M3** | Backfill B1 (seed) + B2 (knowability), with invariant N1 enforced | The provenance split must exist before the store fills up |
| **M4** | Validation engine, findings, triage CLI, daily report | Operability; everything after this generates findings that need somewhere to go |
| **M5** | Errata automation + rate monitor; subject-area notices crawl, classification, and reschedule application | Highest-leverage automation; depends on M4's findings plumbing. Notices also unblock the three release kinds that are underivable from `t_max` (§8.1) |
| **M6** | B3 reconstruction + benchmark cross-checks (invariant 26) | The end-to-end correctness proof |
| **M7** | Remaining programs: SAE, CPS-LN, BED, ECI, ECEC, OEWS, JOLTS-STATE, EP | Mechanical once M2's abstractions hold across both a LABSTAT and a non-LABSTAT program. EP is the hard one — non-static artifact set (§5.1) — do it last, deliberately |

M0 is the load-bearing sequencing decision. The instinct is to design the schema first and capture
once it's right. That instinct is wrong here, because the schema is recoverable and the bytes are
not.

---

## 20. Open decisions and risks

Stated honestly rather than resolved by assertion.

| # | Issue | Position | What would settle it |
|---|---|---|---|
| 1 | **`www.bls.gov` 403.** Whether browser-shaped `httpx` suffices, or a headless backend is mandatory. | Design assumes it may be mandatory (§7.2) and degrades cleanly. | A live probe across all three transport profiles, run at M0. Review §19 flags this as an unresolved queue item — and it determines the whole HTML ingest posture. |
| 2 | **`download.bls.gov` fetch behavior.** The 403 was observed on `www`; the flat-file host and API were not tested. | **Partially settled (R12):** the flat-file host is *not* unrestricted — it 403s bare/default user agents, and BLS policy permits real-time blocking of non-compliant robots. Working assumption: accessible with a compliant, contactable User-Agent at polite rates; §7.1 makes that mandatory from the first request. | Same probe, unchanged and still first: confirm the compliant profile passes and lock the ingest channel before anything else is built. |
| 3 | **Plain Parquet vs. a transactional table format (Delta / Iceberg).** | Parquet, per §9.1 — **re-argued on honest grounds (R2):** not forced by the endpoint (single-writer formats exist without compare-and-swap; catalog-based ones would need a second durable system §1.4 rules out) but *chosen* for zero dependencies in the custody path, with the conceded losses answered in §9.1 — pruning by R4's manifest statistics, naive-reader snapshot isolation by the CLI as the supported interface. | Adopt a format if a downstream consumer mandates one, if the endpoint gains conditional PUT *and* a second writer becomes necessary, or if the §17.4 generation swap proves fragile in practice. **Reversible** — the change log is derived (P1), so adoption is a rebuild, not a migration. |
| 4 | **QCEW artifact choice**: LABSTAT `en` prefix vs. the quarterly singlefile CSVs (and the by-size ZIP). | **Direction settled (R13): the quarterly singlefile CSVs**, corroborated as the tractable per-quarter diff artifact; fixes `authoritative_scope` as per-quarter (§8.3), settled before M2 hardens the differ. | **(open — resolved by measurement, not argument):** actual byte sizes and diff cost against the 8 GB envelope, at M0/M2 — the review's size figures are third-party. |
| 5 | **CES national 21-month NSA span** is confirmed verbatim for SAE only; for national CES it is derived, not quoted (review §2.5.4). | **Closed (R14):** the CES FAQ and benchmark article state it explicitly for national CES — the final benchmark revises 21 months, anchored to March of the prior year, published with the January preliminaries. Encoding unchanged: still a `[FLAG]` lower bound per P5. | Settled — cite `bls.gov/web/empsit/cesfaq.htm` and `cesbmart.htm` in the registry entry. |
| 6 | **OEWS publication window** (Mar–May drift) and correction history — the one unresolved item from the review's verification pass. | Treated as calendar-driven with a wide watch window; no fixed date encoded. | Observing two or three cycles post-`T₀`. |
| 7 | **EP `ep` flat file twice-yearly refresh** (wages in April, projections in September) is single-source. | Two distinct detectors on the same prefix, both flagged. | Observing one full year. |
| 8 | **Notification transport.** Deliberately unspecified. | A pluggable sink; the triage report is the real interface. | Operator preference. |
| 9 | **Subject-area notices coverage.** Index URLs are not uniformly patterned; NCS fans out to two in-scope programs; most areas are out of scope. | Registry table enumerated by hand at M1, with ambiguous routes attached to all candidate programs rather than dropped (§12.5). | Manually enumerating the nine in-scope subject areas and confirming each index URL and its historical depth. |
| 10 | **Notice classification precision.** A misclassified `discontinuation` suppresses a real data-loss alert. | Rule-based with an explicit `unknown` bucket routed to triage; classification never guesses. | Backtesting the classifier against the historical notice corpus recovered at M5. |
| 11 | **JOLTS-STATE routine `N` is undocumented.** Review §2.5.4 lists JOLTS state benchmarking detail as unverified. | Routine footprint = `t_max` only, `semantics = lower_bound`, FLAG on any prior-period change; invariant 11a's stage counts scoped per program so JOLTS-STATE is not swept in under JOLTS's `N=2`. | Observing one full benchmark cycle post-`T₀`. |
| 12 | **Mass-change threshold** for the §12.9 circuit breaker. | **Anchored (R15):** QCEW's initial threshold from BLS's published revision envelope (establishments ±1%, employment/wages ±0.1%, initial→final); other programs by the same method where revision statistics exist, conservative defaults where they do not (§12.9). | The first year of observed footprints tunes per-program values — now a calibration, no longer a guess carrying ASSERT severity. |
| 13 | **`content_sha256` normalization boundary.** Whether decode-only is enough, or whether line-ending/BOM normalization is also needed to avoid phantom vintages. | **Closed (R10):** decode-only, aligned with WARC payload-digest practice — normalization beyond transfer-decode would trade forensic fidelity for convenience. Line-ending/BOM handling belongs to the parse layer. `wire_sha256` keeps any future re-normalization derivable from the archive. | Settled against further normalization; a byte-differing, semantically identical payload, if ever observed, is handled at parse, not in the hash. |
| 14 | **Endpoint topology.** §1.4 says compute is ephemeral containers; the development endpoint is a loopback service on a workstation. Those two are incompatible as written — a container cannot reach another host's loopback. | Endpoint is configuration (`AWS_ENDPOINT_URL`), never a constant, so the code is topology-agnostic and the development and deployment stores differ only by that variable. The spec asserts no topology. | Confirming the deployment endpoint's address and reachability from the container network. This also determines whether §17.3's lease is contended in practice or only theoretically. |
| 15 | **Archive durability.** Retention and versioning protect against *logical* deletion; neither protects against media loss, and §17.4's archive is by P1 the one irreplaceable artifact. | **In scope now (R6/R7; adjudicated — §21):** §17.4 requires an independent second copy of `raw/` + `log/fetch/` — `doctor`-reported, with any single-copy period longer than one release cycle a standing finding (N12) — and a periodic fixity audit (N11). Mechanism remains the deployment's choice. | The deployment's replication decision, confirmed by `doctor`, plus the endpoint's own redundancy posture. |

### The three risks that actually matter

1. **A missed capture is permanent.** No amount of later engineering recovers a vintage BLS
   overwrote. This is why M0 ships first and why the fast loop is kept trivial.
2. **Silent correctness failure.** The store can be wrong in a way that produces no error, no alert,
   and plausible-looking output — the seed-provenance bug (§11.2) is the archetype. Defenses are
   structural (N1, provenance gating), not procedural.
3. **Alert fatigue disabling the invariants.** The review states it directly: an invariant that
   fires false positives gets disabled, which is worse than never writing it. Defenses are the
   ASSERT/FLAG split, scoped `finding_policy` suppression, and the errata *rate budget* — expecting
   ~12/year rather than treating each one as an anomaly.

---

## 21. External-review synthesis record (2026-08-10)

The external critique (`specs/bls-stats-spec-review.md`, commit `a8e5dd6`) came back as a
**first-pass review — not adjudicated in the Chat session**: it records the reviewer's own
verification caveats but no positions reached under push-back. Adjudication was therefore
performed in-repo (describe-critique-methodology, synthesize mode, 2026-08-10); two points went to
the project owner, and both decisions are recorded below. Locators **(R-n)** throughout this
document cite this table; the "Review anchor" column names the section of the critique each point
comes from.

| R | Review anchor | Verdict | Disposition |
|---|---|---|---|
| R1 | Key Findings 1; Details (1) | accept | §2 prior-art subsection; no design change |
| R2 | Details (2a); improvement 5 | accept | §9.1 argument re-stated as a choice with conceded costs; §20 issue 3 |
| R3 | Improvement 4 (adopt a format); Stage 3 | **reject** | A catalog is a second durable system §1.4 rules out; delta-rs single-writer re-imports a format whose remaining value R4 captures; §20 issue 3's reversal triggers stand |
| R4 | Improvement 4 (emulate statistics) | accept | §9.1 manifest statistics; §10.1 pruning; §17.4 compaction `_stats.json` |
| R5 | Key Findings 3; Details (2b); improvement 1 | accept — **adjudicated: lightweight form** | §9.1 manifest attestation: async OpenTimestamps / RFC 3161, never blocking, batch-root fallback; N10; `verify attest` |
| R6 | Improvement 2 (fixity) | accept | §17.4 fixity requirement; N11; `verify fixity` |
| R7 | Details (2d); Stage 2 (replication) | accept — **adjudicated: stated deployment requirement** | §17.4 second-copy requirement, `doctor`-reported, one-release-cycle threshold; N12 |
| R8 | Improvement 2 (external custodian) | **reject** | An organizational action, not spec content; revisit as an ops decision if R7's second copy lands with an external host |
| R9 | Improvement 3 (WARC container); Stage 4 | **reject** | Replay tooling serves a §1.3 non-goal; content-addressed keys are load-bearing for P2's lock-free idempotency; R10 keeps a later WARC wrap lossless |
| R10 | Improvement 3 (headers); Details (2c) | accept | §7.3 full header capture; §20 issue 13 closed decode-only |
| R11 | Improvement 6 | accept | §11.5 formalized as invariant 26b |
| R12 | Details (4), issues 1–2 | accept | §7.1 mandatory contactable UA; §20 issue 2 position corrected |
| R13 | Details (4), issue 4 | accept | §20 issue 4: singlefile CSVs settled; sizes stay (open) by measurement |
| R14 | Details (4), issue 5 | accept | §5.3 citation; §20 issue 5 closed; P5 lower-bound encoding unchanged |
| R15 | Details (4), issue 12 | accept | §12.9 threshold anchored to QCEW's published revision envelope; §20 issue 12 |
| R16 | Caveats (QCEW Dec→Jan unverified) | accept — with (open) | §18.2 golden case carries the verification marker; the 2019-09-09 reload is the verified fallback fixture |
| R17 | Key Findings 4; Caveats (−862k NSA) | accept | §1.1 and §13.2 dated evidence; the uncorroborated −898k SA figure is never used |
| R18 | Recommendations (polars note) | accept | §16 pin-and-gate; §18.1 memory-envelope layer |
| R19 | Details (4), closing risk 2 | accept — no change needed | §18.1's as-of property test already asserts the point-in-time-join property; citation recorded |
| R20 | Caveats (Akamai attribution) | accept — no change needed | The term does not appear in this document; it concerns the companion domain review |

Untouched by the review's verification pass and unchanged here: §20 issues 6, 7, and 11. The
review independently endorsed `restamp_only` (§6.1) as a deliberate, defensible deviation from
ALFRED's revision-only vintage model — recorded in §2's prior-art note — and corroborated the
API-quota characterization the design already treats as spot-check-only (§1.3).
