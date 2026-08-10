# Stage-1 ground-truth probes

Method record for `specs/bls-stats-stage1-findings.md` (roadmap Stage 1). Not package
code — Stage 2 bootstraps `src/bls_stats/`.

Run any probe from the repo root:

    uv run probes/<script>.py

Raw dated output lands in `probes/results/` and is committed; the findings document
cites it.

Rules (spec §7.1, R12, §1.4):

- Contactable User-Agent on every BLS request — defined once in `_lib.py`.
- **Never** send a bare/default UA to a BLS host, even to "confirm the 403": BLS
  blocks non-compliant robots in real time, and a blocked IP forfeits Stage-2 capture.
- Sequential requests only, ≥ 2 s apart. One full-file download total (`qcew_sizes.py`).
- `objstore_capabilities.py` creates only throwaway `bls-stats-probe-*` buckets and
  deletes them. The real buckets are created in Stage 2 from the findings' parameter
  sheet — never here.
