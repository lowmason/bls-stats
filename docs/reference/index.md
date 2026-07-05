# API reference

Generated from the package's docstrings by
[mkdocstrings](https://mkdocstrings.github.io/). The import name is `bls_stats`.

## Layering

Dependencies point strictly downward — lower layers never import higher ones:

```text
cli
 └── pipeline
      └── engines · releases · vintage · storage · enrich
           └── core
                └── registry
```

Two approved sibling edges exist inside the middle layer: `vintage → storage` (the ledger persists
through the store) and `enrich → storage` (metadata export writes Delta tables).

## Modules

| Page | Modules | Role |
|---|---|---|
| [registry](registry.md) | `bls_stats.registry` | The eight programs as data: specs, URLs, revision profiles. |
| [core](core.md) | `config` · `http` · `periods` · `series_id` | Settings, the shared HTTP client, period grammar, series-ID parsing. |
| [releases](releases.md) | `feeds` · `calendar` · `profiles` | Release detection, the release-date calendar, slot expansion. |
| [vintage](vintage.md) | `ledger` | The append-only slot ledger. |
| [storage](storage.md) | `backend` · `delta` · `reads` · `doctor` | The Delta vintage store, canonical reads, pre-flight probes. |
| [engines](engines.md) | `labstat` · `qcew` · `oews` · `ep` · `api_v2` | Per-source download + parse. |
| [enrich](enrich.md) | `cps` | CPS metadata tables and enrichment joins. |
| [pipeline](pipeline.md) | `bls_stats.pipeline` | The orchestrator: ingest, backfill, validation, exit codes. |
| [cli](cli.md) | `bls_stats.cli` | The typer command surface. |
