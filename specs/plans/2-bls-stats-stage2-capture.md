# Stage 2: Capture Plane (M0) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: implement this plan task-by-task via
> subagent-driven-development (the default) — or executing-plans when your human partner chose
> inline execution at the handoff. Steps use checkbox (`- [ ]`) syntax for tracking.

> Roadmap: specs/bls-stats-spec-roadmap.md, Stage 2 — on plan completion, tick the stage and
> re-validate later stages against what shipped.

**Goal:** Permanent byte capture for every in-scope artifact — the lock-free plane of §17.3,
deployed and running on a schedule, writing content-addressed blobs and `fetch_log` records and
nothing else.

**Architecture:** Bootstrap the package (`uv` + `hatchling`, `src/bls_stats/`). A purpose-built
`objstore` adapter is the only module that talks to the object store (§16.1). A `transport` layer
carries three configured profiles over `httpx`; the sweep HEAD-polls every artifact in a TOML
inventory, escalates to a streaming GET on a changed validator, hashes the wire bytes and the
transfer-decoded bytes in one pass, PUTs the blob at its content-addressed key, and appends a
JSONL `fetch_log` record. **There is no Parquet, no schema, no ledger, no parsing and no writer
lease in this stage** (§19 M0) — the one piece of derived state the capture path needs, "what did
this URL look like last time", is a pure fold over `log/fetch/` itself.

**Tech Stack:** Python ≥ 3.12, `uv` + `hatchling`, `httpx[http2]`, `boto3`, `typer`,
`pydantic` + `pydantic-settings`, `tenacity`, `structlog`. Dev: `pytest`, `respx`, `ruff`,
`mypy --strict`.

## Global Constraints

Project-wide requirements, copied from the spec. Every task's requirements include this section.

**Platform and packaging (§1.4, §16)**
- Python ≥ 3.12; `uv` + `hatchling`; `typer` CLI; `httpx` for all HTTP; `uv.lock` committed and
  `uv sync --frozen` in the image.
- **`polars` for all tabular data (no pandas)** — Stage 2 has no tabular data and does not declare
  it. Each stage declares the subset of §16's dependency table it actually uses; later stages add
  the rest. This is not a §16 gap.
- Compute is ephemeral containers with no durable local disk; scratch under `$TMPDIR`, deleted
  after use (§17.2).
- Memory envelope: ~4 cores / 25 GB RAM, **target peak RSS < 8 GB** — streaming is mandatory,
  never `.read()` a whole artifact into memory (§1.4, §16.1).

**Object store (§1.4, §16.1)**
- An S3-compatible object store is the only durable storage, reached at a configurable endpoint
  (`AWS_ENDPOINT_URL`). **No endpoint, region, or credential is ever a constant.**
- The only S3 operations relied on are `PUT`, `GET`, `HEAD`, `LIST`, `DELETE` on individual
  objects, with **single-object PUT as the sole atomicity primitive**.
- `objstore` is the **only** module that talks to the object store.
- **Every write failure raises.** There is no falsy return path on the write side.
- **A read may return `None` only for a genuinely absent object.** Any other failure raises.
- `list(prefix)` is **fully paginated**, continuation-token driven. A capped or truncating listing
  is a defect, not a tuning parameter.
- `storage_options()` emits `object_store` key names (`aws_endpoint_url`, not `endpoint_url`), adds
  `aws_allow_http` iff the endpoint scheme is `http`, and **never falls back to ambient AWS
  credential resolution** — a missing credential raises.
- Two buckets: `bls-stats-raw` (archive; immutable) and `bls-stats-main` (`log/`, `ledger/`,
  `store/`, `ops/`). Names confirmed by the operator 2026-08-11 (findings §7, decision 2).

**Transport (§7.1, §7.2, R12)**
- **A descriptive, contactable User-Agent is mandatory from the first request, not a courtesy** —
  the flat-file host 403s bare user agents and BLS policy permits real-time blocking of
  non-compliant robots.
- **NEVER send a bare or default User-Agent to any BLS host**, and never send browser-shaped
  headers to `www.bls.gov`: Stage 1 measured them blocked 6/6 (findings §3). The `html` profile
  uses the same contact headers as `flatfile` (§7.1, as amended 2026-08-11).
- `flatfile` pins `Accept-Encoding: gzip` so archived bytes are reproducible across
  implementations and across time (§7.3). Conservative concurrency (≤ 4).
- All profiles: `tenacity` retry with jitter on 5xx/429/timeout, no retry on 4xx except 403/429,
  per-host token bucket, and **every request — including failures — appended to `fetch_log`**.
- Robots clearance is per-path, not per-host: check any `www.bls.gov` path not among findings §3's
  six against a re-fetched `robots.txt` generic `User-agent: *` block before fetching it.

**Capture (P1, P2, §7.3)**
- Every distinct byte sequence ever retrieved is stored, content-addressed by SHA-256, immutably,
  forever. Bytes stored **exactly as received**, including the original `Content-Encoding`.
- **Two hashes, not one.** `wire_sha256` (bytes off the socket) is the blob key and the forensic
  record; `content_sha256` (after transfer-encoding decode) is the change signal. Decode-only —
  no line-ending or BOM normalization (§20 issue 13, closed).
- The durability boundary is the blob PUT plus the `fetch_log` append. Neither is a store write;
  nothing serializes them.
- **One object per process-run, never an append in place** — object storage has no append.
- Ordering: `HEAD` (detect change) → `GET` (stream to archive) → append `fetch_log`.

**CLI (§15)**
- **Every command is idempotent.** Re-running after a crash is always safe.
- Exit codes: `0` success · `1` unexpected error · `10` ASSERT failure · `20` late slots/triage ·
  `30` degraded (an HTML surface unreachable, ingest continuing) · `40` writer lease unavailable.
  Stage 2 can emit `0`, `1`, `30`; the enum carries all six from the start.
- Everything Stage 2 ships is **lock-free** — `watch`, `capture`, `tick --fast|--sweep` take no
  lease and write no store table (§17.3).

**Testing (§18.1, §18.3)**
- Transport: `respx`-mocked httpx — 403 handling, `Last-Modified` semantics, restamp-only,
  retry/backoff.
- `objstore`: local logic tests, plus a `real_store` marker for a live round-trip asserting that
  `storage_options()` produces keys the reader/writer actually honour, that `list()` paginates past
  one page, and that a failed write raises. **A mocked object store cannot catch a wrong option
  key — it accepts whatever it is handed.**
- Integration: `network` marker, excluded from CI, run on demand against live BLS.
- `ruff` and `mypy --strict` pass.

**Project conventions**
- Commit messages: plain imperative mood, matching the existing log (no `feat:` prefixes).
- No secrets in committed output. Local secrets live in repo-root `.project.env` (git-ignored).
- Every finding is dated. Service behavior is point-in-time — a result is evidence for its date.

**Two recorded deviations from the spec's literal text.** Both are deliberate; annotate them as
`> Deviation:` notes at the steps that introduce them, so review reads them as decisions, not drift.

1. **`fetch_log` objects are `<run_id>-<seq>.jsonl`, not one `<run_id>.jsonl` per run** (§7.3).
   The constraint §7.3 states is "never an append in place", whose stated purpose is that
   "concurrent lock-free capture processes never contend" — unique keys satisfy that regardless of
   how many a run writes. Flushing once per artifact instead of once per sweep means a crash
   mid-sweep loses no records for artifacts already captured, which matters because the log is half
   the durability boundary (P2). Cost is one small PUT per artifact (~100/day).
2. **`fetch_log` has no `program` or `artifact_key` column, and Stage 2 does not add one.** §7.3's
   field list is definitional and omits both, while `file_vintage` (§7.4) carries them — so the
   Stage-3 writer resolves `url → (program, artifact_key)` through the artifact inventory this
   stage produces. Recorded here because it is a real interface obligation on Stage 3, and because
   a URL later re-keyed in the inventory would make historical log records unmappable.

## Out of scope, stated so review does not read these as gaps

- **No store writes at all** — no `file_vintage`, no parsing, no ledger, no schema, no manifest, no
  writer lease (§19 M0). Those are Stage 3.
- **No slots.** `tick --fast` ships with its due-artifact seam stubbed; Stage 4 wires it.
- **No metadata *parsing*.** `tick --sweep` fetches and archives the `.ics`, feed, and errata
  surfaces as blobs with `fetch_log` records, "exactly like a data artifact" (§17.1). Ingesting
  those blobs into the ledger is `tick --write`, Stage 4.
- **No notices surfaces.** §17.1's `--sweep` row names four sync surfaces; the inventory covers
  three. The fourth, `notices sync`, needs the hand-enumerated subject-area registry that §12.5
  and §20 issue 9 place at Stage 9 — there is no URL list to sweep yet. Deliberate, not a miss.
- **No headless `HtmlFetcher`.** Left unbuilt-and-pluggable while the contact profile passes
  (§7.2 rung 4; findings §8).
- **No `api` call path.** §7.1's `api` profile is configured but unused — §1.3 makes the API
  spot-check-only and Stage 2 has no command that spot-checks. Its "never trust HTTP 200 or
  `REQUEST_SUCCEEDED`" rule ships as standing policy for whichever stage first calls it.
- **No replication job (N12).** The mechanism decision is pending (findings §6; deferred item 2).
  **Consequence, chosen rather than overlooked:** N12's single-copy period starts accruing at this
  stage's first capture, so the archive is single-copy until a later stage builds it — a standing
  finding from day one, reported by `doctor` at Stage 7.
- **No attestation (R5).** Fires after the `release_event` PUT, which is Stage 3. Task 3 creates
  the `attest/` prefix only because the lock-inheritance gate object lives there.

## Deferred items this plan closes

Tick these in `specs/deferred_items.md` at the completion gate:

- **3 — spec amendment (§7.1 `html` row, §7.2 mitigation ladder).** Already done: commit `9cf6cc6`,
  2026-08-11, as a prerequisite to writing this plan.
- **7 — delete-deny check omits `VersionId`.** Task 3 re-runs it against the non-admin runtime
  credential covering **both** a delete-marker attempt and a versioned-delete attempt.
- **8 — delete-deny policy action coverage.** Needs operator input 3 below; Task 3 provisions the
  runtime credential to whatever that answer says.
- **1 and 2 — deployment endpoint matrix and replication mechanism.** Closed only if operator
  inputs 1–2 and 4 arrive. If they do not, they stay deferred and Task 3/Task 10 execute against
  the dev endpoint alone, which does **not** satisfy this stage's Exit criterion (see Task 10).

## Execution-time inputs (stop and ask your human partner if missing)

0. **Development endpoint (known — this workstation, verified 2026-08-10):** MinIO via the
   LaunchAgent `~/Library/LaunchAgents/com.lowell.minio.plist`, serving `/Users/lowell/S3` at
   `http://127.0.0.1:9000`. Credentials in repo-root `.project.env` (git-ignored; load with
   `set -a; source .project.env; set +a`). This is the **root** credential — it bypassed the
   delete-deny bucket policy at Stage 1, which is exactly why input 3 exists.
1. **Deployment endpoint credentials** — `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`,
   `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`. **Blocking for Task 3's deployment run and for Task 10.**
   Not in `.project.env` — its `AWS_*` values name the dev MinIO. (Deferred item 1.)
2. **A deployment-side container host with a shell** — `docker`/`podman` or the platform's
   equivalent. **Blocking for Task 10.** (Deferred item 1; §20 issue 14.)
3. **Runtime-credential permission grant (deferred item 8).** The findings' delete-deny policy JSON
   denies only `s3:DeleteObject` / `s3:DeleteObjectVersion`. Recommendation on record: the
   non-admin runtime credential's own grant should **additionally exclude**
   `s3:PutBucketLifecycleConfiguration`, `s3:PutBucketPolicy`, and `s3:DeleteBucketPolicy` on the
   raw bucket — an expiration rule and a policy rewrite are each a path to deletion the pinned
   policy does not cover. **Answer this before Task 3 provisions the credential.**
4. **Replication mechanism (findings §6; deferred item 2).** Default is option B (second endpoint +
   pull job) until the deployment endpoint's `replication_api` result is known. Not built here —
   confirm the standing-finding consequence above is accepted.
5. **BLS contact email** — `BLS_CONTACT_EMAIL` in `.project.env`, already set and used by the
   Stage-1 probes. Note the fallback-address discrepancy recorded as deferred item 9.

## File structure

| Path | Responsibility |
|---|---|
| `pyproject.toml`, `uv.lock` | Packaging, pinned deps, `ruff`/`mypy`/`pytest` config |
| `src/bls_stats/config/settings.py` | `pydantic-settings`; endpoint, buckets, contact email. No AWS defaults |
| `src/bls_stats/objstore/client.py` | The §16.1 adapter. The only module that talks to the store |
| `src/bls_stats/objstore/buckets.py` | Findings §7's creation sheet + its three hard gates, executable |
| `src/bls_stats/transport/profiles.py` | The three §7.1 profiles as data |
| `src/bls_stats/transport/client.py` | httpx clients, pacing, retry, dual-hash streaming GET |
| `src/bls_stats/capture/inventory.py` | Inventory loader + `url → artifact` map |
| `src/bls_stats/capture/discover.py` | LABSTAT directory enumeration, for building the inventory |
| `src/bls_stats/capture/fetchlog.py` | `FetchLogRecord` + the per-run JSONL writer |
| `src/bls_stats/capture/state.py` | The fold over `log/fetch/` → per-URL `WatchState` |
| `src/bls_stats/capture/pipeline.py` | One artifact: decide → fetch → archive → record |
| `src/bls_stats/cli/*.py` | `typer` app; `capture`, `watch`, `tick`, `admin buckets`; `runtime.py` is the single dependency seam |
| `src/bls_stats/config/artifacts.toml` | The artifact inventory (package data — §16 puts TOML overlays under `config/`) |
| `deploy/Containerfile`, `deploy/crontab`, `deploy/README.md` | Image, schedule, runbook |
| `tests/**` | Mirrors the source tree; `network` and `real_store` markers |

## Task dependency order

Task 1 first. Task 2 needs 1. Task 3 needs 2 (and operator inputs 1, 3). Task 4 needs 1. Task 5
needs 2 and 4. Task 6 needs 4. Task 7 needs 2 and 5. Task 8 needs 4–7. Task 9 needs 8. Task 10
needs 9 and operator inputs 1–2. Tasks 3 and 4–6 are independent of each other and may run in
either order.

---

### Task 1: Package bootstrap, settings, test scaffolding

**Files:**
- Create: `pyproject.toml`, `.gitignore` (extend), `README.md`
- Create: `src/bls_stats/__init__.py`, `src/bls_stats/config/__init__.py`,
  `src/bls_stats/config/settings.py`
- Test: `tests/conftest.py`, `tests/test_settings.py`

**Interfaces:**
- Consumes: nothing (the repo has `specs/` and `probes/` only; `probes/` is Stage-1 method code and
  is **not** imported by the package).
- Produces (used by every later task):
  - `bls_stats.config.settings.Settings` — pydantic-settings model with fields
    `aws_endpoint_url: str`, `aws_access_key_id: str`, `aws_secret_access_key: str`,
    `aws_region: str = "us-east-1"`, `raw_bucket: str = "bls-stats-raw"`,
    `main_bucket: str = "bls-stats-main"`, `bls_contact_email: str`, and property
    `endpoint_is_http: bool`.
  - `bls_stats.config.settings.load_settings() -> Settings`
  - pytest markers `network` and `real_store`, opted in by `--network` / `--real-store`.

- [ ] **Step 1: Write `pyproject.toml`**

Only the dependencies Stage 2 uses; later stages add the rest of §16's table.

```toml
[project]
name = "bls-stats"
version = "0.1.0"
description = "Point-in-time vintage store of BLS statistical output"
requires-python = ">=3.12"
dependencies = [
    "httpx[http2]>=0.27",
    "boto3>=1.35.35",
    "typer>=0.12",
    "pydantic>=2.7",
    "pydantic-settings>=2.3",
    "tenacity>=8.3",
    "structlog>=24.1",
]

[project.scripts]
bls-stats = "bls_stats.cli.app:app"

[dependency-groups]
dev = [
    "pytest>=8.2",
    "respx>=0.21",
    "ruff>=0.5",
    "mypy>=1.10",
    "boto3-stubs[s3]>=1.35",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/bls_stats"]

[tool.ruff]
line-length = 100
src = ["src", "tests"]

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM"]

[tool.mypy]
strict = true
files = ["src", "tests"]

# Strict on the package; tests may use untyped fixtures.
[[tool.mypy.overrides]]
module = "tests.*"
disallow_untyped_defs = false
disallow_untyped_calls = false

[tool.pytest.ini_options]
testpaths = ["tests"]
markers = [
    "network: hits live BLS hosts; excluded from CI (spec 18.1)",
    "real_store: hits a live object-store endpoint (spec 16.1, 18.1)",
]
# No `-m` here on purpose. An `addopts` marker expression is applied on every
# invocation and WINS over --network / --real-store, so `pytest --real-store`
# would deselect the very tests the flag exists to run. The conftest hook
# below is the whole mechanism: unmarked by default, opt in by flag.
```

- [ ] **Step 2: Write `src/bls_stats/config/settings.py`**

```python
"""Configuration. Nothing here has an AWS default: spec 1.4 — no endpoint,
region, or credential is ever a constant."""

from __future__ import annotations

from functools import lru_cache
from urllib.parse import urlparse

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # populate_by_name so tests can construct this directly: a
    # validation_alias otherwise makes the field name unusable as a kwarg.
    model_config = SettingsConfigDict(
        env_file=".project.env", extra="ignore", populate_by_name=True)

    aws_endpoint_url: str = Field(validation_alias="AWS_ENDPOINT_URL")
    aws_access_key_id: str = Field(validation_alias="AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str = Field(validation_alias="AWS_SECRET_ACCESS_KEY")
    aws_region: str = Field("us-east-1", validation_alias="AWS_REGION")
    raw_bucket: str = Field("bls-stats-raw", validation_alias="BLS_RAW_BUCKET")
    main_bucket: str = Field("bls-stats-main", validation_alias="BLS_MAIN_BUCKET")
    bls_contact_email: str = Field(validation_alias="BLS_CONTACT_EMAIL")

    @property
    def endpoint_is_http(self) -> bool:
        return urlparse(self.aws_endpoint_url).scheme == "http"


@lru_cache(maxsize=1)
def load_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
```

- [ ] **Step 3: Write `tests/conftest.py`**

```python
from __future__ import annotations

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--network", action="store_true", help="run live-BLS tests")
    parser.addoption("--real-store", action="store_true", help="run live object-store tests")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    enabled = {
        "network": config.getoption("--network"),
        "real_store": config.getoption("--real-store"),
    }
    for item in items:
        for marker, on in enabled.items():
            if marker in item.keywords and not on:
                item.add_marker(pytest.mark.skip(reason=f"needs --{marker.replace('_', '-')}"))


@pytest.fixture
def store_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://127.0.0.1:9000")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
    monkeypatch.setenv("BLS_CONTACT_EMAIL", "tests@example.invalid")
```

Note the `addopts` in `pyproject.toml` deselects both markers by default and the hook skips them
unless opted in — belt and braces, because a marked test that silently runs in CI against a live
BLS host is the failure R12 warns about.

- [ ] **Step 4: Write the failing test**

`tests/test_settings.py`:

```python
from __future__ import annotations

import pytest
from pydantic import ValidationError

from bls_stats.config.settings import Settings


def test_settings_load_from_env(store_env: None) -> None:
    s = Settings()  # type: ignore[call-arg]
    assert s.aws_endpoint_url == "http://127.0.0.1:9000"
    assert s.raw_bucket == "bls-stats-raw"
    assert s.main_bucket == "bls-stats-main"
    assert s.endpoint_is_http is True


def test_missing_credential_raises_never_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec 16.1: a missing credential raises rather than silently resolving
    to something else (ambient AWS credential resolution). `_env_file=None`
    so a developer's own .project.env cannot make this pass."""
    for var in ("AWS_ENDPOINT_URL", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
                "BLS_CONTACT_EMAIL"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(ValidationError):
        Settings(_env_file=None)  # type: ignore[call-arg]


def test_https_endpoint_is_not_http(monkeypatch: pytest.MonkeyPatch, store_env: None) -> None:
    monkeypatch.setenv("AWS_ENDPOINT_URL", "https://objects.example.invalid")
    assert Settings().endpoint_is_http is False  # type: ignore[call-arg]


def test_settings_are_constructible_by_field_name() -> None:
    """populate_by_name: every later test builds Settings directly rather
    than round-tripping through the environment."""
    s = Settings(_env_file=None, aws_endpoint_url="http://x:9000",  # type: ignore[call-arg]
                 aws_access_key_id="k", aws_secret_access_key="s",
                 bls_contact_email="t@example.invalid")
    assert s.raw_bucket == "bls-stats-raw"
```

- [ ] **Step 5: Run the tests and confirm they fail, then pass**

```bash
uv run pytest tests/test_settings.py -v
```

Expected before Step 2's file exists: `ModuleNotFoundError: No module named 'bls_stats'`.
Expected after: 4 passed.

- [ ] **Step 6: Write `README.md` and extend `.gitignore`**

`README.md` states: what the package is (one paragraph, pointing at `specs/bls-stats-spec.md`), how
to run (`uv sync`, `uv run bls-stats --help`), and the test invocations including the two opt-in
markers. **Do not name any employer or deployment operator.**

Append to `.gitignore`: `.venv/`, `dist/`, `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`.

- [ ] **Step 7: Run the full gate**

```bash
uv sync && uv run ruff check . && uv run mypy && uv run pytest -q
```

Expected: ruff clean, mypy clean, 4 passed.

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml uv.lock .gitignore README.md src tests
git commit -m "Bootstrap the bls-stats package and settings"
```

---

### Task 2: The object-store adapter (§16.1)

**Files:**
- Create: `src/bls_stats/objstore/__init__.py`, `src/bls_stats/objstore/client.py`
- Test: `tests/objstore/test_client.py`, `tests/objstore/test_real_store.py`

**Interfaces:**
- Consumes: `Settings` from Task 1.
- Produces (used by Tasks 3, 5, 7, 8, 9, 10):
  - `ObjectStoreError(RuntimeError)`
  - `ObjectHead` — frozen dataclass `key: str`, `size: int`, `etag: str`,
    `last_modified: datetime`
  - `ObjectStore(settings: Settings, bucket: str)` with:
    - `storage_options() -> dict[str, str]`
    - `put_atomic(key: str, body: bytes) -> None`
    - `put_stream(key: str, fileobj: BinaryIO) -> None`
    - `get_bytes(key: str) -> bytes | None`
    - `open_stream(key: str, chunk_size: int = 1 << 20) -> Iterator[bytes] | None`
    - `list(prefix: str) -> Iterator[str]`
    - `head(key: str) -> ObjectHead | None`
    - `exists(key: str) -> bool`
    - `append_jsonl(key: str, records: Iterable[Mapping[str, Any]]) -> None`
  - `raw_store(settings) -> ObjectStore`, `main_store(settings) -> ObjectStore`

- [ ] **Step 1: Write the failing tests**

`tests/objstore/test_client.py` — the logic that does not need a live endpoint:

```python
from __future__ import annotations

import io
import json

import pytest
from botocore.exceptions import ClientError

from bls_stats.config.settings import Settings
from bls_stats.objstore.client import ObjectStore, ObjectStoreError


def _store(**over: str) -> ObjectStore:
    env = {
        "AWS_ENDPOINT_URL": "http://127.0.0.1:9000",
        "AWS_ACCESS_KEY_ID": "k",
        "AWS_SECRET_ACCESS_KEY": "s",
        "BLS_CONTACT_EMAIL": "t@example.invalid",
    } | over
    return ObjectStore(Settings(_env_file=None, **{k.lower(): v for k, v in env.items()}),
                       bucket="b")


def test_storage_options_use_object_store_key_names() -> None:
    opts = _store().storage_options()
    assert set(opts) == {
        "aws_endpoint_url", "aws_access_key_id", "aws_secret_access_key",
        "aws_region", "aws_allow_http",
    }
    assert "endpoint_url" not in opts, "boto3 key name would silently fall back to ambient AWS"


def test_allow_http_only_for_http_endpoints() -> None:
    assert _store().storage_options()["aws_allow_http"] == "true"
    https = _store(AWS_ENDPOINT_URL="https://objects.example.invalid")
    assert "aws_allow_http" not in https.storage_options()


def test_absent_read_returns_none_but_other_errors_raise(monkeypatch: pytest.MonkeyPatch) -> None:
    store = _store()

    def raise_code(code: str):
        def _call(**_: object) -> None:
            raise ClientError({"Error": {"Code": code}}, "GetObject")
        return _call

    monkeypatch.setattr(store._client, "get_object", raise_code("NoSuchKey"))
    assert store.get_bytes("missing") is None

    monkeypatch.setattr(store._client, "get_object", raise_code("AccessDenied"))
    with pytest.raises(ObjectStoreError):
        store.get_bytes("forbidden")


def test_write_failure_raises_never_returns_falsy(monkeypatch: pytest.MonkeyPatch) -> None:
    store = _store()

    def boom(**_: object) -> None:
        raise ClientError({"Error": {"Code": "InternalError"}}, "PutObject")

    monkeypatch.setattr(store._client, "put_object", boom)
    with pytest.raises(ObjectStoreError):
        store.put_atomic("k", b"v")


def test_append_jsonl_serializes_one_object(monkeypatch: pytest.MonkeyPatch) -> None:
    store = _store()
    seen: dict[str, bytes] = {}
    monkeypatch.setattr(store, "put_atomic", lambda key, body: seen.update({key: body}))
    store.append_jsonl("log/fetch/dt=2026-08-11/r-0001.jsonl", [{"a": 1}, {"a": 2}])
    lines = seen["log/fetch/dt=2026-08-11/r-0001.jsonl"].decode().splitlines()
    assert [json.loads(line)["a"] for line in lines] == [1, 2]
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest tests/objstore/test_client.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'bls_stats.objstore'`.

- [ ] **Step 3: Write `src/bls_stats/objstore/client.py`**

```python
"""The object-store adapter (spec 16.1).

The ONLY module that talks to the object store, so the endpoint's identity
and capabilities are known in exactly one place. Purpose-built rather than a
general-purpose S3 helper: whole-object buffering reads violate 1.4's
streaming mandate, unpaginated listings silently truncate an archive of
millions of objects, and log-and-return-falsy error handling loses a vintage
without raising, which P1 ranks the one unrecoverable failure in the system.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, BinaryIO

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError

from bls_stats.config.settings import Settings

# Codes that mean "this object is genuinely not there", as opposed to "the
# request failed". Absence is a real answer callers act on (spec 16.1).
_ABSENT_CODES = frozenset({"404", "NoSuchKey", "NotFound"})


class ObjectStoreError(RuntimeError):
    """Any object-store failure. There is no falsy return on the write side."""


@dataclass(frozen=True)
class ObjectHead:
    key: str
    size: int
    etag: str
    last_modified: datetime


def _is_absent(exc: ClientError) -> bool:
    err = exc.response.get("Error", {})
    status = str(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", ""))
    return str(err.get("Code", "")) in _ABSENT_CODES or status == "404"


class ObjectStore:
    def __init__(self, settings: Settings, bucket: str) -> None:
        self._settings = settings
        self.bucket = bucket
        self._client = boto3.client(
            "s3",
            endpoint_url=settings.aws_endpoint_url,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

    # -- configuration ---------------------------------------------------

    def storage_options(self) -> dict[str, str]:
        """The spec 9.1 dict, in `object_store` key names, and the single
        source of it. `aws_endpoint_url`, NOT boto3's `endpoint_url`: the
        overlapping subset makes a wrong mapping look correct while the
        client silently falls back to ambient AWS credential resolution."""
        opts = {
            "aws_endpoint_url": self._settings.aws_endpoint_url,
            "aws_access_key_id": self._settings.aws_access_key_id,
            "aws_secret_access_key": self._settings.aws_secret_access_key,
            "aws_region": self._settings.aws_region,
        }
        if self._settings.endpoint_is_http:
            opts["aws_allow_http"] = "true"
        return opts

    # -- writes: every failure raises ------------------------------------

    def put_atomic(self, key: str, body: bytes) -> None:
        """Single-object PUT — the sole atomicity primitive (spec 1.4).
        Returns only on confirmed durability."""
        try:
            self._client.put_object(Bucket=self.bucket, Key=key, Body=body)
        except (ClientError, BotoCoreError) as exc:
            raise ObjectStoreError(f"put_atomic s3://{self.bucket}/{key}") from exc

    def put_stream(self, key: str, fileobj: BinaryIO) -> None:
        try:
            self._client.upload_fileobj(fileobj, self.bucket, key)
        except (ClientError, BotoCoreError, OSError) as exc:
            raise ObjectStoreError(f"put_stream s3://{self.bucket}/{key}") from exc

    def append_jsonl(self, key: str, records: Iterable[Mapping[str, Any]]) -> None:
        """One uniquely-named object per flush under `log/fetch/dt=.../`.
        Half of P2's durability boundary. Object storage has no append."""
        body = b"".join(
            json.dumps(r, separators=(",", ":"), default=str).encode("utf-8") + b"\n"
            for r in records
        )
        self.put_atomic(key, body)

    # -- reads: None only for genuine absence -----------------------------

    def get_bytes(self, key: str) -> bytes | None:
        try:
            resp = self._client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if _is_absent(exc):
                return None
            raise ObjectStoreError(f"get_bytes s3://{self.bucket}/{key}") from exc
        except BotoCoreError as exc:
            raise ObjectStoreError(f"get_bytes s3://{self.bucket}/{key}") from exc
        body: Any = resp["Body"]
        try:
            return bytes(body.read())
        finally:
            body.close()

    def open_stream(self, key: str, chunk_size: int = 1 << 20) -> Iterator[bytes] | None:
        """Chunked read; never `.read()` into memory. `None` iff absent."""
        try:
            resp = self._client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if _is_absent(exc):
                return None
            raise ObjectStoreError(f"open_stream s3://{self.bucket}/{key}") from exc
        except BotoCoreError as exc:
            raise ObjectStoreError(f"open_stream s3://{self.bucket}/{key}") from exc

        def _chunks() -> Iterator[bytes]:
            body: Any = resp["Body"]
            try:
                yield from body.iter_chunks(chunk_size)
            finally:
                body.close()

        return _chunks()

    def list(self, prefix: str) -> Iterator[str]:
        """Fully paginated, continuation-token driven, lazy. A capped or
        truncating listing is a defect: `verify replay` and the ledger
        rebuild both enumerate the whole archive, and a silent truncation
        there reports a clean reconciliation over data it never looked at."""
        paginator = self._client.get_paginator("list_objects_v2")
        try:
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    yield str(obj["Key"])
        except (ClientError, BotoCoreError) as exc:
            raise ObjectStoreError(f"list s3://{self.bucket}/{prefix}") from exc

    def head(self, key: str) -> ObjectHead | None:
        try:
            resp = self._client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if _is_absent(exc):
                return None
            raise ObjectStoreError(f"head s3://{self.bucket}/{key}") from exc
        except BotoCoreError as exc:
            raise ObjectStoreError(f"head s3://{self.bucket}/{key}") from exc
        return ObjectHead(
            key=key,
            size=int(resp["ContentLength"]),
            etag=str(resp["ETag"]),
            last_modified=resp["LastModified"],
        )

    def exists(self, key: str) -> bool:
        return self.head(key) is not None


def raw_store(settings: Settings) -> ObjectStore:
    return ObjectStore(settings, settings.raw_bucket)


def main_store(settings: Settings) -> ObjectStore:
    return ObjectStore(settings, settings.main_bucket)
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
uv run pytest tests/objstore/test_client.py -v
```

Expected: 5 passed.

- [ ] **Step 5: Write the live round-trip test (`real_store` marker)**

`tests/objstore/test_real_store.py`. §18.1 is explicit that a mocked object store cannot catch a
wrong option key, so this test is the one that proves the adapter. **The pagination assertion needs
more than one page** — S3 and MinIO default to 1000 keys per `ListObjectsV2` response, so a naive
20-object test passes without ever paging, which is exactly the silent truncation §16.1 calls a
defect. Write 1001 tiny objects.

```python
from __future__ import annotations

import io
import uuid

import pytest

from bls_stats.config.settings import load_settings
from bls_stats.objstore.client import ObjectStore, ObjectStoreError

pytestmark = pytest.mark.real_store


@pytest.fixture
def scratch() -> ObjectStore:
    settings = load_settings()
    store = ObjectStore(settings, bucket=settings.main_bucket)
    return store


def test_round_trip_and_absence(scratch: ObjectStore) -> None:
    key = f"ops/_test/{uuid.uuid4().hex}"
    assert scratch.get_bytes(key) is None, "absence must read as None, not an error"
    scratch.put_atomic(key, b"hello")
    assert scratch.get_bytes(key) == b"hello"
    assert scratch.exists(key)
    head = scratch.head(key)
    assert head is not None and head.size == 5


def test_put_stream_round_trips(scratch: ObjectStore) -> None:
    key = f"ops/_test/{uuid.uuid4().hex}"
    payload = b"x" * (3 << 20)
    scratch.put_stream(key, io.BytesIO(payload))
    stream = scratch.open_stream(key)
    assert stream is not None
    assert sum(len(c) for c in stream) == len(payload)


def test_list_paginates_past_one_page(scratch: ObjectStore) -> None:
    """MinIO/S3 return at most 1000 keys per ListObjectsV2 response. A test
    under that ceiling never exercises the continuation token."""
    prefix = f"ops/_test/page-{uuid.uuid4().hex}/"
    for i in range(1001):
        scratch.put_atomic(f"{prefix}{i:05d}", b"")
    assert sum(1 for _ in scratch.list(prefix)) == 1001


def test_failed_write_raises(scratch: ObjectStore) -> None:
    bad = ObjectStore(scratch._settings, bucket=f"no-such-bucket-{uuid.uuid4().hex}")
    with pytest.raises(ObjectStoreError):
        bad.put_atomic("k", b"v")
```

- [ ] **Step 6: Run it against the dev endpoint**

```bash
set -a; source .project.env; set +a; uv run pytest tests/objstore/test_real_store.py --real-store -v
```

Expected: 4 passed. The pagination test writes 1001 empty objects to `bls-stats-main` — the main
bucket is mutable, so this is cleanup-safe. `bls-stats-main` must already exist; if it does not,
run Task 3 first, or create it by hand and let Task 3 assert its parameters.

> If `bls-stats-main` does not exist yet, this step reorders after Task 3. Record which order was
> taken as a `> Deviation:` note.

- [ ] **Step 7: Commit**

```bash
git add src/bls_stats/objstore tests/objstore
git commit -m "Add the object-store adapter with a live round-trip test"
```

---

### Task 3: Create the buckets and pass the three hard gates

**This is the irreversible task.** §7.3: object-lock immutability is commonly settable only at
bucket creation, "the one storage decision that cannot be corrected later." Findings §7 is the
parameter sheet; this task makes it executable and runs it.

**Files:**
- Create: `src/bls_stats/objstore/buckets.py`
- Create: `src/bls_stats/cli/__init__.py`, `src/bls_stats/cli/app.py`,
  `src/bls_stats/cli/admin.py`
- Test: `tests/objstore/test_buckets.py`

**Interfaces:**
- Consumes: `ObjectStore`, `Settings`.
- Produces (used by Task 10 and, later, Stage 7's `doctor`):
  - `BucketPlan` — frozen dataclass of the sheet's parameters
  - `create_buckets(settings, *, apply: bool) -> list[GateResult]`
  - `verify_buckets(settings) -> list[GateResult]`
  - `GateResult` — frozen dataclass `name: str`, `ok: bool`, `detail: str`
  - `DELETE_DENY_POLICY: str` — findings §7's JSON, verbatim
  - CLI: `bls-stats admin buckets create [--apply]`, `bls-stats admin buckets verify`

**Operator inputs required before this task runs:** input 3 (runtime-credential grant). Input 1 is
required for the deployment run; the dev run proceeds without it.

- [ ] **Step 1: Answer operator input 3, then provision the non-admin runtime credential**

Findings §7's Delete policy row: the only evidence on record is **NOT ENFORCED**, for MinIO's
**root** credential, which bypasses its own bucket policy. Root must not be the runtime credential.

At the dev endpoint:

```bash
mc admin user add local bls-stats-runtime "$(openssl rand -base64 24)"
mc admin policy attach local readwrite --user bls-stats-runtime
```

Then narrow it per input 3's answer: the runtime credential's grant should exclude
`s3:PutBucketLifecycleConfiguration`, `s3:PutBucketPolicy`, `s3:DeleteBucketPolicy` on the raw
bucket (an expiration rule and a policy rewrite are each a path to deletion the pinned policy JSON
does not cover — deferred item 8). Record the resulting policy document in `deploy/README.md`.

Store the new credential in `.project.env` as the runtime credential; keep root separately for
break-glass only.

- [ ] **Step 2: Write `src/bls_stats/objstore/buckets.py`**

The policy JSON is copied **verbatim** from findings §7 — do not reformat it, do not add
statements. Input 3's tightening lands on the *credential's* grant (Step 1), not on this document.

```python
"""Executable form of the archive-bucket creation sheet
(specs/bls-stats-stage1-findings.md section 7; spec 7.3 and 17.4).

Object-lock ENABLEMENT is part of CreateBucket and can never be retrofitted;
default retention is a separate call afterward. Between them sits the gate
this module exists for: a plain PUT with no Retention argument must INHERIT
the bucket default, because the capture path never calls PutObjectRetention
per object, so immutability rests entirely on inheritance. No Stage-1 probe
tested that link.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from bls_stats.config.settings import Settings

RETENTION_MODE = "GOVERNANCE"       # findings section 7, decision 1 (operator, 2026-08-11)
RETENTION_DAYS = 3650               # a mode AND a duration (spec 17.4)
LOCK_INHERIT_CHECK_KEY = "attest/_verify/lock-inherit-check"

DELETE_DENY_POLICY = """{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DenyDeleteOnRaw",
      "Effect": "Deny",
      "Principal": {"AWS": ["*"]},
      "Action": ["s3:DeleteObject", "s3:DeleteObjectVersion"],
      "Resource": ["arn:aws:s3:::bls-stats-raw/*"]
    }
  ]
}"""


@dataclass(frozen=True)
class GateResult:
    name: str
    ok: bool
    detail: str


def assert_policy_covers(settings: Settings) -> None:
    """The policy JSON is pinned verbatim (findings section 7) and names
    `bls-stats-raw`, but the bucket comes from BLS_RAW_BUCKET. Point that at
    another name and the delete-deny policy attaches to a bucket it does not
    cover -- 17.4's hard requirement silently unenforced. Stop instead."""
    if f"arn:aws:s3:::{settings.raw_bucket}/*" not in DELETE_DENY_POLICY:
        raise RuntimeError(
            f"BLS_RAW_BUCKET={settings.raw_bucket!r} is not the bucket the pinned "
            f"delete-deny policy covers. Either use the operator-confirmed name "
            f"(bls-stats-raw, findings section 7 decision 2) or re-open that "
            f"decision -- do not edit the policy JSON in passing.")


def _client(settings: Settings):  # type: ignore[no-untyped-def]
    return boto3.client(
        "s3",
        endpoint_url=settings.aws_endpoint_url,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        region_name=settings.aws_region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def verify_versioning(s3, bucket: str) -> GateResult:  # type: ignore[no-untyped-def]
    """STOP, not a fallback: conditional-PUT dedup, per-version retention and
    compaction all assume versioning is on (findings section 7, Versioning row)."""
    status = s3.get_bucket_versioning(Bucket=bucket).get("Status")
    return GateResult("versioning", status == "Enabled", f"status={status!r}")


def verify_lock_inheritance(s3, bucket: str) -> list[GateResult]:  # type: ignore[no-untyped-def]
    """Findings section 7's post-creation gate, steps 1-3. Failure is a stop."""
    results: list[GateResult] = []

    cfg = s3.get_object_lock_configuration(Bucket=bucket)["ObjectLockConfiguration"]
    rule = cfg.get("Rule", {}).get("DefaultRetention", {})
    ok = (
        cfg.get("ObjectLockEnabled") == "Enabled"
        and rule.get("Mode") == RETENTION_MODE
        and int(rule.get("Days", 0)) == RETENTION_DAYS
    )
    results.append(GateResult("lock_configuration", ok, f"{cfg.get('ObjectLockEnabled')} {rule}"))
    if not ok:
        return results

    # Step 2: no Retention argument, so it can only pick retention up by
    # inheritance. Deliberately NOT under raw/blob/ -- that namespace is
    # content-addressed and N11 re-hashes it against its keys, so a marker
    # keyed by an arbitrary string would be a standing false fixity concern
    # for the life of the object.
    put = s3.put_object(Bucket=bucket, Key=LOCK_INHERIT_CHECK_KEY,
                        Body=b"lock inheritance verification marker\n")
    version_id = put["VersionId"]

    got = s3.get_object_retention(Bucket=bucket, Key=LOCK_INHERIT_CHECK_KEY,
                                  VersionId=version_id)["Retention"]
    expected = dt.datetime.now(dt.UTC) + dt.timedelta(days=RETENTION_DAYS)
    delta = abs((got["RetainUntilDate"] - expected).total_seconds())
    ok = got.get("Mode") == RETENTION_MODE and delta <= 86_400  # +/- 1 day tolerance
    results.append(
        GateResult("lock_inheritance", ok,
                   f"mode={got.get('Mode')} retain_until={got.get('RetainUntilDate')}")
    )
    return results


def verify_delete_deny(s3, bucket: str, probe_key: str) -> list[GateResult]:  # type: ignore[no-untyped-def]
    """Deferred item 7: cover BOTH paths. An unversioned delete against a
    versioned bucket creates a delete marker and exercises s3:DeleteObject
    only; it never exercises s3:DeleteObjectVersion."""
    results: list[GateResult] = []
    versions = s3.list_object_versions(Bucket=bucket, Prefix=probe_key).get("Versions", [])
    version_id = versions[0]["VersionId"] if versions else None

    try:
        s3.delete_object(Bucket=bucket, Key=probe_key)
        results.append(GateResult("delete_deny.marker", False, "delete marker was ACCEPTED"))
    except ClientError as exc:
        results.append(GateResult("delete_deny.marker", True,
                                  str(exc.response["Error"]["Code"])))

    if version_id is None:
        results.append(GateResult("delete_deny.version", False, "no version to attempt"))
        return results
    try:
        s3.delete_object(Bucket=bucket, Key=probe_key, VersionId=version_id)
        results.append(GateResult("delete_deny.version", False, "version delete was ACCEPTED"))
    except ClientError as exc:
        results.append(GateResult("delete_deny.version", True,
                                  str(exc.response["Error"]["Code"])))
    return results


def create_buckets(settings: Settings, *, apply: bool) -> list[GateResult]:
    """Creation, in the sheet's row order, then every gate. `apply=False`
    prints the plan and touches nothing."""
    assert_policy_covers(settings)
    s3 = _client(settings)
    results: list[GateResult] = []
    if not apply:
        return [
            GateResult("plan", True,
                       f"CreateBucket {settings.raw_bucket} ObjectLockEnabledForBucket=true; "
                       f"versioning; retention {RETENTION_MODE}/{RETENTION_DAYS}d; delete-deny; "
                       f"CreateBucket {settings.main_bucket} versioning, no lock, no lifecycle")
        ]

    s3.create_bucket(Bucket=settings.raw_bucket, ObjectLockEnabledForBucket=True)
    s3.put_bucket_versioning(Bucket=settings.raw_bucket,
                             VersioningConfiguration={"Status": "Enabled"})
    s3.put_object_lock_configuration(
        Bucket=settings.raw_bucket,
        ObjectLockConfiguration={
            "ObjectLockEnabled": "Enabled",
            "Rule": {"DefaultRetention": {"Mode": RETENTION_MODE, "Days": RETENTION_DAYS}},
        },
    )
    s3.put_bucket_policy(Bucket=settings.raw_bucket, Policy=DELETE_DENY_POLICY)
    results.append(GateResult("create.raw", True, settings.raw_bucket))

    s3.create_bucket(Bucket=settings.main_bucket)
    s3.put_bucket_versioning(Bucket=settings.main_bucket,
                             VersioningConfiguration={"Status": "Enabled"})
    results.append(GateResult("create.main", True, settings.main_bucket))

    results.extend(verify_buckets(settings))
    return results


def verify_buckets(settings: Settings) -> list[GateResult]:
    assert_policy_covers(settings)
    s3 = _client(settings)
    results = [verify_versioning(s3, settings.raw_bucket),
               verify_versioning(s3, settings.main_bucket)]
    results.extend(verify_lock_inheritance(s3, settings.raw_bucket))
    results.extend(verify_delete_deny(s3, settings.raw_bucket, LOCK_INHERIT_CHECK_KEY))
    return results
```

- [ ] **Step 3: Write the CLI wrapper**

`src/bls_stats/cli/app.py`:

```python
"""The typer app. Command groups mirror the planes (spec 15)."""

from __future__ import annotations

import typer

from bls_stats.cli import admin

app = typer.Typer(no_args_is_help=True, add_completion=False)
app.add_typer(admin.app, name="admin")
```

`src/bls_stats/cli/admin.py`:

```python
from __future__ import annotations

import typer

from bls_stats.cli.exit_codes import ExitCode
from bls_stats.config.settings import load_settings
from bls_stats.objstore import buckets

app = typer.Typer(no_args_is_help=True)
buckets_app = typer.Typer(no_args_is_help=True)
app.add_typer(buckets_app, name="buckets")


def _report(results: list[buckets.GateResult]) -> None:
    for r in results:
        typer.echo(f"{'ok  ' if r.ok else 'FAIL'} {r.name}: {r.detail}")
    if not all(r.ok for r in results):
        typer.echo("GATE FAILED - first capture must not proceed "
                   "(findings section 7, step 4)", err=True)
        raise typer.Exit(ExitCode.UNEXPECTED_ERROR)


@buckets_app.command("create")
def create(apply: bool = typer.Option(False, "--apply", help="actually create")) -> None:
    _report(buckets.create_buckets(load_settings(), apply=apply))


@buckets_app.command("verify")
def verify() -> None:
    _report(buckets.verify_buckets(load_settings()))
```

`src/bls_stats/cli/exit_codes.py`:

```python
"""Spec 15's exit codes, because cron is a consumer. All six from the start;
Stage 2 can emit SUCCESS, UNEXPECTED_ERROR and DEGRADED."""

from __future__ import annotations

from enum import IntEnum


class ExitCode(IntEnum):
    SUCCESS = 0
    UNEXPECTED_ERROR = 1
    ASSERT_FAILURE = 10
    LATE_OR_TRIAGE = 20
    DEGRADED = 30
    LEASE_UNAVAILABLE = 40
```

- [ ] **Step 4: Write the failing test**

`tests/objstore/test_buckets.py` — the sheet's *content* is testable without an endpoint; the gates
are not, so they run under `real_store`.

```python
from __future__ import annotations

import json

import pytest

from bls_stats.objstore.buckets import (
    DELETE_DENY_POLICY,
    LOCK_INHERIT_CHECK_KEY,
    RETENTION_DAYS,
    RETENTION_MODE,
)


def test_policy_is_the_findings_json_verbatim() -> None:
    doc = json.loads(DELETE_DENY_POLICY)
    stmt = doc["Statement"][0]
    assert stmt["Effect"] == "Deny"
    assert stmt["Principal"] == {"AWS": ["*"]}
    assert stmt["Action"] == ["s3:DeleteObject", "s3:DeleteObjectVersion"]
    assert stmt["Resource"] == ["arn:aws:s3:::bls-stats-raw/*"]


def test_retention_carries_a_mode_and_a_duration() -> None:
    assert (RETENTION_MODE, RETENTION_DAYS) == ("GOVERNANCE", 3650)


def test_policy_guard_stops_on_a_renamed_bucket() -> None:
    """BLS_RAW_BUCKET is configurable; the pinned policy ARN is not. A
    mismatch would attach delete-deny to a bucket it does not cover."""
    from bls_stats.config.settings import Settings
    from bls_stats.objstore.buckets import assert_policy_covers

    def _settings(bucket: str) -> Settings:
        return Settings(_env_file=None, aws_endpoint_url="http://x:9000",
                        aws_access_key_id="k", aws_secret_access_key="s",
                        bls_contact_email="t@example.invalid", raw_bucket=bucket)

    assert_policy_covers(_settings("bls-stats-raw"))
    with pytest.raises(RuntimeError, match="delete-deny policy"):
        assert_policy_covers(_settings("something-else"))


def test_gate_object_is_outside_the_content_addressed_namespace() -> None:
    """N11 re-hashes raw/ blobs against their content-addressed keys; a
    marker keyed by an arbitrary string there would never match."""
    assert not LOCK_INHERIT_CHECK_KEY.startswith("raw/blob/")
    assert LOCK_INHERIT_CHECK_KEY.startswith("attest/")
```

Run: `uv run pytest tests/objstore/test_buckets.py -v` — expected FAIL (`ModuleNotFoundError`),
then PASS after Step 2.

- [ ] **Step 5: Dry-run, then execute the sheet against the dev endpoint**

```bash
set -a; source .project.env; set +a
uv run bls-stats admin buckets create
uv run bls-stats admin buckets create --apply
```

Expected: every gate line prints `ok`. **A `FAIL` line is a stop, not a warning** — do not proceed
to Task 8. Remediation branches are in findings §7 ("If the gate fails"); follow them there rather
than improvising, noting that the delete-deny policy is already attached by then and must be
removed or suspended before any teardown delete, and that removing a bucket requires deleting
**every version and every delete marker**, not just the object.

⚠ Step 2's marker object is written under real GOVERNANCE/3650-day retention **at whichever
endpoint runs this**, including dev. It is immutable there for ten years absent an admin
governance-bypass delete (which itself needs the bucket policy detached first). That is the
intended cost of proving inheritance; do not "clean it up".

- [ ] **Step 6: Re-run the delete-deny gate under the runtime credential**

The gate in Step 5 ran under whichever credential `.project.env` names. Re-run it explicitly under
the **non-admin runtime credential** from Step 1 — that is the credential §17.4's hard requirement
is about, and the only evidence on record today is NOT ENFORCED for root:

```bash
AWS_ACCESS_KEY_ID=bls-stats-runtime AWS_SECRET_ACCESS_KEY=... \
  uv run bls-stats admin buckets verify
```

Expected: `ok delete_deny.marker` **and** `ok delete_deny.version`. Both must pass — deferred item 7
exists because Stage 1 only exercised the first. **If delete-deny is not enforced for the runtime
credential, that is a stop, not a fallback** (§17.4: "Do not run without it").

- [ ] **Step 7: Run the same sheet against the deployment endpoint**

Requires operator input 1. The script is endpoint-agnostic by construction — only
`AWS_ENDPOINT_URL` and the credentials change:

```bash
AWS_ENDPOINT_URL=... AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
  uv run bls-stats admin buckets create --apply
```

Record the resulting gate output in `specs/bls-stats-stage1-findings.md` §5's blocked
`workstation-deploy` column — that closes deferred item 1's first half. If input 1 has not arrived,
**stop and ask**; do not substitute the dev result (§1.4 anticipates the two endpoints differing,
and this is precisely the promotion it warns against).

- [ ] **Step 8: Commit**

```bash
git add src/bls_stats/objstore/buckets.py src/bls_stats/cli tests/objstore/test_buckets.py
git commit -m "Create the archive buckets from the Stage-1 parameter sheet"
```

---

### Task 4: Transport profiles, pacing, retry, and the dual-hash streaming GET

**Files:**
- Create: `src/bls_stats/transport/__init__.py`, `src/bls_stats/transport/profiles.py`,
  `src/bls_stats/transport/client.py`
- Test: `tests/transport/test_profiles.py`, `tests/transport/test_client.py`

**Interfaces:**
- Consumes: `Settings` (for `bls_contact_email`).
- Produces (used by Tasks 6, 8):
  - `Profile` — frozen dataclass `name`, `headers: Mapping[str, str]`, `max_connections: int`,
    `min_interval_s: float`, `change_check: Literal["head", "conditional_get"]`
  - `build_profiles(contact_email: str) -> dict[str, Profile]` with keys
    `"flatfile"`, `"html"`, `"api"`
  - `contact_user_agent(email: str) -> str`
  - `Probe` — frozen dataclass: `status_code: int | None`, `request_headers: dict[str, str]`,
    `response_headers: dict[str, str]`, `etag: str | None`, `http_last_modified: str | None`,
    `content_length: int | None`, `content_encoding: str | None`, `duration_ms: int`,
    `error_class: str | None`
  - `Transfer` — `Probe` fields plus `wire_sha256: str | None`, `content_sha256: str | None`,
    `byte_size: int | None`, `spool: Path | None`
  - `Transport(profiles: Mapping[str, Profile], *, min_interval_override=None, timeout=120.0,
    max_attempts=3)` with `head(url, profile_name) -> Probe`,
    `stream_get(url, profile_name, *, conditional: Mapping[str, str] | None = None) -> Transfer`,
    `profile(name) -> Profile`, and context-manager `close()`

- [ ] **Step 1: Write the failing tests**

`tests/transport/test_profiles.py`:

```python
from __future__ import annotations

from bls_stats.transport.profiles import build_profiles, contact_user_agent


def test_contact_ua_is_descriptive_and_contactable() -> None:
    ua = contact_user_agent("ops@example.invalid")
    assert ua.startswith("bls-stats/")
    assert "ops@example.invalid" in ua


def test_flatfile_pins_accept_encoding() -> None:
    """Spec 7.3: pinned so archived bytes are reproducible across
    implementations and across time."""
    assert build_profiles("o@e.invalid")["flatfile"].headers["Accept-Encoding"] == "gzip"


def test_html_uses_the_contact_profile_not_browser_shaped_headers() -> None:
    """Stage-1 findings section 3: browser-shaped headers were blocked 6/6 on
    www.bls.gov; the contact profile passed 6/6."""
    html = build_profiles("o@e.invalid")["html"]
    assert html.headers["User-Agent"].startswith("bls-stats/")
    assert "Mozilla" not in html.headers["User-Agent"]
    assert "Accept-Language" not in html.headers
    assert "Upgrade-Insecure-Requests" not in html.headers


def test_html_uses_conditional_get_not_head() -> None:
    """Spec 6.1: where HEAD is unreliable, use conditional GET."""
    profiles = build_profiles("o@e.invalid")
    assert profiles["flatfile"].change_check == "head"
    assert profiles["html"].change_check == "conditional_get"


def test_flatfile_concurrency_is_conservative() -> None:
    assert build_profiles("o@e.invalid")["flatfile"].max_connections <= 4
```

`tests/transport/test_client.py`:

```python
from __future__ import annotations

import gzip
import hashlib
import resource

import httpx
import pytest
import respx

from bls_stats.transport.client import Transport
from bls_stats.transport.profiles import build_profiles

URL = "https://download.bls.gov/pub/time.series/ce/ce.period"


@pytest.fixture
def transport() -> Transport:
    return Transport(build_profiles("t@example.invalid"), min_interval_override=0.0)


@respx.mock
def test_head_records_validators(transport: Transport) -> None:
    respx.head(URL).mock(return_value=httpx.Response(
        200, headers={"Last-Modified": "Fri, 07 Aug 2026 12:30:00 GMT",
                      "ETag": '"094a5766826dd1:0"', "Content-Length": "419"}))
    probe = transport.head(URL, "flatfile")
    assert probe.status_code == 200
    assert probe.etag == '"094a5766826dd1:0"'
    assert probe.http_last_modified == "Fri, 07 Aug 2026 12:30:00 GMT"
    assert probe.request_headers["user-agent"].startswith("bls-stats/")


@respx.mock
def test_two_hashes_differ_under_gzip(transport: Transport) -> None:
    """Spec 7.3: wire_sha256 over the bytes off the socket keys the blob;
    content_sha256 over the transfer-decoded bytes is the change signal. If
    change detection keyed on the wire hash, a CDN re-compressing at a
    different level would manufacture a vintage that is permanent in an
    append-only store."""
    plain = b"series\tvalue\n" * 100
    wire = gzip.compress(plain)
    respx.get(URL).mock(return_value=httpx.Response(
        200, content=wire,
        headers={"Content-Encoding": "gzip", "Content-Length": str(len(wire))}))
    t = transport.stream_get(URL, "flatfile")
    assert t.wire_sha256 == hashlib.sha256(wire).hexdigest()
    assert t.content_sha256 == hashlib.sha256(plain).hexdigest()
    assert t.wire_sha256 != t.content_sha256
    assert t.byte_size == len(wire)
    assert t.spool is not None and t.spool.read_bytes() == wire, "archive the WIRE bytes"


@respx.mock
def test_identity_encoding_makes_the_hashes_equal(transport: Transport) -> None:
    body = b"no encoding here\n"
    respx.get(URL).mock(return_value=httpx.Response(200, content=body))
    t = transport.stream_get(URL, "flatfile")
    assert t.wire_sha256 == t.content_sha256 == hashlib.sha256(body).hexdigest()


@respx.mock
def test_unknown_content_encoding_raises_rather_than_guessing(transport: Transport) -> None:
    respx.get(URL).mock(return_value=httpx.Response(
        200, content=b"...", headers={"Content-Encoding": "zstd"}))
    with pytest.raises(ValueError, match="zstd"):
        transport.stream_get(URL, "flatfile")


@respx.mock
def test_403_is_retried_then_reported_not_raised(transport: Transport) -> None:
    """Spec 7.1: no retry on 4xx except 403/429."""
    route = respx.get(URL).mock(return_value=httpx.Response(403, content=b"Access Denied"))
    t = transport.stream_get(URL, "html")
    assert t.status_code == 403
    assert route.call_count > 1


@respx.mock
def test_404_is_not_retried(transport: Transport) -> None:
    route = respx.get(URL).mock(return_value=httpx.Response(404))
    t = transport.stream_get(URL, "flatfile")
    assert t.status_code == 404
    assert route.call_count == 1


@respx.mock
def test_conditional_get_304_carries_no_body(transport: Transport) -> None:
    respx.get(URL).mock(return_value=httpx.Response(304))
    t = transport.stream_get(URL, "html", conditional={"If-None-Match": '"abc"'})
    assert t.status_code == 304
    assert t.wire_sha256 is None and t.spool is None


@respx.mock
def test_timeout_is_recorded_as_an_error_class(transport: Transport) -> None:
    respx.get(URL).mock(side_effect=httpx.ConnectTimeout("boom"))
    t = transport.stream_get(URL, "flatfile")
    assert t.status_code is None
    assert t.error_class == "ConnectTimeout"


@respx.mock
def test_large_body_never_materializes(transport: Transport) -> None:
    """Spec 1.4: peak RSS < 8 GB, streaming mandatory. ru_maxrss is a
    high-water mark, so a delta across the call detects a full read into
    memory even though it never falls."""
    chunk = b"x" * (1 << 20)
    total = 512

    def _stream() -> httpx.SyncByteStream:
        class S(httpx.SyncByteStream):
            def __iter__(self):  # type: ignore[no-untyped-def]
                for _ in range(total):
                    yield chunk
        return S()

    respx.get(URL).mock(return_value=httpx.Response(200, stream=_stream()))
    before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    t = transport.stream_get(URL, "flatfile")
    after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    scale = 1 if __import__("sys").platform == "darwin" else 1024  # macOS bytes, Linux KiB
    grew_mib = (after - before) * scale / (1 << 20)
    assert t.byte_size == total * len(chunk)
    assert grew_mib < 128, f"streaming GET grew RSS by {grew_mib:.0f} MiB for a 512 MiB body"
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest tests/transport -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'bls_stats.transport'`.

- [ ] **Step 3: Write `src/bls_stats/transport/profiles.py`**

```python
"""Spec 7.1's three transport profiles, as data.

The `html` row reflects the amended spec (2026-08-11) and Stage-1 findings
section 3: the browser-shaped profile was blocked 6/6 on www.bls.gov by a
uniform Akamai block page, while these same contact headers returned genuine
content on all six surfaces. Do not "improve" this by adding browser-shaped
headers.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

ChangeCheck = Literal["head", "conditional_get"]


@dataclass(frozen=True)
class Profile:
    name: str
    headers: Mapping[str, str]
    max_connections: int
    min_interval_s: float
    change_check: ChangeCheck


def contact_user_agent(email: str) -> str:
    """Mandatory from the first request, not a courtesy (spec 7.1, R12): the
    flat-file host 403s bare user agents and BLS policy permits real-time
    blocking of non-compliant robots."""
    return f"bls-stats/0.1 (data-archival research; contact: {email})"


def build_profiles(contact_email: str) -> dict[str, Profile]:
    contact = {
        "User-Agent": contact_user_agent(contact_email),
        "Accept": "*/*",
        # Pinned so archived bytes are reproducible across implementations
        # and across time (spec 7.3).
        "Accept-Encoding": "gzip",
    }
    return {
        "flatfile": Profile("flatfile", contact, max_connections=4,
                            min_interval_s=2.0, change_check="head"),
        # HEAD is unreliable on the HTML surfaces, so change detection is a
        # conditional GET (spec 6.1). Slower rate: these are pages, not bulk.
        "html": Profile("html", contact, max_connections=1,
                        min_interval_s=5.0, change_check="conditional_get"),
        # Configured, not called, in Stage 2 -- spec 1.3 makes the API
        # spot-check-only. Whichever stage first calls it inherits 7.1's
        # rule: never trust HTTP 200 or REQUEST_SUCCEEDED; inspect the
        # message array and per-series data presence.
        "api": Profile("api", contact | {"Accept": "application/json"}, max_connections=1,
                       min_interval_s=10.0, change_check="head"),
    }
```

- [ ] **Step 4: Write `src/bls_stats/transport/client.py`**

```python
"""httpx clients, pacing, retry, and the dual-hash streaming GET.

The one subtle piece is the hashing. httpx transparently decodes
Content-Encoding, but P1 archives the bytes EXACTLY as received and spec 7.3
needs both hashes -- so this reads the raw stream and runs its own
decompressor alongside, computing both digests in a single pass without ever
holding the body in memory.
"""

from __future__ import annotations

import hashlib
import tempfile
import threading
import time
import zlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import httpx
from tenacity import (
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from bls_stats.transport.profiles import Profile

RETRYABLE_STATUS = frozenset({403, 429})  # plus every 5xx; spec 7.1
CHUNK = 1 << 20


@dataclass(frozen=True)
class Probe:
    status_code: int | None
    request_headers: dict[str, str]
    response_headers: dict[str, str]
    etag: str | None
    http_last_modified: str | None
    content_length: int | None
    content_encoding: str | None
    duration_ms: int
    error_class: str | None


@dataclass(frozen=True)
class Transfer:
    status_code: int | None
    request_headers: dict[str, str]
    response_headers: dict[str, str]
    etag: str | None
    http_last_modified: str | None
    content_length: int | None
    content_encoding: str | None
    duration_ms: int
    error_class: str | None
    wire_sha256: str | None
    content_sha256: str | None
    byte_size: int | None
    spool: Path | None


class _Decoder(Protocol):
    def decompress(self, data: bytes) -> bytes: ...
    def flush(self) -> bytes: ...


class _Identity:
    def decompress(self, data: bytes) -> bytes:
        return data

    def flush(self) -> bytes:
        return b""


def _decoder_for(content_encoding: str) -> _Decoder:
    enc = content_encoding.strip().lower()
    if enc in {"", "identity"}:
        return _Identity()
    if enc == "gzip":
        return zlib.decompressobj(16 + zlib.MAX_WBITS)
    if enc == "deflate":
        return zlib.decompressobj()
    # Loud, not silent: an unrecognised encoding would make content_sha256
    # a hash of compressed bytes, and 6.1 keys change detection on it.
    raise ValueError(f"unsupported Content-Encoding: {content_encoding!r}")


class _RetryableStatus(Exception):
    """A response whose status says 'try again' (spec 7.1: 5xx, 429, 403).

    Carries the status and both header blocks rather than the response
    object, because the response is CLOSED before this is raised -- a
    streamed response left open on every retry leaks a connection, and the
    error page's body is not something any caller reads.
    """

    def __init__(self, status_code: int, request_headers: dict[str, str],
                 response_headers: dict[str, str]) -> None:
        super().__init__(f"retryable status {status_code}")
        self.status_code = status_code
        self.request_headers = request_headers
        self.response_headers = response_headers


class _Pacer:
    """Per-host token bucket, degenerate case: a minimum interval between
    requests. Sequential by construction in Stage 2; max_connections is the
    ceiling a later concurrent sweep must respect (spec 7.1, <=4)."""

    def __init__(self, min_interval_s: float) -> None:
        self._min = min_interval_s
        self._next = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            if now < self._next:
                time.sleep(self._next - now)
            self._next = max(now, self._next) + self._min


def _headers(obj: httpx.Headers) -> dict[str, str]:
    """Complete header block, verbatim (R10) -- not a curated subset."""
    return {k.lower(): v for k, v in obj.items()}


class Transport:
    def __init__(self, profiles: Mapping[str, Profile], *,
                 min_interval_override: float | None = None,
                 timeout: float = 120.0, max_attempts: int = 3) -> None:
        self._profiles = dict(profiles)
        self._max_attempts = max_attempts
        self._clients: dict[str, httpx.Client] = {}
        self._pacers: dict[str, _Pacer] = {}
        for name, profile in profiles.items():
            interval = (min_interval_override if min_interval_override is not None
                        else profile.min_interval_s)
            self._pacers[name] = _Pacer(interval)
            self._clients[name] = httpx.Client(
                http2=True,
                headers=dict(profile.headers),
                timeout=timeout,
                follow_redirects=True,
                limits=httpx.Limits(max_connections=profile.max_connections),
            )

    def profile(self, name: str) -> Profile:
        return self._profiles[name]

    def close(self) -> None:
        for client in self._clients.values():
            client.close()

    def __enter__(self) -> "Transport":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- HEAD -------------------------------------------------------------

    def head(self, url: str, profile: str) -> Probe:
        client = self._clients[profile]
        started = time.monotonic()
        try:
            response = self._with_retry(profile, lambda: client.send(
                client.build_request("HEAD", url)))
        except _RetryableStatus as exhausted:
            return Probe(exhausted.status_code, exhausted.request_headers,
                         exhausted.response_headers,
                         exhausted.response_headers.get("etag"),
                         exhausted.response_headers.get("last-modified"),
                         _int_or_none(exhausted.response_headers.get("content-length")),
                         exhausted.response_headers.get("content-encoding"),
                         _ms(started), None)
        except httpx.HTTPError as exc:
            return Probe(None, dict(client.headers), {}, None, None, None, None,
                         _ms(started), type(exc).__name__)
        return Probe(
            status_code=response.status_code,
            request_headers=_headers(response.request.headers),
            response_headers=_headers(response.headers),
            etag=response.headers.get("etag"),
            http_last_modified=response.headers.get("last-modified"),
            content_length=_int_or_none(response.headers.get("content-length")),
            content_encoding=response.headers.get("content-encoding"),
            duration_ms=_ms(started),
            error_class=None,
        )

    # -- GET --------------------------------------------------------------

    def stream_get(self, url: str, profile: str, *,
                   conditional: Mapping[str, str] | None = None) -> Transfer:
        client = self._clients[profile]
        started = time.monotonic()
        headers = dict(conditional or {})
        try:
            response = self._with_retry(profile, lambda: client.send(
                client.build_request("GET", url, headers=headers), stream=True))
        except _RetryableStatus as exhausted:
            # Retries exhausted on a 403/429/5xx. The response is already
            # closed; its status and headers are what the record needs.
            return Transfer(exhausted.status_code, exhausted.request_headers,
                            exhausted.response_headers,
                            exhausted.response_headers.get("etag"),
                            exhausted.response_headers.get("last-modified"),
                            _int_or_none(exhausted.response_headers.get("content-length")),
                            exhausted.response_headers.get("content-encoding"),
                            _ms(started), None, None, None, None, None)
        except httpx.HTTPError as exc:
            return _failed_transfer(dict(client.headers), started, type(exc).__name__)

        try:
            if response.status_code == 304 or response.status_code >= 400:
                response.close()  # 304 has no body; a non-retryable 4xx page is not read
                return _bodiless_transfer(response, started)
            encoding = response.headers.get("content-encoding", "")
            decoder = _decoder_for(encoding)
            wire, content = hashlib.sha256(), hashlib.sha256()
            size = 0
            # Scratch under $TMPDIR, deleted by the caller after the blob PUT
            # (spec 17.2). The blob key is the wire hash, unknown until the
            # stream ends, so the body is spooled to disk -- never to memory.
            with tempfile.NamedTemporaryFile(delete=False, suffix=".blob") as spool:
                for chunk in response.iter_raw(CHUNK):
                    wire.update(chunk)
                    content.update(decoder.decompress(chunk))
                    spool.write(chunk)
                    size += len(chunk)
                content.update(decoder.flush())
                path = Path(spool.name)
            return Transfer(
                status_code=response.status_code,
                request_headers=_headers(response.request.headers),
                response_headers=_headers(response.headers),
                etag=response.headers.get("etag"),
                http_last_modified=response.headers.get("last-modified"),
                content_length=_int_or_none(response.headers.get("content-length")),
                content_encoding=encoding or None,
                duration_ms=_ms(started),
                error_class=None,
                wire_sha256=wire.hexdigest(),
                content_sha256=content.hexdigest(),
                byte_size=size,
                spool=path,
            )
        finally:
            response.close()

    # -- retry ------------------------------------------------------------

    def _with_retry(self, profile: str, call: Callable[[], httpx.Response]) -> httpx.Response:
        """tenacity retry with jitter on 5xx/429/timeout, no retry on 4xx
        except 403/429 (spec 7.1). `reraise=True` so the caller sees either
        the underlying httpx error or the carried status, never a
        tenacity-shaped wrapper."""
        pacer = self._pacers[profile]

        def _once() -> httpx.Response:
            pacer.wait()
            response = call()
            if response.status_code in RETRYABLE_STATUS or response.status_code >= 500:
                status, req, res = (response.status_code,
                                    _headers(response.request.headers),
                                    _headers(response.headers))
                response.close()  # never leave a discarded stream open
                raise _RetryableStatus(status, req, res)
            return response

        retrying = Retrying(
            stop=stop_after_attempt(self._max_attempts),
            wait=wait_exponential_jitter(initial=1, max=30),
            retry=retry_if_exception_type((_RetryableStatus, httpx.TimeoutException,
                                           httpx.NetworkError)),
            reraise=True,
        )
        return retrying(_once)


def _ms(started: float) -> int:
    return round((time.monotonic() - started) * 1000)


def _int_or_none(value: str | None) -> int | None:
    return int(value) if value is not None and value.isdigit() else None


def _failed_transfer(request_headers: dict[str, str], started: float,
                     error_class: str) -> Transfer:
    return Transfer(None, request_headers, {}, None, None, None, None,
                    _ms(started), error_class, None, None, None, None)


def _bodiless_transfer(response: httpx.Response, started: float) -> Transfer:
    return Transfer(
        status_code=response.status_code,
        request_headers=_headers(response.request.headers),
        response_headers=_headers(response.headers),
        etag=response.headers.get("etag"),
        http_last_modified=response.headers.get("last-modified"),
        content_length=_int_or_none(response.headers.get("content-length")),
        content_encoding=response.headers.get("content-encoding"),
        duration_ms=_ms(started),
        error_class=None,
        wire_sha256=None, content_sha256=None, byte_size=None, spool=None,
    )
```

> Ranged / resumable transfers are deliberately **not** built. Findings §8 recommends omitting
> `Accept-Encoding: gzip` on range-based bulk transfers, which conflicts with §7.3's requirement
> that `flatfile` pin it — and since `wire_sha256` is the blob key, the same content would land
> under two different keys depending on which path fetched it. No Stage-2 Exit criterion needs
> resumability. Record as a deferred item with that reasoning, alongside the unresolved
> 350,208,884-vs-47,300,620 `Content-Length` divergence on an identical ETag.

- [ ] **Step 5: Run the tests to verify they pass**

```bash
uv run pytest tests/transport -v
```

Expected: 14 passed.

- [ ] **Step 6: Commit**

```bash
git add src/bls_stats/transport tests/transport
git commit -m "Add transport profiles and the dual-hash streaming GET"
```

---

### Task 5: The `fetch_log` record and its writer

**Files:**
- Create: `src/bls_stats/capture/__init__.py`, `src/bls_stats/capture/fetchlog.py`
- Test: `tests/capture/test_fetchlog.py`

**Interfaces:**
- Consumes: `ObjectStore` (Task 2), `Probe` / `Transfer` (Task 4).
- Produces (used by Tasks 7, 8, 9, and by Stage 3's materializer):
  - `Outcome` — `StrEnum` with `UNCHANGED`, `RESTAMP_ONLY`, `NEW_BYTES`, `ERROR`, `BLOCKED`
  - `FetchLogRecord` — frozen dataclass carrying **exactly** §7.3's field list
  - `new_run_id() -> str` — `<YYYYMMDDTHHMMSSZ>-<8 hex>`, sorts chronologically
  - `FetchLogWriter(store, run_id)` with `.append(record) -> None` (flushes immediately) and
    `.records_written: int`
  - `record_from_probe(...)` / `record_from_transfer(...)` builders

- [ ] **Step 1: Write the failing test**

`tests/capture/test_fetchlog.py`:

```python
from __future__ import annotations

import json
from dataclasses import fields

from bls_stats.capture.fetchlog import (
    FetchLogRecord,
    FetchLogWriter,
    Outcome,
    new_run_id,
)

SPEC_FIELDS = [
    "request_id", "url", "method", "profile", "requested_at", "status_code",
    "http_last_modified", "etag", "content_length", "content_encoding",
    "request_headers", "response_headers",
    "wire_sha256", "content_sha256", "blob_key", "byte_size",
    "outcome", "error_class", "duration_ms", "slot_id",
]


def test_record_carries_exactly_the_spec_field_list() -> None:
    """Spec 7.3. The writer materializes file_vintage from this log and
    nothing else, so a missing field is a missing column downstream."""
    assert [f.name for f in fields(FetchLogRecord)] == SPEC_FIELDS


def test_outcomes_are_the_spec_enum() -> None:
    assert {o.value for o in Outcome} == {
        "unchanged", "restamp_only", "new_bytes", "error", "blocked"}


def test_run_ids_are_unique_and_timestamp_prefixed() -> None:
    """Timestamp first so keys sort chronologically inside a partition;
    uuid suffix so two runs starting in the same second never collide."""
    ids = {new_run_id() for _ in range(50)}
    assert len(ids) == 50
    stamp, _, suffix = next(iter(ids)).partition("-")
    assert len(stamp) == 16 and stamp[8] == "T" and stamp.endswith("Z")
    assert len(suffix) == 8


def test_writer_flushes_one_object_per_record(fake_store) -> None:
    """One object per flush, never an append in place -- object storage has
    no append, and unique keys are what keep concurrent lock-free capture
    processes from contending (spec 7.3)."""
    writer = FetchLogWriter(fake_store, run_id="20260811T120000Z-abcd1234")
    for i in range(3):
        writer.append(_record(f"https://example.invalid/{i}"))
    keys = sorted(fake_store.written)
    assert len(keys) == 3
    assert all(k.startswith("log/fetch/dt=") for k in keys)
    assert all(k.endswith(".jsonl") for k in keys)
    assert len(set(keys)) == 3, "keys must be unique"
    body = fake_store.written[keys[0]].decode()
    assert json.loads(body.strip())["outcome"] == "new_bytes"


def _record(url: str) -> FetchLogRecord:
    return FetchLogRecord(
        request_id="r1", url=url, method="GET", profile="flatfile",
        requested_at="2026-08-11T12:00:00+00:00", status_code=200,
        http_last_modified=None, etag=None, content_length=1, content_encoding=None,
        request_headers={}, response_headers={}, wire_sha256="a" * 64,
        content_sha256="b" * 64, blob_key="raw/blob/sha256=aa/aa/" + "a" * 64,
        byte_size=1, outcome=Outcome.NEW_BYTES, error_class=None, duration_ms=5,
        slot_id=None)
```

Add to `tests/conftest.py`:

```python
@pytest.fixture
def fake_store():  # type: ignore[no-untyped-def]
    """An in-memory stand-in for ObjectStore. Legitimate here because these
    tests exercise OUR serialization, not the endpoint's option handling --
    the latter is what tests/objstore/test_real_store.py is for (spec 16.1)."""

    class _Fake:
        def __init__(self) -> None:
            self.written: dict[str, bytes] = {}

        def put_atomic(self, key: str, body: bytes) -> None:
            self.written[key] = body

        def append_jsonl(self, key, records) -> None:  # type: ignore[no-untyped-def]
            import json
            self.written[key] = b"".join(
                json.dumps(r, separators=(",", ":"), default=str).encode() + b"\n"
                for r in records)

        def exists(self, key: str) -> bool:
            return key in self.written

        def list(self, prefix: str):  # type: ignore[no-untyped-def]
            return (k for k in sorted(self.written) if k.startswith(prefix))

        def open_stream(self, key: str, chunk_size: int = 1 << 20):  # type: ignore[no-untyped-def]
            if key not in self.written:
                return None
            return iter([self.written[key]])

        def put_stream(self, key: str, fileobj) -> None:  # type: ignore[no-untyped-def]
            self.written[key] = fileobj.read()

    return _Fake()
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest tests/capture/test_fetchlog.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'bls_stats.capture'`.

- [ ] **Step 3: Write `src/bls_stats/capture/fetchlog.py`**

```python
"""The fetch log (spec 7.3): the forensic record, and half of P2's
durability boundary.

When a vintage looks wrong six months later, "what exactly did we ask for
and what exactly came back" must be answerable -- which is why both header
blocks are stored verbatim rather than as a curated subset (R10).
"""

from __future__ import annotations

import datetime as dt
import uuid
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any, Protocol

from bls_stats.transport.client import Probe, Transfer


class Outcome(StrEnum):
    UNCHANGED = "unchanged"
    RESTAMP_ONLY = "restamp_only"
    NEW_BYTES = "new_bytes"
    ERROR = "error"
    BLOCKED = "blocked"


@dataclass(frozen=True)
class FetchLogRecord:
    """Exactly spec 7.3's field list, in its order. It carries every field
    file_vintage needs, because the Stage-3 writer materializes that row
    from this log and nothing else. `program` and `artifact_key` are NOT
    here -- 7.3's list omits them; Stage 3 resolves url -> (program,
    artifact_key) through src/bls_stats/config/artifacts.toml."""

    request_id: str
    url: str
    method: str
    profile: str
    requested_at: str
    status_code: int | None
    http_last_modified: str | None
    etag: str | None
    content_length: int | None
    content_encoding: str | None
    request_headers: Mapping[str, str]
    response_headers: Mapping[str, str]
    wire_sha256: str | None
    content_sha256: str | None
    blob_key: str | None
    byte_size: int | None
    outcome: Outcome
    error_class: str | None
    duration_ms: int
    slot_id: str | None


class _Store(Protocol):
    def append_jsonl(self, key: str, records: Any) -> None: ...


def new_run_id() -> str:
    """Timestamp-first so object keys sort chronologically inside a
    partition; uuid suffix so two runs starting in the same second never
    collide."""
    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{uuid.uuid4().hex[:8]}"


def new_request_id() -> str:
    return uuid.uuid4().hex


def utcnow_iso() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds")


class FetchLogWriter:
    """One object per record under log/fetch/dt=<date>/<run_id>-<seq>.jsonl.

    Deviation from 7.3's literal `<run_id>.jsonl`, recorded deliberately:
    the stated constraint is "never an append in place", whose stated
    purpose is that concurrent lock-free capture processes never touch the
    same key -- which unique keys satisfy however many a run writes.
    Flushing per record instead of per run means a crash mid-sweep loses no
    record for an artifact already captured, and the log is half the
    durability boundary.
    """

    def __init__(self, store: _Store, run_id: str) -> None:
        self._store = store
        self._run_id = run_id
        self._seq = 0
        self.records_written = 0

    def append(self, record: FetchLogRecord) -> None:
        day = record.requested_at[:10]
        key = f"log/fetch/dt={day}/{self._run_id}-{self._seq:04d}.jsonl"
        self._store.append_jsonl(key, [asdict(record)])
        self._seq += 1
        self.records_written += 1


def record_from_probe(probe: Probe, *, url: str, profile: str, outcome: Outcome,
                      requested_at: str, slot_id: str | None = None) -> FetchLogRecord:
    return FetchLogRecord(
        request_id=new_request_id(), url=url, method="HEAD", profile=profile,
        requested_at=requested_at, status_code=probe.status_code,
        http_last_modified=probe.http_last_modified, etag=probe.etag,
        content_length=probe.content_length, content_encoding=probe.content_encoding,
        request_headers=probe.request_headers, response_headers=probe.response_headers,
        wire_sha256=None, content_sha256=None, blob_key=None, byte_size=None,
        outcome=outcome, error_class=probe.error_class, duration_ms=probe.duration_ms,
        slot_id=slot_id)


def record_from_transfer(transfer: Transfer, *, url: str, profile: str, outcome: Outcome,
                         requested_at: str, blob_key: str | None,
                         slot_id: str | None = None) -> FetchLogRecord:
    return FetchLogRecord(
        request_id=new_request_id(), url=url, method="GET", profile=profile,
        requested_at=requested_at, status_code=transfer.status_code,
        http_last_modified=transfer.http_last_modified, etag=transfer.etag,
        content_length=transfer.content_length, content_encoding=transfer.content_encoding,
        request_headers=transfer.request_headers, response_headers=transfer.response_headers,
        wire_sha256=transfer.wire_sha256, content_sha256=transfer.content_sha256,
        blob_key=blob_key, byte_size=transfer.byte_size, outcome=outcome,
        error_class=transfer.error_class, duration_ms=transfer.duration_ms, slot_id=slot_id)
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
uv run pytest tests/capture/test_fetchlog.py -v
```

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add src/bls_stats/capture tests/capture tests/conftest.py
git commit -m "Add the fetch-log record and its per-record writer"
```

---

### Task 6: The artifact inventory

**Files:**
- Create: `src/bls_stats/config/artifacts.toml`
- Create: `src/bls_stats/capture/inventory.py`, `src/bls_stats/capture/discover.py`
- Test: `tests/capture/test_inventory.py`, `tests/capture/test_discover.py`,
  `tests/capture/test_inventory_network.py`
- Test fixture: `tests/fixtures/labstat_ce_listing.html`

**Interfaces:**
- Consumes: `Transport` (Task 4) for discovery and the network-marked reachability check.
- Produces (used by Tasks 8, 9, 10 and by Stage 3's `url → (program, artifact_key)` resolution):
  - `Artifact` — frozen dataclass `key: str`, `programs: tuple[str, ...]`, `url: str`,
    `profile: str`, `kind: Literal["data", "mapping", "metadata"]`
  - `load_inventory(path: Path | None = None) -> tuple[Artifact, ...]`
  - `default_path() -> Path` — package data, overridable via `BLS_ARTIFACTS_PATH`
  - `by_url(inventory) -> dict[str, Artifact]`
  - `parse_listing(html: str, prefix: str) -> tuple[str, ...]` (in `discover.py`)
  - `parse_feed_links(html: str) -> tuple[str, ...]` (in `discover.py`)

**Selection rule, stated once so the inventory is reviewable rather than arbitrary:**

1. **One universe data file per program** — the `AllItems` / `AllData` / `AllCESSeries` variant
   where BLS ships one, otherwise `Current`. Never both a superset and its subsets: capturing
   `jt.data.1.AllItems` *and* `jt.data.2.JobOpenings` archives the same cells twice under two
   change signals.
2. **Every non-`.data.` file in the prefix** — `.series`, the mapping files, `.txt`, `.release`.
   They are small, they change rarely, and **P6 makes metadata vintage data**: a mapping file that
   silently gains a code is a real event the store must be able to date.
3. **The `www.bls.gov` metadata surfaces** — the `.ics` schedule feed, **every** in-scope
   program's Atom release feed (enumerated in Step 4, not hand-listed), the errata table. §17.1 puts the *fetch* half of `calendar sync` / `feed poll` /
   `errata sync` in `tick --sweep`, "each retrieved surface archived as a blob with a `fetch_log`
   record, exactly like a data artifact". The feeds retain only ~12 entries, so P1's "a window not
   captured is gone" applies to them exactly as it does to a data file.
4. **EP is flat files only.** Its `DiscoveryRule` crawl over per-vintage landing pages is Stage 12.
5. **JOLTS and JOLTS-STATE may share an artifact** — hence `programs` is a list, not a scalar.

- [ ] **Step 1: Write the failing tests**

`tests/capture/test_inventory.py`:

```python
from __future__ import annotations

from bls_stats.capture.inventory import by_url, load_inventory

IN_SCOPE = {"ces-n", "sae", "jolts", "jolts-state", "cps-ln", "bed", "qcew",
            "oews", "eci", "ecec", "ep"}

# Filled in at Step 4 from `discover --feeds`: the in-scope programs BLS
# actually publishes a release feed for. Not every program has one; recording
# the observed set is what makes the assertion below mean something.
PROGRAMS_WITH_FEEDS: set[str] = set()  # <- replace with the discovered set


def test_every_in_scope_program_has_at_least_one_data_artifact() -> None:
    """Spec 1.2: eleven programs. Spec 19 M0: a sweep over EVERY in-scope
    artifact -- an unlisted program is a permanently unobservable vintage."""
    inv = load_inventory()
    covered = {p for a in inv if a.kind == "data" for p in a.programs}
    assert IN_SCOPE <= covered, f"no data artifact for {IN_SCOPE - covered}"


def test_urls_are_unique() -> None:
    inv = load_inventory()
    assert len(by_url(inv)) == len(inv)


def test_keys_are_unique() -> None:
    inv = load_inventory()
    assert len({a.key for a in inv}) == len(inv)


def test_only_known_profiles_and_hosts() -> None:
    for a in load_inventory():
        assert a.profile in {"flatfile", "html"}
        host = a.url.split("/")[2]
        assert host in {"download.bls.gov", "data.bls.gov", "www.bls.gov"}
        assert (a.profile == "html") == (host == "www.bls.gov")


def test_html_paths_are_robots_clear() -> None:
    """www.bls.gov robots.txt generic `User-agent: *` block, 2026-08-10
    (findings section 3). Clearance is per-path: a new html surface must be
    checked before it is fetched (R12)."""
    disallowed = ("/scripts", "/crs", "/_private", "/iisadmin", "/srchadm",
                  "/advisory/members/", "/idcf")
    for a in load_inventory():
        if a.profile != "html":
            continue
        path = "/" + a.url.split("/", 3)[3]
        assert not path.startswith(disallowed), path
        assert "print" not in path, "the generic block disallows /*print*"


def test_metadata_surfaces_are_present() -> None:
    """Spec 17.1: the sweep archives the fetch half of calendar sync, feed
    poll and errata sync. Stage 4 has nothing to ingest otherwise."""
    urls = {a.url for a in load_inventory()}
    assert "https://www.bls.gov/schedule/news_release/bls.ics" in urls
    assert "https://www.bls.gov/errata/" in urls


def test_every_program_with_a_feed_has_it_archived() -> None:
    """Feeds retain ~12 entries (spec 6.4), so an unarchived program feed is
    a permanently lost window. Fill PROGRAMS_WITH_FEEDS from the --feeds
    discovery pass: an `any(...)` assertion would pass with one feed forever
    and never notice the other ten."""
    feed_programs = {
        p for a in load_inventory() if a.url.startswith("https://www.bls.gov/feed/")
        for p in a.programs
    }
    assert PROGRAMS_WITH_FEEDS <= feed_programs, (
        f"no archived feed for {PROGRAMS_WITH_FEEDS - feed_programs}")


def test_no_superset_and_subset_of_the_same_data_file() -> None:
    """Selection rule 1: never capture both AllItems and its per-measure
    subsets -- two change signals over the same cells."""
    data_keys = {a.key for a in load_inventory() if a.kind == "data"}
    for key in data_keys:
        prefix = key.split(".")[0]
        siblings = {k for k in data_keys if k.startswith(f"{prefix}.data.")}
        assert len(siblings) <= 1, f"{prefix}: multiple data files {siblings}"
```

`tests/capture/test_discover.py`:

```python
from __future__ import annotations

from pathlib import Path

from bls_stats.capture.discover import parse_listing

FIXTURE = Path(__file__).parent.parent / "fixtures" / "labstat_ce_listing.html"


def test_parse_listing_extracts_prefix_scoped_filenames() -> None:
    names = parse_listing(FIXTURE.read_text(), prefix="ce")
    assert "ce.data.0.AllCESSeries" in names
    assert "ce.series" in names
    assert all(n.startswith("ce.") for n in names)
    assert len(names) == len(set(names))
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest tests/capture/test_inventory.py tests/capture/test_discover.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'bls_stats.capture.inventory'`.

- [ ] **Step 3: Write `src/bls_stats/capture/discover.py`**

`download.bls.gov` serves an IIS directory listing per prefix. Parse the anchor hrefs rather than
guessing filenames.

```python
"""Enumerate what a LABSTAT prefix actually ships, so artifacts.toml
is built from observation rather than from memory of file names."""

from __future__ import annotations

import re
import sys

from bls_stats.config.settings import load_settings
from bls_stats.transport.client import Transport
from bls_stats.transport.profiles import build_profiles

LISTING_URL = "https://download.bls.gov/pub/time.series/{prefix}/"
FEED_INDEX_URL = "https://www.bls.gov/bls/rss.htm"
_HREF = re.compile(r'href="([^"?]+)"', re.IGNORECASE)


def parse_listing(html: str, prefix: str) -> tuple[str, ...]:
    seen: dict[str, None] = {}
    for href in _HREF.findall(html):
        name = href.rstrip("/").rsplit("/", 1)[-1]
        if name.startswith(f"{prefix}."):
            seen.setdefault(name, None)
    return tuple(seen)


def parse_feed_links(html: str) -> tuple[str, ...]:
    """Every /feed/*.rss href on the release-feed index.

    Worth enumerating rather than hand-listing: the feeds retain only ~12
    entries (spec 6.4), so a program feed nobody archived is a permanently
    lost window -- P1's logic, applied to a surface 17.1 puts squarely in
    this stage's sweep.
    """
    seen: dict[str, None] = {}
    for href in _HREF.findall(html):
        if "/feed/" in href and href.endswith(".rss"):
            url = href if href.startswith("http") else f"https://www.bls.gov{href}"
            seen.setdefault(url, None)
    return tuple(seen)


def main(prefixes: list[str]) -> int:
    settings = load_settings()
    with Transport(build_profiles(settings.bls_contact_email)) as transport:
        if prefixes == ["--feeds"]:
            transfer = transport.stream_get(FEED_INDEX_URL, "html")
            if transfer.spool is None:
                print(f"feeds: status={transfer.status_code} "
                      f"error={transfer.error_class}", file=sys.stderr)
                return 1
            html = transfer.spool.read_bytes().decode("utf-8", "replace")
            transfer.spool.unlink()
            for url in sorted(parse_feed_links(html)):
                print(f"feed\t{url.rsplit('/', 1)[-1]}\t{url}")
            return 0
        for prefix in prefixes:
            url = LISTING_URL.format(prefix=prefix)
            transfer = transport.stream_get(url, "flatfile")
            if transfer.spool is None:
                print(f"{prefix}: status={transfer.status_code} "
                      f"error={transfer.error_class}", file=sys.stderr)
                continue
            html = transfer.spool.read_bytes().decode("utf-8", "replace")
            transfer.spool.unlink()
            for name in sorted(parse_listing(html, prefix)):
                print(f"{prefix}\t{name}\thttps://download.bls.gov/pub/time.series/"
                      f"{prefix}/{name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
```

Record the fixture for the offline test by saving one real listing:

```bash
set -a; source .project.env; set +a
uv run python -m bls_stats.capture.discover ce > /tmp/ce-listing.tsv
```

Then save the raw HTML of that one request to `tests/fixtures/labstat_ce_listing.html` (add a
`--save-html PATH` option to `discover.py` if that is easier than re-fetching; one extra request to
`download.bls.gov` is acceptable, a second full pass is not).

- [ ] **Step 4: Build `src/bls_stats/config/artifacts.toml` from the discovery output**

```bash
set -a; source .project.env; set +a
uv run python -m bls_stats.capture.discover ce sm jt ln bd oe ci cm ep > /tmp/labstat.tsv
```

Then enumerate the release feeds — **check the index path against the generic `User-agent: *`
block in a re-fetched `robots.txt` before this request**, per the Global Constraints rule:

```bash
uv run python -m bls_stats.capture.discover --feeds > /tmp/feeds.tsv
```

If `https://www.bls.gov/bls/rss.htm` does not resolve, find the current feed index from
`https://www.bls.gov/bls/newsrels.htm` (already robots-cleared, findings §3) and record the URL you
used as a `> Deviation:` note. Add one `[[artifact]]` per in-scope program's feed, keyed
`feed.<slug>`, `profile = "html"`, `kind = "metadata"`.

⚠ **Politeness:** ten sequential GETs at the `flatfile` profile's 2-second pacing, plus one or two
for the feed index. Do not parallelise, do not re-run in a loop while iterating on the TOML — the
output is a file, iterate on that.

Prefix → program map (`bls-data-context` skill, `references/`): `ce` = CES-N · `sm` = SAE ·
`jt` = JOLTS (and, if it carries state series, JOLTS-STATE) · `ln` = CPS-LN · `bd` = BED ·
`oe` = OEWS · `ci` = ECI · `cm` = ECEC · `ep` = EP. **QCEW is not a LABSTAT prefix here:** R13
settles it on the quarterly singlefile CSVs at
`https://data.bls.gov/cew/data/files/{year}/csv/{year}_qtrly_singlefile.zip`, whose 2024 and 2025
patterns are both confirmed live (findings §1, §4).

Write the TOML in this shape, filling `[[artifact]]` blocks from the discovery output per the
selection rule. Three entries are pre-verified by Stage 1 and can be written as-is:

```toml
# The artifact inventory. Volatile strings live here, not in code (spec 5.1).
# Stage 4 grows this into the full ProgramSpec registry; Stage 2 needs only
# "what to sweep, over which profile".

[defaults]
profile = "flatfile"

# ---- CES national -----------------------------------------------------
[[artifact]]
key = "ce.data.0.AllCESSeries"
programs = ["ces-n"]
url = "https://download.bls.gov/pub/time.series/ce/ce.data.0.AllCESSeries"
kind = "data"                       # verified 2026-08-10: HEAD 200, ~334 MiB

[[artifact]]
key = "ce.series"
programs = ["ces-n"]
url = "https://download.bls.gov/pub/time.series/ce/ce.series"
kind = "mapping"

# ---- JOLTS ------------------------------------------------------------
[[artifact]]
key = "jt.data.1.AllItems"
programs = ["jolts", "jolts-state"]  # confirm state series presence via jt.series
url = "https://download.bls.gov/pub/time.series/jt/jt.data.1.AllItems"
kind = "data"                        # verified 2026-08-10: HEAD 200, ~33 MiB

# ---- QCEW -------------------------------------------------------------
[[artifact]]
key = "qcew.2025.qtrly_singlefile"
programs = ["qcew"]
url = "https://data.bls.gov/cew/data/files/2025/csv/2025_qtrly_singlefile.zip"
kind = "data"                        # verified 2026-08-10: HEAD 200, ~291 MiB

# ---- metadata surfaces (spec 17.1: the fetch half of the sync commands) --
[[artifact]]
key = "schedule.ics"
programs = ["_calendar"]
url = "https://www.bls.gov/schedule/news_release/bls.ics"
profile = "html"
kind = "metadata"                    # verified 2026-08-10: contact profile, 200

[[artifact]]
key = "feed.empsit"
programs = ["ces-n", "cps-ln"]
url = "https://www.bls.gov/feed/empsit.rss"
profile = "html"
kind = "metadata"                    # Atom body served as application/rss+xml

[[artifact]]
key = "errata.index"
programs = ["_errata"]
url = "https://www.bls.gov/errata/"
profile = "html"
kind = "metadata"                    # verified 2026-08-10: contact profile, 200
```

⚠ **The QCEW URL carries a literal year and needs an annual bump** until Stage 5 makes it a rule.
Record that as a deferred item at the completion gate rather than leaving it implicit.

⚠ **Every additional `www.bls.gov` path** — a second release feed, a notices index — must be
checked against the generic `User-agent: *` block in a re-fetched `robots.txt` before it goes in
(R12; findings §3's scope caveat). The `test_html_paths_are_robots_clear` test encodes the
2026-08-10 policy; re-fetch and update it if a new path is close to a disallowed prefix.

- [ ] **Step 5: Write `src/bls_stats/capture/inventory.py`**

```python
"""The artifact inventory: what to sweep, over which profile.

Spec 5.1 splits program facts by volatility -- URLs and filenames change
without a release and live in TOML that an operator can edit under time
pressure; date arithmetic and delta rules live in typed Python. This is the
TOML half, and Stage 4 grows it into the full ProgramSpec registry.
"""

from __future__ import annotations

import os
import tomllib
from collections.abc import Iterable
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import Literal

Kind = Literal["data", "mapping", "metadata"]


def default_path() -> Path:
    """Package data, not a repo-relative path: the container installs the
    wheel and has no repo root. `BLS_ARTIFACTS_PATH` overrides, so an
    operator can point at an edited copy under time pressure (spec 5.1)."""
    override = os.environ.get("BLS_ARTIFACTS_PATH")
    if override:
        return Path(override)
    return Path(str(files("bls_stats.config") / "artifacts.toml"))


@dataclass(frozen=True)
class Artifact:
    key: str
    programs: tuple[str, ...]
    url: str
    profile: str
    kind: Kind


def load_inventory(path: Path | None = None) -> tuple[Artifact, ...]:
    raw = tomllib.loads((path or default_path()).read_text(encoding="utf-8"))
    defaults = raw.get("defaults", {})
    artifacts = tuple(
        Artifact(
            key=entry["key"],
            programs=tuple(entry["programs"]),
            url=entry["url"],
            profile=entry.get("profile", defaults.get("profile", "flatfile")),
            kind=entry["kind"],
        )
        for entry in raw.get("artifact", [])
    )
    _assert_unique(artifacts)
    return artifacts


def _assert_unique(artifacts: Iterable[Artifact]) -> None:
    keys: set[str] = set()
    urls: set[str] = set()
    for a in artifacts:
        if a.key in keys:
            raise ValueError(f"duplicate artifact key: {a.key}")
        if a.url in urls:
            raise ValueError(f"duplicate artifact url: {a.url}")
        keys.add(a.key)
        urls.add(a.url)


def by_url(artifacts: Iterable[Artifact]) -> dict[str, Artifact]:
    """The map Stage 3 needs: fetch_log carries `url`, file_vintage needs
    `program` and `artifact_key` (spec 7.3 vs 7.4)."""
    return {a.url: a for a in artifacts}
```

- [ ] **Step 6: Run the tests to verify they pass**

```bash
uv run pytest tests/capture/test_inventory.py tests/capture/test_discover.py -v
```

Expected: 9 passed. `test_every_in_scope_program_has_at_least_one_data_artifact` is the one that
fails until every program has an entry — that is the point of it.

- [ ] **Step 7: Write and run the network-marked reachability check**

`tests/capture/test_inventory_network.py`:

```python
from __future__ import annotations

import pytest

from bls_stats.capture.inventory import load_inventory
from bls_stats.config.settings import load_settings
from bls_stats.transport.client import Transport
from bls_stats.transport.profiles import build_profiles

pytestmark = pytest.mark.network


def test_every_inventory_url_is_reachable() -> None:
    """Spec 18.3: a design decision built on a mocked assumption about a
    third party's edge network is a decision built on nothing. Run on
    demand, never in CI."""
    settings = load_settings()
    failures: list[str] = []
    with Transport(build_profiles(settings.bls_contact_email)) as transport:
        for artifact in load_inventory():
            probe = transport.head(artifact.url, artifact.profile)
            if probe.status_code != 200:
                failures.append(f"{artifact.key} -> {probe.status_code} "
                                f"{probe.error_class or ''}")
    assert not failures, failures
```

```bash
set -a; source .project.env; set +a
uv run pytest tests/capture/test_inventory_network.py --network -v
```

Expected: 1 passed. A `www.bls.gov` entry returning something other than 200 on HEAD is expected
for `conditional_get` surfaces — if HEAD is refused there, change the assertion for `html` entries
to a conditional GET returning 200 or 304, and record it as a `> Deviation:` note.

- [ ] **Step 8: Commit**

```bash
git add src/bls_stats/config/artifacts.toml src/bls_stats/capture/inventory.py \
        src/bls_stats/capture/discover.py tests/capture tests/fixtures
git commit -m "Add the artifact inventory and its discovery helper"
```

---

### Task 7: Watch state — the fold over `log/fetch/`

**Files:**
- Create: `src/bls_stats/capture/state.py`
- Test: `tests/capture/test_state.py`

**Interfaces:**
- Consumes: `ObjectStore` (Task 2), `FetchLogRecord` / `Outcome` (Task 5).
- Produces (used by Task 8):
  - `WatchState` — frozen dataclass `url: str`, `etag: str | None`,
    `http_last_modified: str | None`, `content_sha256: str | None`,
    `validators_observed_at: str | None`, `content_observed_at: str | None`
  - `load_watch_state(store) -> dict[str, WatchState]`
  - `decide_fetch(prior: WatchState | None, probe: Probe) -> bool`
  - `classify_bytes(prior: WatchState | None, content_sha256: str) -> Outcome`

- [ ] **Step 1: Write the failing test**

`tests/capture/test_state.py`:

```python
from __future__ import annotations

import json

from bls_stats.capture.fetchlog import Outcome
from bls_stats.capture.state import (
    WatchState,
    classify_bytes,
    decide_fetch,
    load_watch_state,
)
from bls_stats.transport.client import Probe

URL = "https://download.bls.gov/pub/time.series/ce/ce.data.0.AllCESSeries"


def _probe(etag: str | None = None, lm: str | None = None) -> Probe:
    return Probe(200, {}, {}, etag, lm, None, None, 1, None)


def _line(**over: object) -> str:
    base = {
        "url": URL, "requested_at": "2026-08-01T12:00:00+00:00", "outcome": "unchanged",
        "etag": '"v1"', "http_last_modified": "Sat, 01 Aug 2026 12:30:00 GMT",
        "content_sha256": None, "status_code": 200,
    }
    return json.dumps(base | over)


def test_fold_takes_validators_from_any_record(fake_store) -> None:
    fake_store.written["log/fetch/dt=2026-08-01/r-0000.jsonl"] = (_line() + "\n").encode()
    state = load_watch_state(fake_store)
    assert state[URL].etag == '"v1"'
    assert state[URL].content_sha256 is None


def test_fold_takes_content_hash_only_from_new_bytes(fake_store) -> None:
    """The asymmetry is load-bearing: HEAD records have no content hash and
    `unchanged` records never carry one, so folding it from 'the newest
    record' would blank it on every sweep."""
    fake_store.written["log/fetch/dt=2026-08-01/r-0000.jsonl"] = (
        _line(outcome="new_bytes", content_sha256="a" * 64,
              requested_at="2026-08-01T12:00:00+00:00") + "\n").encode()
    fake_store.written["log/fetch/dt=2026-08-02/r-0000.jsonl"] = (
        _line(outcome="unchanged", requested_at="2026-08-02T12:00:00+00:00",
              etag='"v2"') + "\n").encode()
    state = load_watch_state(fake_store)
    assert state[URL].etag == '"v2"', "validators come from the newest record"
    assert state[URL].content_sha256 == "a" * 64, "content hash survives later HEADs"


def test_fold_is_order_independent(fake_store) -> None:
    """Newest-wins by requested_at, not by the order records are read. Both
    records sit in ONE object, newest first, so key ordering cannot rescue a
    fold that quietly relies on arrival order."""
    fake_store.written["log/fetch/dt=2026-08-02/r-0000.jsonl"] = (
        _line(requested_at="2026-08-02T12:00:00+00:00", etag='"v2"') + "\n"
        + _line(requested_at="2026-08-01T12:00:00+00:00", etag='"v1"') + "\n"
    ).encode()
    assert load_watch_state(fake_store)[URL].etag == '"v2"'


def test_error_records_never_update_state(fake_store) -> None:
    fake_store.written["log/fetch/dt=2026-08-01/r-0000.jsonl"] = (
        _line(outcome="new_bytes", content_sha256="a" * 64) + "\n").encode()
    fake_store.written["log/fetch/dt=2026-08-03/r-0000.jsonl"] = (
        _line(outcome="error", requested_at="2026-08-03T12:00:00+00:00",
              status_code=None, etag=None) + "\n").encode()
    state = load_watch_state(fake_store)
    assert state[URL].etag == '"v1"'
    assert state[URL].content_sha256 == "a" * 64


def test_cold_start_always_fetches() -> None:
    assert decide_fetch(None, _probe(etag='"v1"')) is True


def test_matching_validators_mean_unchanged() -> None:
    prior = WatchState(URL, '"v1"', "Sat, 01 Aug 2026 12:30:00 GMT", "a" * 64, None, None)
    assert decide_fetch(prior, _probe('"v1"', "Sat, 01 Aug 2026 12:30:00 GMT")) is False


def test_either_validator_moving_means_fetch() -> None:
    prior = WatchState(URL, '"v1"', "Sat, 01 Aug 2026 12:30:00 GMT", "a" * 64, None, None)
    assert decide_fetch(prior, _probe('"v2"', "Sat, 01 Aug 2026 12:30:00 GMT")) is True
    assert decide_fetch(prior, _probe('"v1"', "Mon, 03 Aug 2026 12:30:00 GMT")) is True


def test_no_validators_on_the_probe_means_fetch() -> None:
    """Unchanged must be PROVEN, never assumed."""
    prior = WatchState(URL, '"v1"', "Sat, 01 Aug 2026 12:30:00 GMT", "a" * 64, None, None)
    assert decide_fetch(prior, _probe(None, None)) is True


def test_same_content_hash_is_restamp_only() -> None:
    """Spec 6.1: 'BLS republished this series unchanged on date D' is a real
    and different fact from 'BLS did not publish on D', and only the former
    proves the value was still current."""
    prior = WatchState(URL, '"v1"', "x", "a" * 64, None, None)
    assert classify_bytes(prior, "a" * 64) is Outcome.RESTAMP_ONLY
    assert classify_bytes(prior, "b" * 64) is Outcome.NEW_BYTES


def test_no_prior_content_hash_is_new_bytes() -> None:
    assert classify_bytes(None, "a" * 64) is Outcome.NEW_BYTES
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest tests/capture/test_state.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'bls_stats.capture.state'`.

- [ ] **Step 3: Write `src/bls_stats/capture/state.py`**

```python
"""What did this URL look like last time?

The capture plane writes no store table (spec 17.3), so this is a pure fold
over log/fetch/ -- which is exactly what P1 promises: everything derived is
rebuildable by deterministic replay. Stage 3's file_vintage supersedes this
as the efficient path; until then the fold is the only source.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from typing import Any, Protocol

from bls_stats.capture.fetchlog import Outcome
from bls_stats.transport.client import Probe

FETCH_LOG_PREFIX = "log/fetch/"
_STATE_BEARING = frozenset({Outcome.UNCHANGED, Outcome.RESTAMP_ONLY, Outcome.NEW_BYTES})


@dataclass(frozen=True)
class WatchState:
    url: str
    etag: str | None
    http_last_modified: str | None
    content_sha256: str | None
    validators_observed_at: str | None
    content_observed_at: str | None


class _Store(Protocol):
    def list(self, prefix: str) -> Any: ...
    def open_stream(self, key: str, chunk_size: int = ...) -> Any: ...


def load_watch_state(store: _Store) -> dict[str, WatchState]:
    """Fold every fetch_log record into per-URL last-known state.

    Two lookups with different rules, and the asymmetry is load-bearing:

      * `etag` / `http_last_modified` come from the newest record with any
        state-bearing outcome -- a HEAD carries them and so does a GET.
      * `content_sha256` comes from the newest `new_bytes` record ONLY.
        HEAD records have none, and an `unchanged` record never carries one,
        so folding it from "the newest record" would blank it every sweep.

    Reading the whole log rather than a recent window is deliberate. An
    artifact whose last content change predates the window would fold to
    content_sha256=None, and the next GET would be recorded as `new_bytes` --
    a fabricated vintage, permanent in an append-only store (P7).

    Cost: one small object per artifact per sweep, ~100/day. Fine now, and
    superseded by file_vintage at Stage 3 rather than grown into a problem.
    """
    state: dict[str, WatchState] = {}
    for key in store.list(FETCH_LOG_PREFIX):
        stream = store.open_stream(key)
        if stream is None:  # deleted between LIST and GET; nothing to fold
            continue
        for line in b"".join(stream).splitlines():
            if line.strip():
                _apply(state, json.loads(line))
    return state


def _apply(state: dict[str, WatchState], record: dict[str, Any]) -> None:
    outcome = record.get("outcome")
    if outcome not in {o.value for o in _STATE_BEARING}:
        return  # error / blocked records observe nothing about the artifact
    url = record["url"]
    at = record["requested_at"]
    prior = state.get(url) or WatchState(url, None, None, None, None, None)

    if prior.validators_observed_at is None or at >= prior.validators_observed_at:
        prior = replace(prior, etag=record.get("etag"),
                        http_last_modified=record.get("http_last_modified"),
                        validators_observed_at=at)
    if outcome == Outcome.NEW_BYTES.value and record.get("content_sha256"):
        if prior.content_observed_at is None or at >= prior.content_observed_at:
            prior = replace(prior, content_sha256=record["content_sha256"],
                            content_observed_at=at)
    state[url] = prior


def decide_fetch(prior: WatchState | None, probe: Probe) -> bool:
    """Spec 6.1: unchanged -> done, one request. Last-Modified or ETag
    changed -> GET. Unchanged must be PROVEN: a probe carrying neither
    validator cannot prove it, so it escalates."""
    if prior is None:
        return True
    if probe.status_code != 200:
        return False  # nothing to fetch; the record carries the status
    matched = False
    if probe.etag is not None and prior.etag is not None:
        if probe.etag != prior.etag:
            return True
        matched = True
    if probe.http_last_modified is not None and prior.http_last_modified is not None:
        if probe.http_last_modified != prior.http_last_modified:
            return True
        matched = True
    return not matched


def classify_bytes(prior: WatchState | None, content_sha256: str) -> Outcome:
    """A re-stamped Last-Modified with an identical hash is proof the value
    was still current -- a real and different fact from silence (spec 6.1)."""
    if prior is None or prior.content_sha256 is None:
        return Outcome.NEW_BYTES
    return (Outcome.RESTAMP_ONLY if content_sha256 == prior.content_sha256
            else Outcome.NEW_BYTES)
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
uv run pytest tests/capture/test_state.py -v
```

Expected: 10 passed.

- [ ] **Step 5: Commit**

```bash
git add src/bls_stats/capture/state.py tests/capture/test_state.py
git commit -m "Derive watch state as a fold over the fetch log"
```

---

### Task 8: The capture pipeline

**Files:**
- Create: `src/bls_stats/capture/pipeline.py`
- Test: `tests/capture/test_pipeline.py`

**Interfaces:**
- Consumes: everything from Tasks 2, 4, 5, 6, 7.
- Produces (used by Task 9):
  - `blob_key(wire_sha256: str) -> str`
  - `capture_artifact(artifact, prior, *, transport, raw, log_writer, force=False) -> FetchLogRecord`
  - `CaptureSummary` — frozen dataclass `checked: int`, `new_bytes: int`, `restamped: int`,
    `unchanged: int`, `errors: int`, `blocked: int`, `html_failures: int`, with
    `.degraded: bool` (true iff `html_failures > 0`) and
    `.plus(outcome, *, profile) -> CaptureSummary`
  - `sweep(artifacts, *, transport, raw, log_writer, state) -> CaptureSummary`

- [ ] **Step 1: Write the failing test**

`tests/capture/test_pipeline.py`:

```python
from __future__ import annotations

import gzip
import hashlib

import httpx
import pytest
import respx

from bls_stats.capture.fetchlog import FetchLogWriter, Outcome, new_run_id
from bls_stats.capture.inventory import Artifact
from bls_stats.capture.pipeline import blob_key, capture_artifact, sweep
from bls_stats.capture.state import WatchState
from bls_stats.transport.client import Transport
from bls_stats.transport.profiles import build_profiles

URL = "https://download.bls.gov/pub/time.series/ce/ce.data.0.AllCESSeries"
ART = Artifact("ce.data.0.AllCESSeries", ("ces-n",), URL, "flatfile", "data")


@pytest.fixture
def transport() -> Transport:
    return Transport(build_profiles("t@example.invalid"), min_interval_override=0.0)


def _writer(store):  # type: ignore[no-untyped-def]
    return FetchLogWriter(store, new_run_id())


def test_blob_key_is_two_level_hex_fan_out() -> None:
    h = "ab" + "c" * 62
    assert blob_key(h) == f"raw/blob/sha256=ab/cc/{h}"


@respx.mock
def test_unchanged_costs_one_head(transport: Transport, fake_store) -> None:
    head = respx.head(URL).mock(return_value=httpx.Response(
        200, headers={"ETag": '"v1"', "Last-Modified": "Sat, 01 Aug 2026 12:30:00 GMT"}))
    get = respx.get(URL)
    prior = WatchState(URL, '"v1"', "Sat, 01 Aug 2026 12:30:00 GMT", "a" * 64, None, None)
    record = capture_artifact(ART, prior, transport=transport, raw=fake_store,
                              log_writer=_writer(fake_store))
    assert record.outcome is Outcome.UNCHANGED
    assert record.method == "HEAD" and head.called and not get.called
    assert record.blob_key is None


@respx.mock
def test_changed_validator_archives_the_wire_bytes(transport: Transport, fake_store) -> None:
    plain = b"series\tvalue\n"
    wire = gzip.compress(plain)
    respx.head(URL).mock(return_value=httpx.Response(200, headers={"ETag": '"v2"'}))
    respx.get(URL).mock(return_value=httpx.Response(
        200, content=wire, headers={"Content-Encoding": "gzip", "ETag": '"v2"'}))
    prior = WatchState(URL, '"v1"', None, "a" * 64, None, None)
    record = capture_artifact(ART, prior, transport=transport, raw=fake_store,
                              log_writer=_writer(fake_store))
    assert record.outcome is Outcome.NEW_BYTES
    assert record.wire_sha256 == hashlib.sha256(wire).hexdigest()
    assert record.content_sha256 == hashlib.sha256(plain).hexdigest()
    assert record.blob_key == blob_key(record.wire_sha256)
    assert fake_store.written[record.blob_key] == wire, "bytes exactly as received"


@respx.mock
def test_restamp_only_still_archives_but_is_labelled(transport: Transport, fake_store) -> None:
    plain = b"unchanged content\n"
    respx.head(URL).mock(return_value=httpx.Response(200, headers={"ETag": '"v2"'}))
    respx.get(URL).mock(return_value=httpx.Response(200, content=plain))
    prior = WatchState(URL, '"v1"', None, hashlib.sha256(plain).hexdigest(), None, None)
    record = capture_artifact(ART, prior, transport=transport, raw=fake_store,
                              log_writer=_writer(fake_store))
    assert record.outcome is Outcome.RESTAMP_ONLY
    assert record.blob_key is not None, "P1: every distinct byte sequence is stored"


@respx.mock
def test_identical_blob_is_not_re_uploaded(transport: Transport, fake_store) -> None:
    body = b"same bytes\n"
    key = blob_key(hashlib.sha256(body).hexdigest())
    fake_store.written[key] = body
    respx.head(URL).mock(return_value=httpx.Response(200, headers={"ETag": '"v9"'}))
    respx.get(URL).mock(return_value=httpx.Response(200, content=body))
    record = capture_artifact(ART, None, transport=transport, raw=fake_store,
                              log_writer=_writer(fake_store))
    assert record.blob_key == key
    assert fake_store.written[key] == body


@respx.mock
def test_403_records_blocked_and_never_raises(transport: Transport, fake_store) -> None:
    html = Artifact("errata.index", ("_errata",), "https://www.bls.gov/errata/", "html",
                    "metadata")
    respx.get("https://www.bls.gov/errata/").mock(return_value=httpx.Response(403))
    record = capture_artifact(html, None, transport=transport, raw=fake_store,
                              log_writer=_writer(fake_store))
    assert record.outcome is Outcome.BLOCKED
    assert record.method == "GET", "html surfaces use conditional GET, not HEAD"


@respx.mock
def test_conditional_get_304_is_unchanged(transport: Transport, fake_store) -> None:
    html = Artifact("errata.index", ("_errata",), "https://www.bls.gov/errata/", "html",
                    "metadata")
    route = respx.get("https://www.bls.gov/errata/").mock(return_value=httpx.Response(304))
    prior = WatchState(html.url, '"e1"', None, "a" * 64, None, None)
    record = capture_artifact(html, prior, transport=transport, raw=fake_store,
                              log_writer=_writer(fake_store))
    assert record.outcome is Outcome.UNCHANGED
    assert route.calls[0].request.headers["if-none-match"] == '"e1"'


@respx.mock
def test_timeout_records_error_and_the_sweep_continues(transport: Transport,
                                                       fake_store) -> None:
    other = Artifact("jt.data.1.AllItems", ("jolts",),
                     "https://download.bls.gov/pub/time.series/jt/jt.data.1.AllItems",
                     "flatfile", "data")
    respx.head(URL).mock(side_effect=httpx.ConnectTimeout("boom"))
    respx.head(other.url).mock(return_value=httpx.Response(200, headers={"ETag": '"j1"'}))
    respx.get(other.url).mock(return_value=httpx.Response(200, content=b"ok\n"))
    summary = sweep((ART, other), transport=transport, raw=fake_store,
                    log_writer=_writer(fake_store), state={})
    assert summary.errors == 1 and summary.new_bytes == 1
    assert summary.checked == 2, "one artifact failing never aborts the sweep"


@respx.mock
def test_degraded_only_when_an_html_surface_fails(transport: Transport, fake_store) -> None:
    html = Artifact("errata.index", ("_errata",), "https://www.bls.gov/errata/", "html",
                    "metadata")
    respx.get("https://www.bls.gov/errata/").mock(return_value=httpx.Response(403))
    summary = sweep((html,), transport=transport, raw=fake_store,
                    log_writer=_writer(fake_store), state={})
    assert summary.blocked == 1 and summary.degraded is True
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest tests/capture/test_pipeline.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'bls_stats.capture.pipeline'`.

- [ ] **Step 3: Write `src/bls_stats/capture/pipeline.py`**

```python
"""One artifact: decide -> fetch -> archive -> record.

P2's ordering, and the whole of it:

    HEAD (detect change) -> GET (stream bytes to archive)
                         -> append fetch_log record   <- DURABILITY BOUNDARY

Nothing downstream of that boundary exists yet (spec 19 M0). This module is
the piece that must never break, because it is the piece whose failure loses
data permanently -- so it takes no lock, touches no transactional table, and
its only outputs are two immutable object PUTs.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

import structlog

from bls_stats.capture.fetchlog import (
    FetchLogRecord,
    FetchLogWriter,
    Outcome,
    record_from_probe,
    record_from_transfer,
    utcnow_iso,
)
from bls_stats.capture.inventory import Artifact
from bls_stats.capture.state import WatchState, classify_bytes, decide_fetch
from bls_stats.transport.client import Transport

log = structlog.get_logger(__name__)


class _Raw(Protocol):
    def exists(self, key: str) -> bool: ...
    def put_stream(self, key: str, fileobj: Any) -> None: ...


def blob_key(wire_sha256: str) -> str:
    """s3://<raw-bucket>/raw/blob/sha256=<hh>/<hh>/<full-wire-sha256> --
    two-level hex fan-out (spec 7.3). Keyed on the WIRE hash: keying on the
    decoded hash would make the object key a function of the decompressor's
    version."""
    return f"raw/blob/sha256={wire_sha256[:2]}/{wire_sha256[2:4]}/{wire_sha256}"


@dataclass(frozen=True)
class CaptureSummary:
    checked: int = 0
    new_bytes: int = 0
    restamped: int = 0
    unchanged: int = 0
    errors: int = 0
    blocked: int = 0
    html_failures: int = 0

    @property
    def degraded(self) -> bool:
        """Exit code 30: an HTML surface is unreachable, ingest continuing
        (spec 15). Flat-file failures are not degradation -- they are the
        thing that must not happen, and they surface as a non-zero error
        count for the operator."""
        return self.html_failures > 0

    def plus(self, outcome: Outcome, *, profile: str) -> "CaptureSummary":
        counts = {
            Outcome.NEW_BYTES: "new_bytes", Outcome.RESTAMP_ONLY: "restamped",
            Outcome.UNCHANGED: "unchanged", Outcome.ERROR: "errors",
            Outcome.BLOCKED: "blocked",
        }
        field = counts[outcome]
        html_extra = int(profile == "html" and outcome in {Outcome.ERROR, Outcome.BLOCKED})
        return CaptureSummary(
            checked=self.checked + 1,
            html_failures=self.html_failures + html_extra,
            **{k: getattr(self, k) + (1 if k == field else 0)
               for k in ("new_bytes", "restamped", "unchanged", "errors", "blocked")},
        )


def _outcome_for_status(status: int | None, error_class: str | None) -> Outcome:
    if status == 403:
        return Outcome.BLOCKED
    if status is None or status >= 400:
        return Outcome.ERROR
    return Outcome.ERROR if error_class else Outcome.UNCHANGED


def capture_artifact(artifact: Artifact, prior: WatchState | None, *,
                     transport: Transport, raw: _Raw, log_writer: FetchLogWriter,
                     force: bool = False) -> FetchLogRecord:
    profile = transport.profile(artifact.profile)
    requested_at = utcnow_iso()

    if profile.change_check == "head" and not force:
        probe = transport.head(artifact.url, artifact.profile)
        if probe.status_code != 200 or probe.error_class:
            record = record_from_probe(
                probe, url=artifact.url, profile=artifact.profile,
                outcome=_outcome_for_status(probe.status_code, probe.error_class),
                requested_at=requested_at)
            log_writer.append(record)
            return record
        if not decide_fetch(prior, probe):
            record = record_from_probe(probe, url=artifact.url, profile=artifact.profile,
                                       outcome=Outcome.UNCHANGED, requested_at=requested_at)
            log_writer.append(record)
            return record

    conditional: dict[str, str] = {}
    if profile.change_check == "conditional_get" and prior is not None and not force:
        if prior.etag:
            conditional["If-None-Match"] = prior.etag
        if prior.http_last_modified:
            conditional["If-Modified-Since"] = prior.http_last_modified

    requested_at = utcnow_iso()
    transfer = transport.stream_get(artifact.url, artifact.profile,
                                    conditional=conditional or None)
    try:
        if transfer.wire_sha256 is None:
            outcome = (Outcome.UNCHANGED if transfer.status_code == 304
                       else _outcome_for_status(transfer.status_code, transfer.error_class))
            record = record_from_transfer(transfer, url=artifact.url,
                                          profile=artifact.profile, outcome=outcome,
                                          requested_at=requested_at, blob_key=None)
            log_writer.append(record)
            return record

        key = blob_key(transfer.wire_sha256)
        # Content-addressed: a duplicate PUT is a no-op, so skipping it is a
        # bandwidth optimisation, never a semantic one. The outcome below is
        # unaffected.
        if not raw.exists(key):
            assert transfer.spool is not None
            with transfer.spool.open("rb") as fh:
                raw.put_stream(key, fh)

        assert transfer.content_sha256 is not None
        outcome = classify_bytes(prior, transfer.content_sha256)
        record = record_from_transfer(transfer, url=artifact.url, profile=artifact.profile,
                                      outcome=outcome, requested_at=requested_at,
                                      blob_key=key)
        log_writer.append(record)
        return record
    finally:
        if transfer.spool is not None:
            transfer.spool.unlink(missing_ok=True)  # scratch, deleted after use (17.2)


def sweep(artifacts: tuple[Artifact, ...], *, transport: Transport, raw: _Raw,
          log_writer: FetchLogWriter,
          state: dict[str, WatchState]) -> CaptureSummary:
    """Baseline sweep over every in-scope artifact (spec 6.2), so row 7 of
    the truth table -- silent mutation -- is detectable even when nothing is
    due. One artifact failing never aborts the sweep: a missed capture is
    permanent, so the loop keeps going and the failure lands in the log."""
    summary = CaptureSummary()
    for artifact in artifacts:
        try:
            record = capture_artifact(artifact, state.get(artifact.url), transport=transport,
                                      raw=raw, log_writer=log_writer)
            outcome = record.outcome
        except Exception as exc:  # noqa: BLE001 - never let one artifact stop the sweep
            log.error("capture_failed", artifact=artifact.key, error=str(exc))
            outcome = Outcome.ERROR
        summary = summary.plus(outcome, profile=artifact.profile)
    return summary
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
uv run pytest tests/capture/test_pipeline.py -v
```

Expected: 9 passed.

- [ ] **Step 5: Run the whole suite and the linters**

```bash
uv run ruff check . && uv run mypy && uv run pytest -q
```

Expected: clean, all tests passing.

- [ ] **Step 6: Commit**

```bash
git add src/bls_stats/capture/pipeline.py tests/capture/test_pipeline.py
git commit -m "Add the capture pipeline: HEAD, GET, archive, record"
```

---

### Task 9: The CLI — `capture`, `watch`, `tick --fast|--sweep`

**Files:**
- Create: `src/bls_stats/cli/runtime.py`, `src/bls_stats/cli/capture.py`,
  `src/bls_stats/cli/tick.py`
- Modify: `src/bls_stats/cli/app.py`
- Test: `tests/cli/test_tick.py`

**Interfaces:**
- Consumes: Tasks 2, 4, 6, 7, 8.
- Produces (used by Task 10 and Stage 4):
  - `bls-stats capture --url U [--force]` — force a GET + archive
  - `bls-stats watch [--program P] [--due-only]` — HEAD sweep, the fast loop
  - `bls-stats tick --sweep` — baseline sweep over the whole inventory
  - `bls-stats tick --fast` — **the Stage-4 wiring point**:
    `bls_stats.cli.tick.due_artifacts(inventory) -> tuple[Artifact, ...]` returns `()` in Stage 2
    and is replaced by a `release_slot` query in Stage 4. **Stage 4 implementer: this function is
    the seam.**
  - `bls_stats.cli.runtime` — `run_sweep(artifacts) -> CaptureSummary`,
    `report(summary) -> None`, and re-exported `load_settings` / `main_store` / `raw_store` /
    `load_inventory` / `load_watch_state` / `build_profiles`. Stage 3's writer lease wraps
    `run_sweep`'s counterpart here rather than in each command.

- [ ] **Step 1: Write the failing test**

`tests/cli/test_tick.py`:

```python
from __future__ import annotations

import httpx
import pytest
import respx
from typer.testing import CliRunner

from bls_stats.capture.inventory import Artifact
from bls_stats.capture.state import WatchState
from bls_stats.cli import runtime
from bls_stats.cli.app import app
from bls_stats.cli.exit_codes import ExitCode
from bls_stats.config.settings import Settings

runner = CliRunner()

FLAT = Artifact("ce.data.0.AllCESSeries", ("ces-n",),
                "https://download.bls.gov/pub/time.series/ce/ce.data.0.AllCESSeries",
                "flatfile", "data")
HTML = Artifact("errata.index", ("_errata",), "https://www.bls.gov/errata/", "html",
                "metadata")


@pytest.fixture
def wired(monkeypatch: pytest.MonkeyPatch, fake_store):
    """Point the CLI at an in-memory store and a settable inventory. Every
    command resolves its dependencies through bls_stats.cli.runtime, so this
    is one seam rather than one per command module. The endpoint contract is
    tested for real in tests/objstore/test_real_store.py; here the subject is
    exit codes and request counts."""
    settings = Settings(_env_file=None, aws_endpoint_url="http://127.0.0.1:9000",
                        aws_access_key_id="k", aws_secret_access_key="s",
                        bls_contact_email="t@example.invalid")
    monkeypatch.setattr(runtime, "load_settings", lambda: settings)
    monkeypatch.setattr(runtime, "main_store", lambda _s: fake_store)
    monkeypatch.setattr(runtime, "raw_store", lambda _s: fake_store)

    def _set(*artifacts: Artifact, state: dict[str, WatchState] | None = None) -> None:
        monkeypatch.setattr(runtime, "load_inventory", lambda: tuple(artifacts))
        monkeypatch.setattr(runtime, "load_watch_state", lambda _s: state or {})
        monkeypatch.setattr(runtime, "build_profiles", _fast_profiles)

    return _set


def _fast_profiles(email: str):
    """Zero the pacing so the suite does not sleep 2 s per request."""
    from bls_stats.transport.profiles import build_profiles
    return {name: type(p)(p.name, p.headers, p.max_connections, 0.0, p.change_check)
            for name, p in build_profiles(email).items()}


def test_fast_is_a_no_op_until_stage_4_wires_slots(wired) -> None:
    """Spec 17.1: 'No-op when nothing is due.' There are no slots until
    Stage 4, so nothing is ever due and the loop must cost zero requests --
    not one accidental full sweep every two minutes."""
    wired(FLAT)
    with respx.mock(assert_all_called=False) as router:
        result = runner.invoke(app, ["tick", "--fast"])
    assert result.exit_code == ExitCode.SUCCESS
    assert len(router.calls) == 0
    assert "nothing due" in result.stdout


def test_due_artifacts_is_the_stage_4_seam() -> None:
    from bls_stats.capture.inventory import load_inventory
    from bls_stats.cli.tick import due_artifacts

    assert due_artifacts(load_inventory()) == ()


def test_tick_requires_exactly_one_mode(wired) -> None:
    wired(FLAT)
    assert runner.invoke(app, ["tick"]).exit_code == ExitCode.UNEXPECTED_ERROR
    assert runner.invoke(app, ["tick", "--fast", "--sweep"]).exit_code == (
        ExitCode.UNEXPECTED_ERROR)


@respx.mock
def test_sweep_exits_30_when_an_html_surface_is_blocked(wired) -> None:
    """Spec 15: exit 30 = degraded, an HTML surface is unreachable, ingest
    continuing. Not 1 -- flat-file ingest did not fail."""
    wired(HTML)
    respx.get(HTML.url).mock(return_value=httpx.Response(403))
    result = runner.invoke(app, ["tick", "--sweep"])
    assert result.exit_code == ExitCode.DEGRADED
    assert "blocked=1" in result.stdout


@respx.mock
def test_sweep_exits_0_when_everything_is_unchanged(wired) -> None:
    prior = WatchState(FLAT.url, '"v1"', "Sat, 01 Aug 2026 12:30:00 GMT", "a" * 64,
                       None, None)
    wired(FLAT, state={FLAT.url: prior})
    head = respx.head(FLAT.url).mock(return_value=httpx.Response(
        200, headers={"ETag": '"v1"', "Last-Modified": "Sat, 01 Aug 2026 12:30:00 GMT"}))
    get = respx.get(FLAT.url)
    result = runner.invoke(app, ["tick", "--sweep"])
    assert result.exit_code == ExitCode.SUCCESS
    assert "unchanged=1" in result.stdout
    assert head.called and not get.called


@respx.mock
def test_sweep_archives_a_changed_artifact(wired, fake_store) -> None:
    wired(FLAT)
    respx.head(FLAT.url).mock(return_value=httpx.Response(200, headers={"ETag": '"v1"'}))
    respx.get(FLAT.url).mock(return_value=httpx.Response(200, content=b"series\tvalue\n"))
    result = runner.invoke(app, ["tick", "--sweep"])
    assert result.exit_code == ExitCode.SUCCESS
    assert "new_bytes=1" in result.stdout
    assert any(k.startswith("raw/blob/sha256=") for k in fake_store.written)
    assert any(k.startswith("log/fetch/dt=") for k in fake_store.written)


@respx.mock
def test_capture_forces_a_get_without_a_head(wired, fake_store) -> None:
    """Spec 15: `capture` forces a GET + archive. Forcing must skip the HEAD
    entirely -- an operator running this during triage already knows the
    validator says unchanged."""
    wired(FLAT)
    head = respx.head(FLAT.url)
    respx.get(FLAT.url).mock(return_value=httpx.Response(200, content=b"forced\n"))
    result = runner.invoke(app, ["capture", "--url", FLAT.url])
    assert result.exit_code == ExitCode.SUCCESS
    assert not head.called
    assert "new_bytes" in result.stdout


@respx.mock
def test_watch_filters_by_program(wired) -> None:
    other = Artifact("jt.data.1.AllItems", ("jolts",),
                     "https://download.bls.gov/pub/time.series/jt/jt.data.1.AllItems",
                     "flatfile", "data")
    wired(FLAT, other)
    respx.head(FLAT.url).mock(return_value=httpx.Response(200, headers={"ETag": '"v1"'}))
    respx.get(FLAT.url).mock(return_value=httpx.Response(200, content=b"a\n"))
    result = runner.invoke(app, ["watch", "--program", "ces-n"])
    assert result.exit_code == ExitCode.SUCCESS
    assert "checked=1" in result.stdout
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest tests/cli -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'bls_stats.cli.tick'`.

- [ ] **Step 3: Write `src/bls_stats/cli/runtime.py` and `src/bls_stats/cli/tick.py`**

`runtime.py` is the single place the commands resolve their dependencies — one seam for tests,
one place to change when Stage 3 adds the writer lease:

```python
"""Shared CLI wiring. Every command resolves settings, stores, inventory and
watch state through this module, so a test patches one seam and the writer
lease has one place to land at Stage 3."""

from __future__ import annotations

import typer

from bls_stats.capture.fetchlog import FetchLogWriter, new_run_id
from bls_stats.capture.inventory import Artifact, load_inventory
from bls_stats.capture.pipeline import CaptureSummary, sweep
from bls_stats.capture.state import load_watch_state
from bls_stats.cli.exit_codes import ExitCode
from bls_stats.config.settings import load_settings
from bls_stats.objstore.client import main_store, raw_store
from bls_stats.transport.client import Transport
from bls_stats.transport.profiles import build_profiles

__all__ = [
    "Artifact", "build_profiles", "load_inventory", "load_settings",
    "load_watch_state", "main_store", "raw_store", "report", "run_sweep",
]


def run_sweep(artifacts: tuple[Artifact, ...]) -> CaptureSummary:
    settings = load_settings()
    main = main_store(settings)
    raw = raw_store(settings)
    state = load_watch_state(main)
    writer = FetchLogWriter(main, new_run_id())
    with Transport(build_profiles(settings.bls_contact_email)) as transport:
        return sweep(artifacts, transport=transport, raw=raw, log_writer=writer,
                     state=state)


def report(summary: CaptureSummary) -> None:
    typer.echo(
        f"checked={summary.checked} new_bytes={summary.new_bytes} "
        f"restamped={summary.restamped} unchanged={summary.unchanged} "
        f"errors={summary.errors} blocked={summary.blocked}")
    if summary.degraded:
        raise typer.Exit(ExitCode.DEGRADED)
```

`tick.py`:

```python
"""The composite loops (spec 17.1).

--fast and --sweep take no lease and write no store table: every ledger
effect they would have had is derived from the fetch_log records they wrote
and materialised by the next `tick --write` (Stage 3). The fast loop cannot
wait on a lock because it never asks for one.
"""

from __future__ import annotations

import typer

from bls_stats.cli import runtime
from bls_stats.cli.exit_codes import ExitCode


def due_artifacts(inventory: tuple[runtime.Artifact, ...]) -> tuple[runtime.Artifact, ...]:
    """Artifacts inside a watch window (spec 6.2's ladder).

    STAGE-4 WIRING POINT. Slots do not exist until Stage 4, so nothing is
    ever due and this returns (). Stage 4 replaces the body with a
    release_slot query and `tick --fast` starts doing work with no other
    change to this module.
    """
    return ()


def tick(
    fast: bool = typer.Option(False, "--fast", help="poll slots in a watch window"),
    sweep_all: bool = typer.Option(False, "--sweep", help="baseline sweep, every artifact"),
) -> None:
    """The loops cron runs. --fast/--sweep take no lease (spec 15)."""
    if fast == sweep_all:
        typer.echo("choose exactly one of --fast / --sweep", err=True)
        raise typer.Exit(ExitCode.UNEXPECTED_ERROR)
    inventory = runtime.load_inventory()
    artifacts = due_artifacts(inventory) if fast else inventory
    if not artifacts:
        typer.echo("checked=0 (nothing due)")
        return
    runtime.report(runtime.run_sweep(artifacts))
```

- [ ] **Step 4: Write `src/bls_stats/cli/capture.py` and wire the app**

```python
"""Lock-free capture commands (spec 15's plane table): `capture` is
lock-free while `parse` and `commit` are not -- the same split as P2's
durability boundary, so a forced capture during triage always succeeds even
mid-parse."""

from __future__ import annotations

import typer

from bls_stats.capture.fetchlog import FetchLogWriter, Outcome, new_run_id
from bls_stats.capture.inventory import Artifact, by_url
from bls_stats.capture.pipeline import capture_artifact
from bls_stats.cli import runtime
from bls_stats.cli.exit_codes import ExitCode
from bls_stats.cli.tick import due_artifacts
from bls_stats.transport.client import Transport


def capture(url: str = typer.Option(..., "--url"),
            force: bool = typer.Option(True, "--force/--no-force")) -> None:
    """Force a GET + archive."""
    settings = runtime.load_settings()
    artifact = by_url(runtime.load_inventory()).get(url) or Artifact(
        key=url, programs=("_adhoc",), url=url,
        profile="html" if "//www.bls.gov/" in url else "flatfile", kind="data")
    main = runtime.main_store(settings)
    prior = runtime.load_watch_state(main).get(url)
    writer = FetchLogWriter(main, new_run_id())
    with Transport(runtime.build_profiles(settings.bls_contact_email)) as transport:
        record = capture_artifact(artifact, prior, transport=transport,
                                  raw=runtime.raw_store(settings), log_writer=writer,
                                  force=force)
    typer.echo(f"{record.outcome} {record.blob_key or ''}")
    if record.outcome in {Outcome.ERROR, Outcome.BLOCKED}:
        raise typer.Exit(ExitCode.DEGRADED if artifact.profile == "html"
                         else ExitCode.UNEXPECTED_ERROR)


def watch(program: str | None = typer.Option(None, "--program"),
          due_only: bool = typer.Option(False, "--due-only")) -> None:
    """HEAD sweep; the fast loop. `--due-only` is a no-op until Stage 4."""
    inventory = runtime.load_inventory()
    if due_only:
        inventory = due_artifacts(inventory)
    if program:
        inventory = tuple(a for a in inventory if program in a.programs)
    if not inventory:
        typer.echo("checked=0 (nothing selected)")
        return
    runtime.report(runtime.run_sweep(inventory))
```

`src/bls_stats/cli/app.py` — commands registered flat, so `bls-stats tick --sweep` and
`bls-stats capture --url ...` read exactly as §15 writes them:

```python
"""The typer app. Command groups mirror the planes (spec 15)."""

from __future__ import annotations

import typer

from bls_stats.cli import admin
from bls_stats.cli.capture import capture, watch
from bls_stats.cli.tick import tick

app = typer.Typer(no_args_is_help=True, add_completion=False)
app.add_typer(admin.app, name="admin")
app.command("tick")(tick)
app.command("capture")(capture)
app.command("watch")(watch)
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
uv run pytest tests/cli -v && uv run bls-stats --help
```

Expected: tests pass; `--help` lists `admin`, `tick`, `capture`, `watch`.

- [ ] **Step 6: First real sweep against the dev endpoint**

```bash
set -a; source .project.env; set +a
uv run bls-stats tick --sweep
```

⚠ **This is the cold-start run and it downloads the world.** The fold over an empty `log/fetch/`
yields no prior state, so every artifact reads as changed: ~334 MiB for `ce.data.0.AllCESSeries`,
~291 MiB for the QCEW singlefile, ~33 MiB for JOLTS, plus the rest — sequentially, at the
`flatfile` profile's 2-second pacing. Budget 20–40 minutes and a few GB of transfer. **This is the
first capture; the bytes it archives are the beginning of the store.** Do not interrupt it casually.

Then re-run it:

```bash
uv run bls-stats tick --sweep
```

Expected on the second run: `new_bytes=0` for everything that did not actually change between the
two runs, and a per-artifact cost of one HEAD. That contrast — a full download then a cheap sweep —
is the change-detection path working end to end.

- [ ] **Step 7: Commit**

```bash
git add src/bls_stats/cli tests/cli
git commit -m "Add the capture CLI and the fast/sweep tick loops"
```

---

### Task 10: Deploy the sweep on a schedule

**Files:**
- Create: `deploy/Containerfile`, `deploy/crontab`, `deploy/README.md`
- Test: `tests/test_deployment.py`

**Interfaces:**
- Consumes: everything.
- Produces: a scheduled `tick --sweep` running against the deployment endpoint — this stage's Exit
  criterion.

**Operator inputs required:** 1 (deployment endpoint credentials) and 2 (a container host). **Both
are blocking.** Without them this task cannot complete and the stage's Exit criterion is not met;
say so plainly at the completion gate rather than substituting the dev endpoint.

- [ ] **Step 1: Write `deploy/Containerfile`**

```dockerfile
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src/ src/
RUN uv sync --frozen --no-dev

# Stateless: all state in the object store; scratch under $TMPDIR, deleted
# after use (spec 17.2). Config via environment; secrets via the platform's
# secret store, never baked into the image.
ENV TMPDIR=/tmp
ENTRYPOINT ["uv", "run", "bls-stats"]
CMD ["--help"]
```

- [ ] **Step 2: Write `deploy/crontab`**

For a long-running container. §17.1's cadences, minus the loops that do not exist yet:

```cron
# Spec 17.1. --fast and --sweep take no lease and write no store table.
# tick --write / --daily / compact arrive with Stage 3's writer.
*/2 * * * * cd /app && uv run bls-stats tick --fast >>/proc/1/fd/1 2>&1
17 * * * * cd /app && uv run bls-stats tick --sweep >>/proc/1/fd/1 2>&1
```

For a platform that schedules one-shot containers instead, the same two invocations run as two
scheduled jobs. Either way the invariant is idempotency (§15): re-running after a crash is always
safe, which is what makes ephemeral containers viable.

- [ ] **Step 3: Write `deploy/README.md` — the runbook**

Cover, in this order:

1. **Required environment**: `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
   `AWS_REGION`, `BLS_RAW_BUCKET`, `BLS_MAIN_BUCKET`, `BLS_CONTACT_EMAIL`. Secrets come from the
   platform's secret store. **Do not name the deployment operator or any employer.**
2. **The runtime credential** — non-admin, delete-denied on the raw bucket, and its policy document
   from Task 3 Step 1 including the three excluded actions.
3. **First-run order**: `admin buckets create --apply` → gate output all `ok` → `tick --sweep`.
   **A failed gate is a stop** (findings §7 step 4).
4. **The cold-start cost** — the first sweep downloads every artifact in full; subsequent sweeps
   cost one HEAD each.
5. **Exit codes** (§15) and what cron should do with each: `0` fine, `30` degraded — an HTML
   surface is unreachable and flat-file ingest is continuing, so alert but do not page, `1`
   investigate.
6. **Known standing gaps, dated**: the archive is single-copy until a replication mechanism is
   chosen (N12 accrues from first capture — findings §6, deferred item 2); the QCEW inventory URL
   needs an annual year bump until Stage 5; the BLS API key expires annually — record its
   registration date and renewal date here (findings §2, a Stage-2 setup item).
7. **What this deployment does not do yet**: no parsing, no ledger, no store tables, no writer
   lease. Those arrive at Stage 3.

- [ ] **Step 4: Build and smoke-test the image locally**

```bash
podman build -f deploy/Containerfile -t bls-stats:stage2 .
podman run --rm --env-file .project.env bls-stats:stage2 --help
```

Expected: the CLI help. If the dev endpoint is on the host's loopback, the container cannot reach
it — that is §20 issue 14 in miniature, and the reason input 1 exists. Use the deployment endpoint.

- [ ] **Step 5: Run the sheet and the first sweep at the deployment endpoint**

```bash
podman run --rm --env-file deploy.env bls-stats:stage2 admin buckets create --apply
podman run --rm --env-file deploy.env bls-stats:stage2 tick --sweep
```

Every gate line must read `ok` before the sweep runs.

- [ ] **Step 6: Write and run the coverage test — the Exit criterion, made checkable**

`tests/test_deployment.py`:

```python
from __future__ import annotations

import datetime as dt

import pytest

from bls_stats.capture.inventory import load_inventory
from bls_stats.capture.state import load_watch_state
from bls_stats.config.settings import load_settings
from bls_stats.objstore.client import main_store

pytestmark = pytest.mark.real_store


def test_every_artifact_was_swept_today() -> None:
    """Stage-2 Exit: every in-scope artifact HEAD-swept on schedule. Reads
    the log the deployment actually wrote, not a local run."""
    store = main_store(load_settings())
    today = dt.date.today().isoformat()
    swept: set[str] = set()
    for key in store.list(f"log/fetch/dt={today}/"):
        stream = store.open_stream(key)
        assert stream is not None
        import json
        for line in b"".join(stream).splitlines():
            if line.strip():
                swept.add(json.loads(line)["url"])
    missing = {a.url for a in load_inventory()} - swept
    assert not missing, f"not swept today: {sorted(missing)}"


def test_a_changed_artifact_has_both_hashes_and_a_blob() -> None:
    """Stage-2 Exit: a changed artifact yields blob + fetch_log record with
    both hashes; restamp_only distinguished from new_bytes."""
    store = main_store(load_settings())
    state = load_watch_state(store)
    captured = [s for s in state.values() if s.content_sha256]
    assert captured, "no artifact has ever been captured"
```

```bash
AWS_ENDPOINT_URL=... uv run pytest tests/test_deployment.py --real-store -v
```

Expected: 2 passed, against the deployment endpoint, after a scheduled sweep has run.

- [ ] **Step 7: Confirm the schedule actually fires**

Wait for two scheduled sweeps (or two hours), then re-run Step 6's coverage test and check that
`log/fetch/` has objects from two distinct run ids. A sweep that only ever ran because a human
typed it is not a deployed scheduled sweep.

- [ ] **Step 8: Commit**

```bash
git add deploy tests/test_deployment.py
git commit -m "Deploy the capture sweep on a schedule"
```

---

## Completion

Run the Plan Completion Protocol from the writing-plans skill: the resolve-before-defer gate first
(batch every unanswered operator input as one set of questions), then plan markup, then
`specs/deferred_items.md` — ticking items **3** and **7** (and **1**, **2**, **8** if the operator
inputs arrived), appending this plan's own deferrals — then `git mv` the plan to
`specs/plans/completed/` and tick Stage 2 in the roadmap with the stamp:

> Stage 2: COMPLETE (YYYY-MM-DD) — implemented by plan 2
> (specs/plans/completed/2-bls-stats-stage2-capture.md).
> Next: resume the roadmap.

**Deferrals this plan already knows it will produce** (append them, plus whatever execution adds):

- Ranged / resumable transfers, with the `Accept-Encoding` conflict and the
  350,208,884-vs-47,300,620 `Content-Length` divergence as the recorded reasoning (Task 4 Step 4).
- The QCEW inventory URL's literal year, until Stage 5 makes it a rule (Task 6 Step 4).
- The `log/fetch/` full-fold cost, superseded by `file_vintage` at Stage 3 (Task 7 Step 3).
- The replication mechanism and the resulting single-copy period (N12), unless input 4 resolved it.

**Re-validate later stages against what shipped**, per the roadmap stamp. At minimum: Stage 3
inherits the `url → (program, artifact_key)` resolution obligation and the `fetch_log` object-naming
deviation; Stage 4 inherits `due_artifacts()` as its wiring point and the Atom-served-as-RSS
finding.
