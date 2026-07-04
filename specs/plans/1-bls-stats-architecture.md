# bls-stats Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `bls-stats` package: vintage-aware download of eight BLS data products with two-stage ingest (bulk backfill + daily feed-driven increments) into a Delta Lake store on S3-compatible storage.

**Architecture:** Config-driven core (programs are registry *data*) with a small set of fetch engines behind one protocol; a Delta-backed vintage store keyed `(program, unit, ref_date, release_date)` with semantic `(revision, benchmark)` print counters; a commit-then-record pipeline with presence-check idempotency; typer CLI where `ingest` is the daily cron entrypoint. Spec: `specs/bls-stats-architecture.md` (this plan cites it as **ARCH §n**); behavioral contract: `bls-stats.md` (**BEH §n**).

**Tech Stack:** Python ≥3.12, uv + hatchling, Polars, deltalake (delta-rs), httpx, typer, python-dotenv, lxml/BeautifulSoup, fastexcel; pytest + ruff. Local MinIO available for `real_store` tests.

## Global Constraints

- Python ≥ 3.12; package name `bls-stats`, import `bls_stats`, src layout.
- **Polars only — no pandas anywhere.**
- String locks: `series_id`, `footnote_codes`, `area_fips`, and all code columns are `pl.Utf8` (leading zeros preserved).
- `ref_date` is `pl.Date`; timestamps are `pl.Datetime("us", "UTC")`; never call `datetime.now()` inside pipeline logic — clocks are injected.
- **The employer is never named** in any file, comment, doc, or test. Say "S3-compatible object store".
- Secrets live in `.project.env` (already gitignored). Tests never require real secrets outside `real_store` marks.
- All BLS/Census HTTP goes through `core/http.py`: UA `bls-stats/<version> (<contact email>)`, 4xx fail fast, 5xx/transport retry with backoff, throttled scrapes.
- Default pytest run is offline: `addopts = -m "not network and not slow"`; `real_store` tests self-skip without `AWS_ENDPOINT_URL`.
- Peak RSS design target < 8 GB: flat files parsed via lazy/streaming scans; QCEW strictly one year at a time.
- Dependency rule (ARCH §3): `cli → pipeline → {engines, releases, vintage, storage, enrich} → core → registry`. Lower layers never import higher ones.
- Commit after every task (conventional commits, Co-Authored-By trailer per repo convention).

**Recorded deviations from ARCH §3 layout** (approved rationale inline):
- `core/config.py` added (env/dotenv loading needs one home; registry stays pure data).
- `storage/reads.py` added for the three canonical read helpers (ARCH §4.4).
- `storage/s3_parquet.py` (escape-hatch backend) is **deferred — YAGNI**; the `backend.py` protocol boundary is still built, which is what makes the swap cheap later.
- Extra deps: `boto3` (doctor's conditional-PUT probe needs raw signed S3 calls), `xlsxwriter` (dev-only, builds the OEWS test fixture).

## File Structure

```
pyproject.toml                     # Task 1
src/bls_stats/__init__.py          # Task 1
src/bls_stats/registry.py          # Task 3 — ProgramSpec/RevisionProfile + 8-program data
src/bls_stats/core/config.py       # Task 2 — Settings from env/.project.env
src/bls_stats/core/periods.py      # Task 4 — reference_periods, ref_date rules
src/bls_stats/core/series_id.py    # Task 5 — fixed-width codec
src/bls_stats/core/http.py         # Task 6 — the one HTTP client
src/bls_stats/storage/backend.py   # Task 7 — protocol
src/bls_stats/storage/delta.py     # Task 7 — VintageStore (Delta)
src/bls_stats/storage/reads.py     # Task 8 — latest / as_of / prints
src/bls_stats/storage/doctor.py    # Task 9 — environment probes
src/bls_stats/releases/feeds.py    # Task 10 — Atom → Release events
src/bls_stats/releases/calendar.py # Task 11 — scrape/poll calendar, gaps, filter_published
src/bls_stats/vintage/ledger.py    # Task 12 — slot ledger, statuses, benchmark counts
src/bls_stats/releases/profiles.py # Task 13 — expand(): slots + benchmark windows
src/bls_stats/engines/labstat.py   # Task 14 — 5 flat-file programs
src/bls_stats/engines/qcew.py      # Task 15
src/bls_stats/engines/oews.py      # Task 16
src/bls_stats/engines/ep.py        # Task 17
src/bls_stats/engines/api_v2.py    # Task 18 — utility engine
src/bls_stats/enrich/cps.py        # Task 19 — CPS metadata (BEH §2.5)
src/bls_stats/pipeline.py          # Task 20 — detect→expand→fetch→validate→commit→record
src/bls_stats/cli.py               # Task 21 — typer app
scripts/capture_fixtures.py        # Task 10/14 — network-marked fixture capture
tests/…                            # per task; fixtures under tests/fixtures/
```

Milestones: **A** Foundation (1–6) · **B** Storage (7–9) · **C** Release intelligence (10–13) · **D** Engines (14–18) · **E** Assembly (19–22). Each milestone ends with a checkpoint: full default suite green + ruff clean.

---

### Task 1: Project scaffold

**Files:**
- Create: `pyproject.toml`, `src/bls_stats/__init__.py`, `src/bls_stats/py.typed`, subpackage `__init__.py` files (`core/`, `engines/`, `releases/`, `vintage/`, `storage/`, `enrich/`), `tests/test_smoke.py`

**Interfaces:**
- Produces: importable `bls_stats` package with `__version__`; pytest marker config every later task relies on; `uv run` workflow.

- [ ] **Step 1: Write pyproject.toml**

```toml
[project]
name = "bls-stats"
version = "0.1.0"
description = "Vintage-aware BLS data downloads and S3-compatible object-store ingest"
requires-python = ">=3.12"
dependencies = [
    "polars>=1.0",
    "deltalake>=0.19",
    "httpx>=0.27",
    "typer>=0.12",
    "python-dotenv>=1.0",
    "lxml>=5.0",
    "beautifulsoup4>=4.12",
    "fastexcel>=0.11",
    "boto3>=1.35",
]

[dependency-groups]
dev = ["pytest>=8.0", "ruff>=0.6", "xlsxwriter>=3.2"]

[project.scripts]
bls-stats = "bls_stats.cli:app"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/bls_stats"]

[tool.ruff]
line-length = 100
target-version = "py312"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B"]

[tool.pytest.ini_options]
addopts = "-m 'not network and not slow'"
testpaths = ["tests"]
markers = [
    "network: hits live BLS/Census servers (nightly/manual only)",
    "slow: full-size files or long-running",
    "real_store: needs a live S3/MinIO endpoint (self-skips without AWS_ENDPOINT_URL)",
]
```

- [ ] **Step 2: Create package skeleton**

`src/bls_stats/__init__.py`:

```python
__version__ = "0.1.0"
```

Create empty `__init__.py` in `src/bls_stats/{core,engines,releases,vintage,storage,enrich}/` and an empty `src/bls_stats/py.typed`. `src/bls_stats/cli.py` needs a placeholder so the script entry resolves:

```python
import typer

app = typer.Typer(help="Vintage-aware BLS data downloads and ingest.")


@app.callback()
def main() -> None:
    """bls-stats CLI."""
```

- [ ] **Step 3: Write smoke test** — `tests/test_smoke.py`:

```python
import bls_stats


def test_version() -> None:
    assert bls_stats.__version__ == "0.1.0"
```

- [ ] **Step 4: Sync and verify**

Run: `uv sync && uv run pytest -q && uv run ruff check . && uv run bls-stats --help`
Expected: 1 test passes; ruff clean; CLI help prints.

- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat: project scaffold (uv/hatchling, pytest markers, ruff, typer entry)"`

---

### Task 2: Settings (`core/config.py`)

**Files:**
- Create: `src/bls_stats/core/config.py`
- Test: `tests/core/test_config.py`

**Interfaces:**
- Produces: `Settings` frozen dataclass (`store_uri: str`, `contact_email: str`, `contact_email_is_default: bool`, `api_key: str | None`, `log_level: str`, `aws_endpoint_url: str | None`); `load_settings(env_file: str | Path = ".project.env") -> Settings`; `storage_options(s: Settings) -> dict[str, str]`. Every later task takes a `Settings`.

- [ ] **Step 1: Write the failing tests** — `tests/core/test_config.py`:

```python
from pathlib import Path

from bls_stats.core.config import Settings, load_settings, storage_options


def test_defaults_when_env_absent(monkeypatch, tmp_path: Path) -> None:
    for var in ("BLS_STORE_URI", "BLS_CONTACT_EMAIL", "BLS_API_KEY", "AWS_ENDPOINT_URL"):
        monkeypatch.delenv(var, raising=False)
    s = load_settings(env_file=tmp_path / "missing.env")
    assert s.store_uri == "./data/store"
    assert s.contact_email == "research@example.com"
    assert s.contact_email_is_default is True
    assert s.api_key is None


def test_reads_dotenv_file(monkeypatch, tmp_path: Path) -> None:
    for var in ("BLS_STORE_URI", "BLS_CONTACT_EMAIL"):
        monkeypatch.delenv(var, raising=False)
    env = tmp_path / ".project.env"
    env.write_text("BLS_STORE_URI=s3://bls-stats/store\nBLS_CONTACT_EMAIL=me@example.org\n")
    s = load_settings(env_file=env)
    assert s.store_uri == "s3://bls-stats/store"
    assert s.contact_email == "me@example.org"
    assert s.contact_email_is_default is False


def test_storage_options_http_endpoint() -> None:
    s = Settings(store_uri="s3://bls-stats/store", aws_endpoint_url="http://127.0.0.1:9000")
    opts = storage_options(s)
    assert opts["AWS_ENDPOINT_URL"] == "http://127.0.0.1:9000"
    assert opts["AWS_ALLOW_HTTP"] == "true"
    assert opts["aws_conditional_put"] == "etag"


def test_storage_options_unsafe_rename(monkeypatch) -> None:
    monkeypatch.setenv("BLS_S3_UNSAFE_RENAME", "true")
    opts = storage_options(
        Settings(store_uri="s3://bls-stats/store", aws_endpoint_url="https://s3.example.com")
    )
    assert opts["AWS_S3_ALLOW_UNSAFE_RENAME"] == "true"
    assert "aws_conditional_put" not in opts


def test_storage_options_empty_for_local_store() -> None:
    assert storage_options(Settings()) == {}  # local path: no S3 commit options
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/core/test_config.py -v`
Expected: FAIL — `ModuleNotFoundError: bls_stats.core.config`

- [ ] **Step 3: Implement** — `src/bls_stats/core/config.py`:

```python
"""Environment-driven settings (ARCH §10). Loaded from .project.env via python-dotenv."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

ENV_FILE = ".project.env"


@dataclass(frozen=True)
class Settings:
    store_uri: str = "./data/store"
    contact_email: str = "research@example.com"
    contact_email_is_default: bool = True
    api_key: str | None = None
    log_level: str = "INFO"
    aws_endpoint_url: str | None = None


def load_settings(env_file: str | Path = ENV_FILE) -> Settings:
    load_dotenv(env_file)  # silently a no-op when the file is absent
    email = os.getenv("BLS_CONTACT_EMAIL")
    return Settings(
        store_uri=os.getenv("BLS_STORE_URI", "./data/store"),
        contact_email=email or "research@example.com",
        contact_email_is_default=email is None,
        api_key=os.getenv("BLS_API_KEY"),
        log_level=os.getenv("BLS_LOG_LEVEL", "INFO"),
        aws_endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
    )


def storage_options(s: Settings) -> dict[str, str]:
    """delta-rs storage options. Commit-safety mode per ARCH §4.1: conditional PUT
    by default; BLS_S3_UNSAFE_RENAME=true switches to single-writer mode (doctor advises)."""
    opts: dict[str, str] = {}
    if not s.store_uri.startswith("s3://"):
        return opts  # local store: no S3 options (laptop-only convenience, ARCH §10)
    if s.aws_endpoint_url:
        opts["AWS_ENDPOINT_URL"] = s.aws_endpoint_url
        if s.aws_endpoint_url.startswith("http://"):
            opts["AWS_ALLOW_HTTP"] = "true"
    if os.getenv("BLS_S3_UNSAFE_RENAME", "").lower() == "true":
        opts["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"
    else:
        opts["aws_conditional_put"] = "etag"
    return opts
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/core/test_config.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat: env settings with .project.env loading and delta storage options"`

---

### Task 3: Program registry (`registry.py`)

**Files:**
- Create: `src/bls_stats/registry.py`
- Test: `tests/test_registry.py`

**Interfaces:**
- Produces (consumed by *every* later task):
  - `Frequency` (StrEnum: `MONTHLY|QUARTERLY|ANNUAL|NONE`), `RefDateRule` (StrEnum: `DAY_12|LAST_BUSINESS_DAY|QUARTER_END_12|MAY_12|NONE`)
  - `RevisionProfile(routine_slots: int, routine_rule: str, benchmark_rule: str | None, benchmark_window_years: int | None)` — `routine_rule` ∈ {`"fixed"`, `"year_to_date"`}; `benchmark_rule` ∈ {`"jan_data"`, `"q1_data"`, None}
  - `ProgramSpec(name, frequency, ref_date_rule, series_prefix, unit_columns, backfill_url, increment_url, benchmark_url, feed_url, archive_url, schedule_url, release_time_et, profile, row_band, null_rate_max)` — series-ID layouts live in the separate `SERIES_LAYOUTS` dict (prefix-keyed), not on the spec
  - `REGISTRY: dict[str, ProgramSpec]` with keys `ces sae jolts cps bed qcew oews ep`
  - `SERIES_LAYOUTS: dict[str, tuple[tuple[str, int], ...]]` keyed by 2-char prefix (`CE SM BD JT LN OE EP`)

- [ ] **Step 1: Write the failing tests** — `tests/test_registry.py`:

```python
from bls_stats.registry import REGISTRY, SERIES_LAYOUTS, Frequency

EXPECTED_LAYOUT_LEN = {"CE": 13, "SM": 20, "BD": 28, "JT": 21, "LN": 11, "OE": 26, "EP": 15}


def test_eight_programs() -> None:
    assert set(REGISTRY) == {"ces", "sae", "jolts", "cps", "bed", "qcew", "oews", "ep"}


def test_layout_widths_sum_to_documented_totals() -> None:
    for prefix, total in EXPECTED_LAYOUT_LEN.items():
        widths = sum(w for _, w in SERIES_LAYOUTS[prefix])
        assert widths == total, prefix


def test_periodic_programs_have_feeds_and_archives() -> None:
    for name, spec in REGISTRY.items():
        if name == "ep":
            assert spec.feed_url is None
            continue
        assert spec.feed_url and spec.feed_url.startswith("https://www.bls.gov/feed/")
        assert spec.archive_url


def test_ces_cps_share_empsit() -> None:
    assert REGISTRY["ces"].feed_url == REGISTRY["cps"].feed_url


def test_profiles_match_spec_table() -> None:  # ARCH §2.1/§2.2
    assert REGISTRY["ces"].profile.routine_slots == 3
    assert REGISTRY["ces"].profile.benchmark_window_years == 5
    assert REGISTRY["sae"].profile.routine_slots == 2
    assert REGISTRY["cps"].profile.routine_slots == 1
    assert REGISTRY["qcew"].profile.routine_rule == "year_to_date"
    assert REGISTRY["bed"].profile.benchmark_window_years == 2
    assert REGISTRY["oews"].profile.benchmark_rule is None


def test_unit_columns() -> None:  # ARCH §4.3
    assert REGISTRY["ces"].unit_columns == ("series_id",)
    assert REGISTRY["qcew"].unit_columns == (
        "area_fips", "own_code", "industry_code", "agglvl_code", "size_code",
    )
    assert REGISTRY["ep"].unit_columns == ("occupation_code", "industry_code")


def test_frequencies() -> None:
    assert REGISTRY["qcew"].frequency == Frequency.QUARTERLY
    assert REGISTRY["oews"].frequency == Frequency.ANNUAL
    assert REGISTRY["ep"].frequency == Frequency.NONE
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_registry.py -v` — Expected: FAIL (module missing).

- [ ] **Step 3: Implement** — `src/bls_stats/registry.py` (pure data; URLs verbatim from BEH §2/§5 and ARCH §6.2):

```python
"""The eight programs as data (ARCH §3): specs, source URLs, layouts, revision profiles."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Frequency(StrEnum):
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUAL = "annual"
    NONE = "none"


class RefDateRule(StrEnum):
    DAY_12 = "day_12"
    LAST_BUSINESS_DAY = "last_business_day"
    QUARTER_END_12 = "quarter_end_12"
    MAY_12 = "may_12"
    NONE = "none"


@dataclass(frozen=True)
class RevisionProfile:
    routine_slots: int
    routine_rule: str = "fixed"  # "fixed" | "year_to_date" (QCEW, ARCH §6.2)
    benchmark_rule: str | None = None  # "jan_data" | "q1_data" | None (ARCH §5.3)
    benchmark_window_years: int | None = None  # ARCH §2.2 defaults; verify per §12


@dataclass(frozen=True)
class ProgramSpec:
    name: str
    frequency: Frequency
    ref_date_rule: RefDateRule
    series_prefix: str | None
    unit_columns: tuple[str, ...]
    backfill_url: str | None
    increment_url: str | None
    benchmark_url: str | None
    feed_url: str | None
    archive_url: str | None
    schedule_url: str | None
    release_time_et: str | None  # "08:30" | "10:00"
    profile: RevisionProfile
    row_band: float = 0.20  # ARCH §7.3 sanity band
    null_rate_max: float = 0.05


# Fixed-width series-ID layouts (BEH §2.1 table; verify against <prefix>.series docs).
SERIES_LAYOUTS: dict[str, tuple[tuple[str, int], ...]] = {
    "CE": (("prefix", 2), ("seasonal", 1), ("supersector", 2), ("industry", 6), ("data_type", 2)),
    "SM": (("prefix", 2), ("seasonal", 1), ("state", 2), ("area", 5), ("supersector", 2),
           ("industry", 6), ("data_type", 2)),
    "BD": (("prefix", 2), ("seasonal", 1), ("area_code", 10), ("industry_code", 6),
           ("unit_analysis", 1), ("data_element", 1), ("size_class", 2), ("data_class", 2),
           ("rate_level", 1), ("record_type", 1), ("ownership", 1)),
    "JT": (("prefix", 2), ("seasonal", 1), ("industry_code", 6), ("state_code", 2),
           ("area_code", 5), ("size_class", 2), ("data_element", 2), ("rate_level", 1)),
    "LN": (("prefix", 2), ("seasonal", 1), ("series_code", 8)),
    "OE": (("prefix", 2), ("seasonal", 1), ("state_code", 2), ("area_code", 7),
           ("industry_code", 6), ("occupation_code", 6), ("datatype_code", 2)),
    "EP": (("prefix", 2), ("seasonal", 1), ("occupation_code", 6), ("industry_code", 6)),
}

_TS = "https://download.bls.gov/pub/time.series"

REGISTRY: dict[str, ProgramSpec] = {
    "ces": ProgramSpec(
        name="ces", frequency=Frequency.MONTHLY, ref_date_rule=RefDateRule.DAY_12,
        series_prefix="CE", unit_columns=("series_id",),
        backfill_url=f"{_TS}/ce/ce.data.0.AllCESSeries",
        increment_url=f"{_TS}/ce/ce.data.0.AllCESSeries",  # no .Current exists (ARCH §6.2)
        benchmark_url=f"{_TS}/ce/ce.data.0.AllCESSeries",
        feed_url="https://www.bls.gov/feed/empsit.rss",
        archive_url="https://www.bls.gov/bls/news-release/empsit.htm",
        schedule_url="https://www.bls.gov/schedule/news_release/empsit.htm",
        release_time_et="08:30",
        profile=RevisionProfile(3, "fixed", "jan_data", 5),
    ),
    "sae": ProgramSpec(
        name="sae", frequency=Frequency.MONTHLY, ref_date_rule=RefDateRule.DAY_12,
        series_prefix="SM", unit_columns=("series_id",),
        backfill_url=f"{_TS}/sm/sm.data.1.AllData",
        increment_url=f"{_TS}/sm/sm.data.0.Current",
        benchmark_url=f"{_TS}/sm/sm.data.0.Current",  # window ⊂ .Current coverage
        feed_url="https://www.bls.gov/feed/laus.rss",
        archive_url="https://www.bls.gov/bls/news-release/laus.htm",
        schedule_url="https://www.bls.gov/schedule/news_release/laus.htm",
        release_time_et="10:00",
        profile=RevisionProfile(2, "fixed", "jan_data", 5),
    ),
    "jolts": ProgramSpec(
        name="jolts", frequency=Frequency.MONTHLY, ref_date_rule=RefDateRule.LAST_BUSINESS_DAY,
        series_prefix="JT", unit_columns=("series_id",),
        backfill_url=f"{_TS}/jt/jt.data.1.AllItems",
        increment_url=f"{_TS}/jt/jt.data.0.Current",
        benchmark_url=f"{_TS}/jt/jt.data.0.Current",
        feed_url="https://www.bls.gov/feed/jolts.rss",
        archive_url="https://www.bls.gov/bls/news-release/jolts.htm",
        schedule_url="https://www.bls.gov/schedule/news_release/jolts.htm",
        release_time_et="10:00",
        profile=RevisionProfile(2, "fixed", "jan_data", 5),
    ),
    "cps": ProgramSpec(
        name="cps", frequency=Frequency.MONTHLY, ref_date_rule=RefDateRule.DAY_12,
        series_prefix="LN", unit_columns=("series_id",),
        backfill_url=f"{_TS}/ln/ln.data.1.AllData",
        increment_url=f"{_TS}/ln/ln.data.1.AllData",  # no .Current exists
        benchmark_url=f"{_TS}/ln/ln.data.1.AllData",
        feed_url="https://www.bls.gov/feed/empsit.rss",
        archive_url="https://www.bls.gov/bls/news-release/empsit.htm",
        schedule_url="https://www.bls.gov/schedule/news_release/empsit.htm",
        release_time_et="08:30",
        profile=RevisionProfile(1, "fixed", "jan_data", 5),
    ),
    "bed": ProgramSpec(
        name="bed", frequency=Frequency.QUARTERLY, ref_date_rule=RefDateRule.QUARTER_END_12,
        series_prefix="BD", unit_columns=("series_id",),
        backfill_url=f"{_TS}/bd/bd.data.1.AllItems",
        increment_url=f"{_TS}/bd/bd.data.0.Current",
        benchmark_url=f"{_TS}/bd/bd.data.0.Current",
        feed_url="https://www.bls.gov/feed/cewbd.rss",
        archive_url="https://www.bls.gov/bls/news-release/cewbd.htm",
        schedule_url="https://www.bls.gov/schedule/news_release/cewbd.htm",
        release_time_et="10:00",
        profile=RevisionProfile(1, "fixed", "q1_data", 2),
    ),
    "qcew": ProgramSpec(
        name="qcew", frequency=Frequency.QUARTERLY, ref_date_rule=RefDateRule.QUARTER_END_12,
        series_prefix=None,
        unit_columns=("area_fips", "own_code", "industry_code", "agglvl_code", "size_code"),
        backfill_url="https://data.bls.gov/cew/data/files/{year}/csv/{year}_qtrly_singlefile.zip",
        increment_url="https://data.bls.gov/cew/data/files/{year}/csv/{year}_qtrly_singlefile.zip",
        benchmark_url="https://data.bls.gov/cew/data/files/{year}/csv/{year}_qtrly_singlefile.zip",
        feed_url="https://www.bls.gov/feed/cewqtr.rss",
        archive_url="https://www.bls.gov/bls/news-release/cewqtr.htm",
        schedule_url=None,  # 404s (BEH §5) — tolerated per ARCH §5.4
        release_time_et="10:00",
        profile=RevisionProfile(1, "year_to_date", "q1_data", 1),
    ),
    "oews": ProgramSpec(
        name="oews", frequency=Frequency.ANNUAL, ref_date_rule=RefDateRule.MAY_12,
        series_prefix="OE", unit_columns=("area", "occ_code"),
        backfill_url="https://www.bls.gov/oes/special-requests/oesm{yy}all.zip",
        increment_url="https://www.bls.gov/oes/special-requests/oesm{yy}all.zip",
        benchmark_url=None,
        feed_url="https://www.bls.gov/feed/ocwage.rss",
        archive_url="https://www.bls.gov/oes/release_archive.htm",
        schedule_url=None,
        release_time_et="10:00",
        profile=RevisionProfile(1, "fixed", None, None),
    ),
    "ep": ProgramSpec(
        name="ep", frequency=Frequency.NONE, ref_date_rule=RefDateRule.NONE,
        series_prefix="EP", unit_columns=("occupation_code", "industry_code"),
        backfill_url="https://www.bls.gov/emp/tables/industry-occupation-matrix-occupation.htm",
        increment_url="https://data.bls.gov/projections/nationalMatrix?queryParams={soc}&ioType=o",
        benchmark_url=None,
        feed_url=None, archive_url=None, schedule_url=None, release_time_et=None,  # ARCH §5.2 exception
        profile=RevisionProfile(1, "fixed", None, None),
    ),
}
```

- [ ] **Step 4: Run to verify pass** — `uv run pytest tests/test_registry.py -v` — Expected: 7 passed.

- [ ] **Step 5: Commit** — `git commit -am "feat: program registry with specs, layouts, and revision profiles"`

---

### Task 4: Period math (`core/periods.py`)

**Files:**
- Create: `src/bls_stats/core/periods.py`
- Test: `tests/core/test_periods.py`

**Interfaces:**
- Consumes: `REGISTRY`, `Frequency`, `RefDateRule` (Task 3).
- Produces: `Period = tuple[int, int]`; `reference_periods(program: str, start: str, end: str) -> list[Period]`; `ref_date(program: str, year: int, period: int) -> date | None` (None only for `ep`); `shift(program: str, year: int, period: int, by: int) -> Period`; `PeriodError(ValueError)`.

- [ ] **Step 1: Write the failing tests** — `tests/core/test_periods.py`:

```python
from datetime import date

import pytest

from bls_stats.core.periods import PeriodError, ref_date, reference_periods, shift


def test_monthly_inclusive_range() -> None:
    assert reference_periods("ces", "2025/11", "2026/02") == [
        (2025, 11), (2025, 12), (2026, 1), (2026, 2),
    ]


def test_quarterly_and_annual_parsing() -> None:
    assert reference_periods("qcew", "2024/03", "2025/01") == [(2024, 3), (2024, 4), (2025, 1)]
    assert reference_periods("oews", "2022", "2024") == [(2022, 1), (2023, 1), (2024, 1)]


@pytest.mark.parametrize(
    ("program", "start", "end"),
    [("ces", "2026/13", "2026/13"), ("qcew", "2026/5", "2026/5"),
     ("ces", "2026/03", "2026/01"), ("nope", "2026/01", "2026/01")],
)
def test_invalid_inputs_raise(program: str, start: str, end: str) -> None:
    with pytest.raises(PeriodError):
        reference_periods(program, start, end)


def test_ref_date_rules() -> None:  # BEH §4
    assert ref_date("ces", 2026, 6) == date(2026, 6, 12)
    assert ref_date("jolts", 2026, 2) == date(2026, 2, 27)  # Feb 28 2026 is a Saturday
    assert ref_date("jolts", 2026, 5) == date(2026, 5, 29)  # May 31 2026 is a Sunday
    assert ref_date("qcew", 2026, 1) == date(2026, 3, 12)
    assert ref_date("bed", 2025, 4) == date(2025, 12, 12)
    assert ref_date("oews", 2025, 1) == date(2025, 5, 12)
    assert ref_date("ep", 2026, 1) is None


def test_shift_monthly_across_year() -> None:
    assert shift("ces", 2026, 1, -2) == (2025, 11)
    assert shift("qcew", 2026, 1, -1) == (2025, 4)
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/core/test_periods.py -v` — Expected: FAIL.

- [ ] **Step 3: Implement** — `src/bls_stats/core/periods.py`:

```python
"""Reference-period generation and canonical ref_date rules (BEH §3, §4)."""

from __future__ import annotations

import re
from datetime import date, timedelta

from bls_stats.registry import REGISTRY, Frequency, RefDateRule

Period = tuple[int, int]

_QUARTER_END_MONTH = {1: 3, 2: 6, 3: 9, 4: 12}


class PeriodError(ValueError):
    pass


def _spec(program: str):
    try:
        return REGISTRY[program]
    except KeyError:
        raise PeriodError(f"unknown program: {program!r}") from None


def _parse(program: str, text: str) -> Period:
    freq = _spec(program).frequency
    if freq == Frequency.MONTHLY:
        m = re.fullmatch(r"(\d{4})/(\d{1,2})", text)
        if not m or not 1 <= int(m.group(2)) <= 12:
            raise PeriodError(f"{program}: expected YYYY/MM (01-12), got {text!r}")
        return int(m.group(1)), int(m.group(2))
    if freq == Frequency.QUARTERLY:
        m = re.fullmatch(r"(\d{4})/0?([1-4])", text)
        if not m:
            raise PeriodError(f"{program}: expected YYYY/Q (1-4), got {text!r}")
        return int(m.group(1)), int(m.group(2))
    m = re.fullmatch(r"\d{4}", text)  # ANNUAL and NONE take plain years
    if not m:
        raise PeriodError(f"{program}: expected YYYY, got {text!r}")
    return int(text), 1


def _per_year(program: str) -> int:
    return {Frequency.MONTHLY: 12, Frequency.QUARTERLY: 4}.get(_spec(program).frequency, 1)


def _to_index(period: Period, n: int) -> int:
    return period[0] * n + (period[1] - 1)


def _from_index(idx: int, n: int) -> Period:
    return idx // n, idx % n + 1


def reference_periods(program: str, start: str, end: str) -> list[Period]:
    lo, hi = _parse(program, start), _parse(program, end)
    n = _per_year(program)
    a, b = _to_index(lo, n), _to_index(hi, n)
    if a > b:
        raise PeriodError(f"start {start!r} is after end {end!r}")
    return [_from_index(i, n) for i in range(a, b + 1)]


def shift(program: str, year: int, period: int, by: int) -> Period:
    n = _per_year(program)
    return _from_index(_to_index((year, period), n) + by, n)


def last_business_day(year: int, month: int) -> date:
    nxt = date(year + (month == 12), month % 12 + 1, 1)
    d = nxt - timedelta(days=1)
    while d.weekday() >= 5:  # Sat/Sun
        d -= timedelta(days=1)
    return d


def ref_date(program: str, year: int, period: int) -> date | None:
    rule = _spec(program).ref_date_rule
    if rule == RefDateRule.DAY_12:
        return date(year, period, 12)
    if rule == RefDateRule.LAST_BUSINESS_DAY:
        return last_business_day(year, period)
    if rule == RefDateRule.QUARTER_END_12:
        return date(year, _QUARTER_END_MONTH[period], 12)
    if rule == RefDateRule.MAY_12:
        return date(year, 5, 12)
    return None  # ep (ARCH §4.3)
```

- [ ] **Step 4: Run to verify pass** — `uv run pytest tests/core/test_periods.py -v` — Expected: all pass.

- [ ] **Step 5: Commit** — `git commit -am "feat: reference periods and canonical ref_date rules"`

---

### Task 5: Series-ID codec (`core/series_id.py`)

**Files:**
- Create: `src/bls_stats/core/series_id.py`
- Test: `tests/core/test_series_id.py`

**Interfaces:**
- Consumes: `SERIES_LAYOUTS` (Task 3).
- Produces: `decode(series_id: str) -> dict[str, str]` (layout picked by 2-char prefix; raises `SeriesIdError` on unknown prefix or wrong length).

- [ ] **Step 1: Write the failing tests** — `tests/core/test_series_id.py`:

```python
import pytest

from bls_stats.core.series_id import SeriesIdError, decode


def test_decode_ces() -> None:
    parts = decode("CES0500000003")
    assert parts == {
        "prefix": "CE", "seasonal": "S", "supersector": "05",
        "industry": "000000", "data_type": "03",
    }


def test_decode_cps() -> None:
    assert decode("LNS14000000") == {"prefix": "LN", "seasonal": "S", "series_code": "14000000"}


def test_decode_jolts_length_21() -> None:
    parts = decode("JTS000000000000000HIL")
    assert parts["data_element"] == "HI"
    assert parts["rate_level"] == "L"


def test_whitespace_tolerated() -> None:
    assert decode(" LNS14000000 ")["series_code"] == "14000000"


@pytest.mark.parametrize("bad", ["XX123", "CES05000000", ""])  # unknown prefix / wrong length
def test_errors(bad: str) -> None:
    with pytest.raises(SeriesIdError):
        decode(bad)
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/core/test_series_id.py -v` — Expected: FAIL.

- [ ] **Step 3: Implement** — `src/bls_stats/core/series_id.py`:

```python
"""Fixed-width series-ID decoding (BEH §2.1 layouts, exposed as a registry)."""

from __future__ import annotations

from bls_stats.registry import SERIES_LAYOUTS


class SeriesIdError(ValueError):
    pass


def decode(series_id: str) -> dict[str, str]:
    sid = series_id.strip()
    layout = SERIES_LAYOUTS.get(sid[:2])
    if layout is None:
        raise SeriesIdError(f"unknown series prefix: {sid[:2]!r}")
    total = sum(w for _, w in layout)
    if len(sid) != total:
        raise SeriesIdError(f"{sid!r}: expected length {total}, got {len(sid)}")
    out: dict[str, str] = {}
    pos = 0
    for field, width in layout:
        out[field] = sid[pos : pos + width]
        pos += width
    return out
```

- [ ] **Step 4: Run to verify pass** — `uv run pytest tests/core/test_series_id.py -v`

- [ ] **Step 5: Commit** — `git commit -am "feat: fixed-width series-id codec"`

---

### Task 6: HTTP client (`core/http.py`)

**Files:**
- Create: `src/bls_stats/core/http.py`
- Test: `tests/core/test_http.py`

**Interfaces:**
- Consumes: `Settings` (Task 2), `bls_stats.__version__`.
- Produces (all engines/releases consume these):
  - `build_client(settings: Settings, timeout: float = 300.0) -> httpx.Client` — sets the UA header
  - `get(client, url, *, retries: int = 3, backoff: float = 2.0) -> httpx.Response` — 4xx raises `httpx.HTTPStatusError` immediately; 5xx/transport retried with exponential backoff (injectable `sleep` for tests)
  - `download(client, url, dest: Path) -> Path` — streaming download to file
  - `head_last_modified(client, url) -> datetime | None` — parsed, tz-aware UTC
  - `Throttle(seconds: float)` with `.wait()` (injectable clock/sleep)

- [ ] **Step 1: Write the failing tests** — `tests/core/test_http.py`:

```python
import httpx
import pytest

from bls_stats.core.config import Settings
from bls_stats.core.http import Throttle, build_client, get, head_last_modified


def _client_with(handler) -> httpx.Client:
    c = build_client(Settings(contact_email="me@example.org"))
    c._transport = httpx.MockTransport(handler)
    return c


def test_user_agent_includes_contact() -> None:
    c = build_client(Settings(contact_email="me@example.org"))
    assert "me@example.org" in c.headers["User-Agent"]
    assert c.headers["User-Agent"].startswith("bls-stats/")


def test_5xx_retries_then_succeeds() -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(500 if len(calls) < 3 else 200, text="ok")

    resp = get(_client_with(handler), "https://example.com/x", sleep=lambda _s: None)
    assert resp.status_code == 200
    assert len(calls) == 3


def test_4xx_fails_fast() -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(404)

    with pytest.raises(httpx.HTTPStatusError):
        get(_client_with(handler), "https://example.com/x", sleep=lambda _s: None)
    assert len(calls) == 1


def test_5xx_exhausts_retries() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    with pytest.raises(httpx.HTTPStatusError):
        get(_client_with(handler), "https://example.com/x", retries=2, sleep=lambda _s: None)


def test_head_last_modified_parses_to_utc() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "HEAD"
        return httpx.Response(200, headers={"Last-Modified": "Thu, 02 Jul 2026 12:30:00 GMT"})

    lm = head_last_modified(_client_with(handler), "https://example.com/x")
    assert lm is not None and lm.isoformat() == "2026-07-02T12:30:00+00:00"


def test_throttle_waits_only_when_needed() -> None:
    now = {"t": 0.0}
    slept: list[float] = []
    th = Throttle(2.0, clock=lambda: now["t"], sleep=lambda s: slept.append(s))
    th.wait()          # first call: no sleep
    now["t"] = 0.5
    th.wait()          # 1.5s remaining
    assert slept == [1.5]
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/core/test_http.py -v` — Expected: FAIL.

- [ ] **Step 3: Implement** — `src/bls_stats/core/http.py`:

```python
"""The one HTTP client (ARCH §10): descriptive UA, 4xx fast-fail, 5xx backoff, throttle."""

from __future__ import annotations

import logging
import time as _time
from collections.abc import Callable
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import httpx

import bls_stats
from bls_stats.core.config import Settings

log = logging.getLogger(__name__)


def build_client(settings: Settings, timeout: float = 300.0) -> httpx.Client:
    ua = f"bls-stats/{bls_stats.__version__} ({settings.contact_email})"
    return httpx.Client(headers={"User-Agent": ua}, timeout=timeout, follow_redirects=True)


def get(
    client: httpx.Client,
    url: str,
    *,
    retries: int = 3,
    backoff: float = 2.0,
    method: str = "GET",
    sleep: Callable[[float], None] = _time.sleep,
) -> httpx.Response:
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            resp = client.request(method, url)
            if resp.status_code >= 500:
                resp.raise_for_status()
            resp.raise_for_status()  # 4xx raises here too — but without retry (below)
            return resp
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code < 500:
                log.error("HTTP %s for %s — failing fast", exc.response.status_code, url)
                raise
            last_exc = exc
        except httpx.TransportError as exc:
            last_exc = exc
        if attempt < retries:
            delay = backoff * 2**attempt
            log.warning("retry %d/%d for %s in %.1fs (%s)", attempt + 1, retries, url, delay, last_exc)
            sleep(delay)
    assert last_exc is not None
    raise last_exc


def download(client: httpx.Client, url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with client.stream("GET", url) as resp:
        resp.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in resp.iter_bytes():
                fh.write(chunk)
    return dest


def head_last_modified(client: httpx.Client, url: str) -> datetime | None:
    resp = get(client, url, method="HEAD")
    value = resp.headers.get("Last-Modified")
    if value is None:
        return None
    return parsedate_to_datetime(value).astimezone(timezone.utc)


class Throttle:
    def __init__(
        self,
        seconds: float,
        clock: Callable[[], float] = _time.monotonic,
        sleep: Callable[[float], None] = _time.sleep,
    ) -> None:
        self.seconds, self._clock, self._sleep = seconds, clock, sleep
        self._last: float | None = None

    def wait(self) -> None:
        if self._last is not None:
            remaining = self.seconds - (self._clock() - self._last)
            if remaining > 0:
                self._sleep(remaining)
        self._last = self._clock()
```

- [ ] **Step 4: Run to verify pass** — `uv run pytest tests/core/test_http.py -v`

- [ ] **Step 5: Milestone A checkpoint + commit**

Run: `uv run pytest -q && uv run ruff check .`
Expected: all green. Then: `git commit -am "feat: retrying/throttled http client with descriptive UA"`

---

### Task 7: Delta vintage store (`storage/backend.py`, `storage/delta.py`)

**Files:**
- Create: `src/bls_stats/storage/backend.py`, `src/bls_stats/storage/delta.py`
- Test: `tests/storage/test_delta.py`

**Interfaces:**
- Consumes: `Settings.store_uri`, `storage_options()` (Task 2).
- Produces (pipeline/ledger/CLI consume these — exact signatures):
  - `VINTAGE_COLUMNS: dict[str, pl.DataType]` = `{"ref_date": pl.Date, "release_date": pl.Date, "revision": pl.Int16, "benchmark": pl.Int16, "source": pl.Utf8, "downloaded": pl.Datetime("us", "UTC")}`
  - `backend.py`: `class Store(Protocol)` with the methods below (the ARCH §4.1 swap boundary)
  - `delta.py`: `class VintageStore:`
    - `__init__(self, uri: str, storage_options: dict[str, str] | None = None)`
    - `observations_uri(self, program: str) -> str` → `{uri}/{program}/observations`
    - `append_observations(self, program: str, df: pl.DataFrame) -> None` — validates VINTAGE_COLUMNS dtypes, Delta append partitioned by `release_date`
    - `scan_observations(self, program: str) -> pl.LazyFrame | None` — None when table absent
    - `slot_exists(self, program, ref_date, release_date, revision, benchmark) -> bool` — **null-safe** (ARCH §7.2)
    - `append_state(self, table: str, df: pl.DataFrame) -> None` / `read_state(self, table: str) -> pl.DataFrame | None` — `{uri}/state/{table}`

- [ ] **Step 1: Write the failing tests** — `tests/storage/test_delta.py`:

```python
from datetime import date, datetime, timezone

import polars as pl
import pytest

from bls_stats.storage.delta import VINTAGE_COLUMNS, VintageStore


def obs_frame(ref: date, rel: date, revision: int | None, benchmark: int | None,
              source: str = "increment", value: float = 1.0) -> pl.DataFrame:
    return pl.DataFrame({
        "series_id": ["CES0000000001"], "value": [value], "footnote_codes": [""],
        "ref_date": [ref], "release_date": [rel],
        "revision": pl.Series([revision], dtype=pl.Int16),
        "benchmark": pl.Series([benchmark], dtype=pl.Int16),
        "source": [source],
        "downloaded": [datetime(2026, 7, 2, 13, 0, tzinfo=timezone.utc)],
    })


@pytest.fixture()
def store(tmp_path) -> VintageStore:
    return VintageStore(str(tmp_path / "store"))


def test_append_and_scan_roundtrip(store: VintageStore) -> None:
    store.append_observations("ces", obs_frame(date(2026, 6, 12), date(2026, 7, 2), 0, 0))
    lf = store.scan_observations("ces")
    assert lf is not None
    out = lf.collect()
    assert out.height == 1
    assert out.schema["ref_date"] == pl.Date
    assert out.schema["revision"] == pl.Int16


def test_scan_missing_table_returns_none(store: VintageStore) -> None:
    assert store.scan_observations("jolts") is None


def test_append_rejects_missing_vintage_columns(store: VintageStore) -> None:
    with pytest.raises(ValueError, match="release_date"):
        store.append_observations("ces", pl.DataFrame({"series_id": ["x"]}))


def test_slot_exists_null_safe(store: VintageStore) -> None:
    store.append_observations(
        "ces", obs_frame(date(2020, 1, 12), date(2026, 7, 1), None, None, source="backfill")
    )
    assert store.slot_exists("ces", date(2020, 1, 12), date(2026, 7, 1), None, None) is True
    assert store.slot_exists("ces", date(2020, 1, 12), date(2026, 7, 1), 0, None) is False
    assert store.slot_exists("ces", date(2020, 2, 12), date(2026, 7, 1), None, None) is False


def test_state_roundtrip_and_append(store: VintageStore) -> None:
    row = pl.DataFrame({"program": ["ces"], "note": ["a"]})
    store.append_state("ledger", row)
    store.append_state("ledger", row.with_columns(pl.lit("b").alias("note")))
    out = store.read_state("ledger")
    assert out is not None and out.height == 2
    assert store.read_state("nope") is None
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/storage/test_delta.py -v` — Expected: FAIL.

- [ ] **Step 3: Implement**

`src/bls_stats/storage/backend.py`:

```python
"""Storage protocol — the ARCH §4.1 backend swap boundary."""

from __future__ import annotations

from datetime import date
from typing import Protocol

import polars as pl


class Store(Protocol):
    def observations_uri(self, program: str) -> str: ...
    def append_observations(self, program: str, df: pl.DataFrame) -> None: ...
    def scan_observations(self, program: str) -> pl.LazyFrame | None: ...
    def slot_exists(
        self, program: str, ref_date: date | None, release_date: date,
        revision: int | None, benchmark: int | None,
    ) -> bool: ...
    def append_state(self, table: str, df: pl.DataFrame) -> None: ...
    def read_state(self, table: str) -> pl.DataFrame | None: ...
```

`src/bls_stats/storage/delta.py`:

```python
"""Delta Lake vintage store (ARCH §4). One Delta table per program, partitioned by release_date."""

from __future__ import annotations

from datetime import date

import polars as pl
from deltalake.exceptions import TableNotFoundError

VINTAGE_COLUMNS: dict[str, pl.DataType] = {
    "ref_date": pl.Date,
    "release_date": pl.Date,
    "revision": pl.Int16,
    "benchmark": pl.Int16,
    "source": pl.Utf8,
    "downloaded": pl.Datetime("us", "UTC"),
}


def _eq_missing(col: str, value) -> pl.Expr:
    return pl.col(col).eq_missing(pl.lit(value))


class VintageStore:
    def __init__(self, uri: str, storage_options: dict[str, str] | None = None) -> None:
        self.uri = uri.rstrip("/")
        self.storage_options = storage_options or None

    # -- observations ---------------------------------------------------
    def observations_uri(self, program: str) -> str:
        return f"{self.uri}/{program}/observations"

    def append_observations(self, program: str, df: pl.DataFrame) -> None:
        missing = [c for c in VINTAGE_COLUMNS if c not in df.columns]
        if missing:
            raise ValueError(f"frame missing vintage columns: {missing}")
        for col, dtype in VINTAGE_COLUMNS.items():
            if df.schema[col] != dtype:
                raise ValueError(f"{col}: expected {dtype}, got {df.schema[col]}")
        df.write_delta(
            self.observations_uri(program),
            mode="append",
            storage_options=self.storage_options,
            delta_write_options={"partition_by": ["release_date"]},
        )

    def scan_observations(self, program: str) -> pl.LazyFrame | None:
        return self._scan(self.observations_uri(program))

    def slot_exists(
        self, program: str, ref_date: date | None, release_date: date,
        revision: int | None, benchmark: int | None,
    ) -> bool:
        lf = self.scan_observations(program)
        if lf is None:
            return False
        hit = (
            lf.filter(
                _eq_missing("ref_date", ref_date)
                & (pl.col("release_date") == release_date)
                & _eq_missing("revision", revision)
                & _eq_missing("benchmark", benchmark)
            )
            .head(1)
            .collect()
        )
        return hit.height > 0

    # -- state tables ----------------------------------------------------
    def append_state(self, table: str, df: pl.DataFrame) -> None:
        df.write_delta(
            f"{self.uri}/state/{table}", mode="append", storage_options=self.storage_options
        )

    def read_state(self, table: str) -> pl.DataFrame | None:
        lf = self._scan(f"{self.uri}/state/{table}")
        return None if lf is None else lf.collect()

    def _scan(self, uri: str) -> pl.LazyFrame | None:
        try:
            return pl.scan_delta(uri, storage_options=self.storage_options)
        except (TableNotFoundError, FileNotFoundError, OSError):
            return None
```

Note: `pl.scan_delta` on a nonexistent local path raises differently across versions — the broad `except` is deliberate; the test locks the behavior.

- [ ] **Step 4: Run to verify pass** — `uv run pytest tests/storage/test_delta.py -v`

- [ ] **Step 5: Add a real_store smoke test** (appended to the same file) and commit:

```python
import os


@pytest.mark.real_store
def test_minio_roundtrip() -> None:
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    if not endpoint:
        pytest.skip("no AWS_ENDPOINT_URL configured")
    from bls_stats.core.config import Settings, storage_options as so

    store = VintageStore(
        "s3://bls-stats/test-store",
        so(Settings(store_uri="s3://bls-stats/test-store", aws_endpoint_url=endpoint)),
    )
    store.append_observations("ces", obs_frame(date(2026, 6, 12), date(2026, 7, 2), 0, 0))
    assert store.slot_exists("ces", date(2026, 6, 12), date(2026, 7, 2), 0, 0)
```

Run: `uv run pytest tests/storage -v` (real_store test skips without endpoint).
Commit: `git commit -am "feat: delta vintage store with null-safe slot presence and state tables"`

---

### Task 8: Canonical reads (`storage/reads.py`)

**Files:**
- Create: `src/bls_stats/storage/reads.py`
- Test: `tests/storage/test_reads.py`

**Interfaces:**
- Consumes: LazyFrames from `VintageStore.scan_observations`.
- Produces (ARCH §4.4 — CLI `store query` and all analysis code consume):
  - `latest(lf: pl.LazyFrame, unit_columns: Sequence[str]) -> pl.LazyFrame`
  - `as_of(lf: pl.LazyFrame, unit_columns: Sequence[str], when: date) -> pl.LazyFrame` (inclusive)
  - `prints(lf: pl.LazyFrame, revision: int | None = None, benchmark: int | None = None) -> pl.LazyFrame`

- [ ] **Step 1: Write the failing tests** — `tests/storage/test_reads.py`:

```python
from datetime import date, datetime, timezone

import polars as pl

from bls_stats.storage.reads import as_of, latest, prints

TS = datetime(2026, 7, 2, tzinfo=timezone.utc)


def frame(rows: list[dict]) -> pl.LazyFrame:
    return pl.DataFrame(
        [
            {
                "series_id": r.get("sid", "S1"), "value": r["v"],
                "ref_date": r["ref"], "release_date": r["rel"],
                "revision": r.get("rev"), "benchmark": r.get("bm"),
                "source": r.get("src", "increment"), "downloaded": TS,
            }
            for r in rows
        ],
        schema_overrides={"revision": pl.Int16, "benchmark": pl.Int16},
    ).lazy()


VINTAGES = frame([
    {"ref": date(2026, 4, 12), "rel": date(2026, 5, 1), "rev": 0, "bm": 0, "v": 1.0},
    {"ref": date(2026, 4, 12), "rel": date(2026, 6, 1), "rev": 1, "bm": 0, "v": 2.0},
    {"ref": date(2026, 4, 12), "rel": date(2026, 7, 2), "rev": 2, "bm": 0, "v": 3.0},
])


def test_latest_picks_max_release_date() -> None:
    out = latest(VINTAGES, ["series_id"]).collect()
    assert out.height == 1 and out["value"][0] == 3.0


def test_as_of_never_leaks_future() -> None:  # the ARCH §9 crown-jewel invariant
    out = as_of(VINTAGES, ["series_id"], date(2026, 6, 15)).collect()
    assert out["value"][0] == 2.0
    assert (out["release_date"] <= date(2026, 6, 15)).all()


def test_as_of_inclusive_of_release_day() -> None:
    out = as_of(VINTAGES, ["series_id"], date(2026, 6, 1)).collect()
    assert out["value"][0] == 2.0


def test_tiebreak_prefers_increment_then_counters() -> None:  # ARCH §4.4
    lf = frame([
        {"ref": date(2026, 4, 12), "rel": date(2026, 7, 1), "rev": None, "bm": None,
         "src": "backfill", "v": 10.0},
        {"ref": date(2026, 4, 12), "rel": date(2026, 7, 1), "rev": 2, "bm": 1, "v": 20.0},
    ])
    out = latest(lf, ["series_id"]).collect()
    assert out.height == 1 and out["value"][0] == 20.0


def test_prints_filters_on_counters() -> None:
    out = prints(VINTAGES, revision=1).collect()
    assert out.height == 1 and out["value"][0] == 2.0
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/storage/test_reads.py -v`

- [ ] **Step 3: Implement** — `src/bls_stats/storage/reads.py`:

```python
"""The three canonical vintage reads (ARCH §4.4), with the deterministic tie-break."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

import polars as pl


def latest(lf: pl.LazyFrame, unit_columns: Sequence[str]) -> pl.LazyFrame:
    key = [*unit_columns, "ref_date"]
    return (
        lf.with_columns(
            pl.when(pl.col("source") == "increment").then(1).otherwise(0).alias("_src_rank")
        )
        .sort(
            ["release_date", "_src_rank", "benchmark", "revision"],
            descending=True,
            nulls_last=True,
        )
        .unique(subset=key, keep="first", maintain_order=True)
        .drop("_src_rank")
    )


def as_of(lf: pl.LazyFrame, unit_columns: Sequence[str], when: date) -> pl.LazyFrame:
    return latest(lf.filter(pl.col("release_date") <= when), unit_columns)


def prints(
    lf: pl.LazyFrame, revision: int | None = None, benchmark: int | None = None
) -> pl.LazyFrame:
    if revision is not None:
        lf = lf.filter(pl.col("revision") == revision)
    if benchmark is not None:
        lf = lf.filter(pl.col("benchmark") == benchmark)
    return lf
```

- [ ] **Step 4: Run to verify pass** — `uv run pytest tests/storage/test_reads.py -v`

- [ ] **Step 5: Commit** — `git commit -am "feat: latest/as-of/prints vintage reads with deterministic tie-break"`

---

### Task 9: Doctor (`storage/doctor.py`)

**Files:**
- Create: `src/bls_stats/storage/doctor.py`
- Test: `tests/storage/test_doctor.py`

**Interfaces:**
- Consumes: `Settings`, `storage_options`, `build_client`/`get`.
- Produces (CLI consumes): `CheckResult(name: str, ok: bool, detail: str)`; `run_all(settings: Settings) -> list[CheckResult]`; individual checks `check_env`, `check_deltalake`, `check_store`, `check_conditional_put`, `check_bls` (network).

- [ ] **Step 1: Write the failing tests** — `tests/storage/test_doctor.py`:

```python
import os

import pytest

from bls_stats.core.config import Settings
from bls_stats.storage.doctor import check_conditional_put, check_deltalake, check_env


def test_check_env_flags_default_email_and_local_store() -> None:
    results = {r.name: r for r in check_env(Settings())}
    assert results["contact_email"].ok is False        # default email → warn
    assert results["store_uri"].ok is False            # local path → warn (ARCH §10)
    assert results["api_key"].ok is False


def test_check_env_passes_with_real_config() -> None:
    s = Settings(
        store_uri="s3://bls-stats/store", contact_email="me@example.org",
        contact_email_is_default=False, api_key="k",
    )
    assert all(r.ok for r in check_env(s))


def test_check_deltalake_importable() -> None:
    assert check_deltalake().ok is True


def test_conditional_put_skips_on_local_store() -> None:
    r = check_conditional_put(Settings(store_uri="./data/store"))
    assert r.ok is True and "skipped" in r.detail


@pytest.mark.real_store
def test_conditional_put_against_minio() -> None:
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    if not endpoint:
        pytest.skip("no AWS_ENDPOINT_URL configured")
    r = check_conditional_put(
        Settings(store_uri="s3://bls-stats/test-store", aws_endpoint_url=endpoint)
    )
    assert r.ok is True and "supported" in r.detail
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/storage/test_doctor.py -v`

- [ ] **Step 3: Implement** — `src/bls_stats/storage/doctor.py`:

```python
"""Pre-flight probes (ARCH §8 doctor): env, delta-rs, store, conditional PUT, BLS reachability."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from bls_stats.core.config import Settings, storage_options
from bls_stats.core.http import build_client, get


@dataclass(frozen=True)
class CheckResult:
    name: str
    ok: bool
    detail: str


def check_env(settings: Settings) -> list[CheckResult]:
    return [
        CheckResult(
            "contact_email",
            not settings.contact_email_is_default,
            settings.contact_email if not settings.contact_email_is_default
            else "BLS_CONTACT_EMAIL unset — using placeholder; BLS expects a real contact",
        ),
        CheckResult(
            "store_uri",
            settings.store_uri.startswith("s3://"),
            settings.store_uri if settings.store_uri.startswith("s3://")
            else f"{settings.store_uri} is a local path — laptop-only convenience (ARCH §10)",
        ),
        CheckResult("api_key", settings.api_key is not None,
                    "set" if settings.api_key else "BLS_API_KEY unset — api_v2 engine unavailable"),
    ]


def check_deltalake() -> CheckResult:
    try:
        import deltalake

        return CheckResult("deltalake", True, f"deltalake {deltalake.__version__}")
    except Exception as exc:  # pragma: no cover - import failure environment-specific
        return CheckResult("deltalake", False, str(exc))


def check_store(settings: Settings) -> CheckResult:
    from bls_stats.storage.delta import VintageStore

    try:
        VintageStore(settings.store_uri, storage_options(settings)).read_state("ledger")
        return CheckResult("store", True, f"reachable: {settings.store_uri}")
    except Exception as exc:
        return CheckResult("store", False, f"{settings.store_uri}: {exc}")


def check_conditional_put(settings: Settings) -> CheckResult:
    """If-None-Match probe (ARCH §4.1) — decides delta commit-safety mode."""
    if not settings.store_uri.startswith("s3://"):
        return CheckResult("conditional_put", True, "skipped: local store")
    import boto3
    from botocore.exceptions import ClientError

    bucket = settings.store_uri.removeprefix("s3://").split("/", 1)[0]
    key = f"_doctor/probe-{uuid.uuid4().hex}"
    s3 = boto3.client("s3", endpoint_url=settings.aws_endpoint_url)
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=b"a", IfNoneMatch="*")
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=b"b", IfNoneMatch="*")
            return CheckResult(
                "conditional_put", False,
                "NOT honored — set BLS_S3_UNSAFE_RENAME=true (single-writer mode)",
            )
        except ClientError as exc:
            code = exc.response["ResponseMetadata"]["HTTPStatusCode"]
            ok = code == 412
            return CheckResult("conditional_put", ok,
                               "supported (412 on overwrite)" if ok else f"odd status {code}")
        finally:
            s3.delete_object(Bucket=bucket, Key=key)
    except Exception as exc:
        return CheckResult("conditional_put", False, f"probe failed: {exc}")


def check_bls(settings: Settings) -> CheckResult:
    try:
        client = build_client(settings, timeout=30.0)
        resp = get(client, "https://download.bls.gov/pub/time.series/jt/", method="HEAD")
        return CheckResult("bls", resp.status_code == 200, f"HTTP {resp.status_code}")
    except Exception as exc:
        return CheckResult("bls", False, str(exc))


def run_all(settings: Settings) -> list[CheckResult]:
    return [
        *check_env(settings),
        check_deltalake(),
        check_store(settings),
        check_conditional_put(settings),
        check_bls(settings),
    ]
```

- [ ] **Step 4: Run to verify pass** — `uv run pytest tests/storage/test_doctor.py -v` (real_store case skips locally unless MinIO env is set — run once with `AWS_ENDPOINT_URL=http://127.0.0.1:9000 uv run pytest tests/storage/test_doctor.py -m real_store -v` if MinIO is up and the `bls-stats` bucket exists: `uv run python -c "import boto3,os; boto3.client('s3', endpoint_url=os.environ['AWS_ENDPOINT_URL']).create_bucket(Bucket='bls-stats')"`).

- [ ] **Step 5: Milestone B checkpoint + commit**

Run: `uv run pytest -q && uv run ruff check .` — all green.
Commit: `git commit -am "feat: doctor probes (env, deltalake, store, conditional put, bls)"`

---

### Task 10: Feed parsing (`releases/feeds.py`)

**Files:**
- Create: `src/bls_stats/releases/feeds.py`, `scripts/capture_fixtures.py`, `tests/fixtures/feeds/empsit.xml` (synthetic, quirk-bearing), `tests/fixtures/feeds/cewbd.xml`
- Test: `tests/releases/test_feeds.py`

**Interfaces:**
- Consumes: `REGISTRY`, `Frequency` (Task 3), `get`/`build_client` (Task 6).
- Produces (profiles/pipeline consume):
  - `Release(program: str, release_date: date, ref_year: int, ref_period: int, is_benchmark: bool)` — frozen dataclass
  - `parse_feed(xml_bytes: bytes, program: str) -> list[Release]`
  - `poll(client: httpx.Client, programs: Sequence[str]) -> list[Release]` — fetches each distinct feed URL once, fans out shared feeds (empsit → ces+cps), oldest-first
  - `FeedParseError(ValueError)`

Empirical rules this task encodes (ARCH §5.2, verified 2026-07-04): feeds are **Atom 1.0**; release date comes from the **link href** `…/archives/{slug}_{MMDDYYYY}.htm` (the only stable key — Atom `id` is unstable); monthly titles carry the month but **never the year** (infer: latest occurrence of that month strictly before the link's release date); quarterly/annual titles carry `"<Nth> Quarter YYYY"` / `"May YYYY"`; **no benchmark wording exists** — `is_benchmark` comes from the profile's structural rule (`jan_data`: monthly ref month == 1; `q1_data`: quarterly ref quarter == 1); missing entries (shutdowns) are tolerated.

- [ ] **Step 1: Create the quirk-bearing fixtures**

`tests/fixtures/feeds/empsit.xml` — three entries modeled on the live feed: June 2026 (normal), January 2026 (benchmark by structure, no benchmark wording), and a shutdown-lagged September 2025 published 2025-11-20 (exercises year inference):

```xml
<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>BLS Latest Numbers</title>
  <entry>
    <id>empsit-2026_07_02__08_30_00</id>
    <title>Both payroll employment (+57,000) and unemployment rate (4.2%) change little in June</title>
    <link href="https://www.bls.gov/news.release/archives/empsit_07022026.htm"/>
    <content type="text">Total nonfarm payroll employment changed little in June.</content>
    <published>2026-07-02T08:30:00-04:00</published>
    <updated>2026-07-02T08:30:00-04:00</updated>
  </entry>
  <entry>
    <id>empsit-2026_02_11__08_30_00</id>
    <title>Payroll employment rises by 130,000 in January; unemployment rate changes little at 4.3%</title>
    <link href="https://www.bls.gov/news.release/archives/empsit_02112026.htm"/>
    <content type="text">Employment rose in January.</content>
    <published>2026-02-11T08:30:00-05:00</published>
    <updated>2026-02-11T08:30:00-05:00</updated>
  </entry>
  <entry>
    <id>empsit-2025_11_20__08_30_00</id>
    <title>Payroll employment increases in September; unemployment rate changes little</title>
    <link href="https://www.bls.gov/news.release/archives/empsit_11202025.htm"/>
    <content type="text">Delayed by the lapse in federal funding.</content>
    <published>2025-11-20T08:30:00-05:00</published>
    <updated>2025-11-20T08:30:00-05:00</updated>
  </entry>
</feed>
```

`tests/fixtures/feeds/cewbd.xml` — one quarterly entry whose Atom `id`/`updated` were edited in place (the observed instability), title carrying quarter + year:

```xml
<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>cewbd-2026_06_30__10_00_00</id>
    <title>Business Employment Dynamics — Third Quarter 2025</title>
    <link href="https://www.bls.gov/news.release/archives/cewbd_04292026.htm"/>
    <content type="text">Gross job gains and losses, third quarter 2025.</content>
    <published>2026-04-29T10:00:00-04:00</published>
    <updated>2026-06-30T09:00:00-04:00</updated>
  </entry>
</feed>
```

- [ ] **Step 2: Write the failing tests** — `tests/releases/test_feeds.py`:

```python
from datetime import date
from pathlib import Path

from bls_stats.releases.feeds import Release, parse_feed

FIXTURES = Path(__file__).parent.parent / "fixtures" / "feeds"


def _empsit() -> bytes:
    return (FIXTURES / "empsit.xml").read_bytes()


def test_release_date_from_link_href_not_id() -> None:
    releases = parse_feed(_empsit(), "ces")
    assert releases[0].release_date == date(2025, 11, 20)  # oldest first


def test_monthly_year_inference_handles_shutdown_lag() -> None:
    sept = parse_feed(_empsit(), "ces")[0]
    assert (sept.ref_year, sept.ref_period) == (2025, 9)  # published 2025-11-20, 2-month lag


def test_benchmark_detected_structurally_not_textually() -> None:
    releases = {(r.ref_year, r.ref_period): r for r in parse_feed(_empsit(), "ces")}
    assert releases[(2026, 1)].is_benchmark is True   # January data ⇒ CES benchmark
    assert releases[(2026, 6)].is_benchmark is False


def test_cps_shares_empsit_with_own_events() -> None:
    ces = parse_feed(_empsit(), "ces")
    cps = parse_feed(_empsit(), "cps")
    assert len(ces) == len(cps) == 3
    assert all(r.program == "cps" for r in cps)


def test_quarterly_parse_uses_link_date_despite_edited_entry() -> None:
    releases = parse_feed((FIXTURES / "cewbd.xml").read_bytes(), "bed")
    assert releases == [
        Release("bed", date(2026, 4, 29), 2025, 3, False)
    ]


def test_unparseable_entry_skipped_not_fatal() -> None:
    xml = b"""<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom">
      <entry><title>Nothing useful here</title>
      <link href="https://www.bls.gov/news.release/archives/empsit_01022026.htm"/>
      <published>2026-01-02T08:30:00-05:00</published></entry></feed>"""
    assert parse_feed(xml, "ces") == []
```

- [ ] **Step 3: Run to verify failure** — `uv run pytest tests/releases/test_feeds.py -v`

- [ ] **Step 4: Implement** — `src/bls_stats/releases/feeds.py`:

```python
"""Atom feed → typed Release events (ARCH §5.1–§5.2)."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date
from xml.etree import ElementTree

import httpx

from bls_stats.registry import REGISTRY, Frequency

log = logging.getLogger(__name__)

ATOM = "{http://www.w3.org/2005/Atom}"
_LINK_DATE = re.compile(r"_(\d{2})(\d{2})(\d{4})\.htm")
_MONTHS = {m: i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}
_MONTH_RE = re.compile(r"\b(" + "|".join(_MONTHS) + r")\b")
_QUARTER_RE = re.compile(r"\b(First|Second|Third|Fourth) Quarter (\d{4})\b")
_QUARTERS = {"First": 1, "Second": 2, "Third": 3, "Fourth": 4}
_ANNUAL_RE = re.compile(r"\bMay (\d{4})\b")


class FeedParseError(ValueError):
    pass


@dataclass(frozen=True)
class Release:
    program: str
    release_date: date
    ref_year: int
    ref_period: int
    is_benchmark: bool


def _infer_year(month: int, published: date) -> int:
    """Latest occurrence of `month` strictly before the release date (ARCH §5.2)."""
    year = published.year
    if month >= published.month:
        year -= 1
    return year


def _ref_period(text: str, freq: Frequency, release_date: date) -> tuple[int, int] | None:
    if freq == Frequency.MONTHLY:
        m = _MONTH_RE.search(text)
        if not m:
            return None
        month = _MONTHS[m.group(1)]
        return _infer_year(month, release_date), month
    if freq == Frequency.QUARTERLY:
        m = _QUARTER_RE.search(text)
        return (int(m.group(2)), _QUARTERS[m.group(1)]) if m else None
    m = _ANNUAL_RE.search(text)  # annual (oews): "May YYYY"
    return (int(m.group(1)), 1) if m else None


def _is_benchmark(rule: str | None, ref_period: int) -> bool:
    return rule in ("jan_data", "q1_data") and ref_period == 1


def parse_feed(xml_bytes: bytes, program: str) -> list[Release]:
    spec = REGISTRY[program]
    root = ElementTree.fromstring(xml_bytes)
    releases: list[Release] = []
    for entry in root.iter(f"{ATOM}entry"):
        link = entry.find(f"{ATOM}link")
        href = link.get("href", "") if link is not None else ""
        m = _LINK_DATE.search(href)
        if not m:
            log.warning("%s: entry without parseable archive link (%r) — skipped", program, href)
            continue
        release_date = date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        title = (entry.findtext(f"{ATOM}title") or "") + " " + (entry.findtext(f"{ATOM}content") or "")
        parsed = _ref_period(title, spec.frequency, release_date)
        if parsed is None:
            log.warning("%s: no reference period in entry %r — skipped", program, title[:80])
            continue
        ref_year, ref_period = parsed
        releases.append(Release(
            program, release_date, ref_year, ref_period,
            _is_benchmark(spec.profile.benchmark_rule, ref_period),
        ))
    releases.sort(key=lambda r: r.release_date)
    return releases


def poll(client: httpx.Client, programs: list[str]) -> list[Release]:
    """Fetch each distinct feed once; fan out shared feeds BEFORE any ledger logic (ARCH §5.2)."""
    from bls_stats.core.http import get

    by_feed: dict[str, list[str]] = {}
    for p in programs:
        url = REGISTRY[p].feed_url
        if url is None:  # ep — ARCH §5.2 exception
            continue
        by_feed.setdefault(url, []).append(p)
    out: list[Release] = []
    for url, progs in by_feed.items():
        try:
            body = get(client, url).content
        except httpx.HTTPError as exc:
            log.warning("feed %s failed (%s) — programs %s skipped this run", url, exc, progs)
            continue
        for p in progs:
            out.extend(parse_feed(body, p))
    out.sort(key=lambda r: r.release_date)
    return out
```

- [ ] **Step 5: Create the capture script** — `scripts/capture_fixtures.py` (used again in Task 14; run manually, network):

```python
"""Capture live BLS payloads as trimmed test fixtures. Run manually: uv run python scripts/capture_fixtures.py"""

from pathlib import Path

from bls_stats.core.config import load_settings
from bls_stats.core.http import Throttle, build_client, get
from bls_stats.registry import REGISTRY

FIXTURES = Path("tests/fixtures")


def capture_feeds() -> None:
    client = build_client(load_settings())
    throttle = Throttle(2.0)
    for url in sorted({s.feed_url for s in REGISTRY.values() if s.feed_url}):
        throttle.wait()
        name = url.rsplit("/", 1)[-1].replace(".rss", ".live.xml")
        (FIXTURES / "feeds" / name).write_bytes(get(client, url).content)
        print("captured", name)


if __name__ == "__main__":
    FIXTURES.joinpath("feeds").mkdir(parents=True, exist_ok=True)
    capture_feeds()
```

Add a network-marked validation test at the end of `tests/releases/test_feeds.py`:

```python
import pytest


@pytest.mark.network
def test_live_feeds_parse() -> None:
    from bls_stats.core.config import load_settings
    from bls_stats.core.http import build_client
    from bls_stats.releases.feeds import poll

    releases = poll(build_client(load_settings()), ["ces", "cps", "jolts", "sae", "bed", "qcew", "oews"])
    assert len(releases) >= 20  # 6 feeds × ~12 entries, minus unparseable
```

- [ ] **Step 6: Run to verify pass** — `uv run pytest tests/releases/test_feeds.py -v` (network test deselected by default).

- [ ] **Step 7: Commit** — `git add -A && git commit -m "feat: atom feed parsing with year inference, structural benchmarks, fan-out"`

---

### Task 11: Release calendar (`releases/calendar.py`)

**Files:**
- Create: `src/bls_stats/releases/calendar.py`, `tests/fixtures/html/empsit_archive.html`, `tests/fixtures/html/empsit_schedule.html`, `tests/fixtures/html/lapse.html`
- Test: `tests/releases/test_calendar.py`

**Interfaces:**
- Consumes: `REGISTRY`, `ref_date` (Task 4), `get` (Task 6), `Release`-style month/quarter regexes (Task 10 — import `_ref_period` is private; this module re-uses `parse_archive_text` defined here).
- Produces (pipeline/CLI consume):
  - `CALENDAR_SCHEMA: dict[str, pl.DataType]` = `{"program": pl.Utf8, "ref_date": pl.Date, "release_date": pl.Date, "original_release": pl.Date, "is_benchmark": pl.Boolean}`
  - `scrape_archive(html: bytes, program: str) -> pl.DataFrame` (CALENDAR_SCHEMA, `original_release` null)
  - `scrape_schedule(html: bytes, program: str) -> pl.DataFrame`
  - `apply_lapse_overlay(cal: pl.DataFrame, html: bytes) -> pl.DataFrame` — revises/cancels matched rows
  - `build(client, programs: list[str]) -> pl.DataFrame` — archive + schedule + overlay, deduped `(program, ref_date, release_date)` null-safe; **missing sources skip with a warning** (QCEW schedule 404 — ARCH §5.4)
  - `find_gaps(cal: pl.DataFrame) -> pl.DataFrame` — `(program, ref_date)` rows missing from each program's cadence between its min/max
  - `filter_published(program: str, periods: list[Period], cal: pl.DataFrame) -> list[Period]` — **pinned semantics** (ARCH §5.4): drop only periods later than the program's max published ref_date or explicitly cancelled; pre-calendar periods pass through

- [ ] **Step 1: Create HTML fixtures** (trimmed to the structures the parsers target; the capture script gets real ones for the network test)

`tests/fixtures/html/empsit_archive.html`:

```html
<html><body><div id="bodytext">
<a href="/news.release/archives/empsit_07022026.htm">Employment Situation (June 2026)</a><br>
<a href="/news.release/archives/empsit_06052026.htm">Employment Situation (May 2026)</a><br>
<a href="/news.release/archives/empsit_02112026.htm">Employment Situation (January 2026)</a><br>
</div></body></html>
```

`tests/fixtures/html/empsit_schedule.html`:

```html
<html><body><table class="release-list">
<tr><th>Release Name</th><th>Date</th><th>Time</th></tr>
<tr><td>Employment Situation for June 2026</td><td>Jul. 02, 2026</td><td>08:30 AM</td></tr>
<tr><td>Employment Situation for July 2026</td><td>Aug. 07, 2026</td><td>08:30 AM</td></tr>
</table></body></html>
```

`tests/fixtures/html/lapse.html` (one revision, one cancellation):

```html
<html><body><table>
<tr><th>Release</th><th>Original Date</th><th>Rescheduled Date</th></tr>
<tr><td>Employment Situation for September 2025</td><td>Oct. 03, 2025</td><td>Nov. 20, 2025</td></tr>
<tr><td>Employment Situation for October 2025</td><td>Nov. 07, 2025</td><td>Cancelled</td></tr>
</table></body></html>
```

- [ ] **Step 2: Write the failing tests** — `tests/releases/test_calendar.py`:

```python
from datetime import date
from pathlib import Path

import polars as pl

from bls_stats.releases.calendar import (
    apply_lapse_overlay, filter_published, find_gaps, scrape_archive, scrape_schedule,
)

HTML = Path(__file__).parent.parent / "fixtures" / "html"


def test_scrape_archive_extracts_ref_and_release_dates() -> None:
    cal = scrape_archive((HTML / "empsit_archive.html").read_bytes(), "ces")
    assert cal.height == 3
    june = cal.filter(pl.col("ref_date") == date(2026, 6, 12))
    assert june["release_date"][0] == date(2026, 7, 2)
    jan = cal.filter(pl.col("ref_date") == date(2026, 1, 12))
    assert jan["is_benchmark"][0] is True  # jan_data rule


def test_scrape_schedule_includes_upcoming() -> None:
    cal = scrape_schedule((HTML / "empsit_schedule.html").read_bytes(), "ces")
    assert cal.filter(pl.col("ref_date") == date(2026, 7, 12))["release_date"][0] == date(2026, 8, 7)


def test_lapse_overlay_revises_and_cancels() -> None:
    base = scrape_archive((HTML / "empsit_archive.html").read_bytes(), "ces")
    extra = pl.DataFrame({
        "program": ["ces", "ces"],
        "ref_date": [date(2025, 9, 12), date(2025, 10, 12)],
        "release_date": [date(2025, 10, 3), date(2025, 11, 7)],
        "original_release": pl.Series([None, None], dtype=pl.Date),
        "is_benchmark": [False, False],
    })
    cal = apply_lapse_overlay(pl.concat([base, extra]), (HTML / "lapse.html").read_bytes())
    sept = cal.filter(pl.col("ref_date") == date(2025, 9, 12))
    assert sept["release_date"][0] == date(2025, 11, 20)
    assert sept["original_release"][0] == date(2025, 10, 3)
    octr = cal.filter(pl.col("ref_date") == date(2025, 10, 12))
    assert octr["release_date"][0] is None  # cancelled


def _mini_cal() -> pl.DataFrame:
    return pl.DataFrame({
        "program": ["ces"] * 3,
        "ref_date": [date(2026, 3, 12), date(2026, 4, 12), date(2026, 6, 12)],
        "release_date": [date(2026, 4, 3), date(2026, 5, 8), date(2026, 7, 2)],
        "original_release": pl.Series([None] * 3, dtype=pl.Date),
        "is_benchmark": [False] * 3,
    })


def test_find_gaps_reports_missing_month() -> None:
    gaps = find_gaps(_mini_cal())
    assert gaps.to_dicts() == [{"program": "ces", "ref_date": date(2026, 5, 12)}]


def test_filter_published_pinned_semantics() -> None:  # ARCH §5.4
    periods = [(1948, 1), (2026, 5), (2026, 6), (2026, 7)]
    kept = filter_published("ces", periods, _mini_cal())
    assert (1948, 1) in kept        # pre-calendar coverage passes through
    assert (2026, 6) in kept
    assert (2026, 7) not in kept    # beyond latest published
    assert (2026, 5) in kept        # gap within coverage is NOT dropped (it may exist in bulk)


def test_filter_published_drops_cancelled() -> None:
    cal = _mini_cal().with_columns(
        pl.when(pl.col("ref_date") == date(2026, 4, 12))
        .then(pl.lit(None, dtype=pl.Date)).otherwise(pl.col("release_date"))
        .alias("release_date")
    )
    assert (2026, 4) not in filter_published("ces", [(2026, 4)], cal)
```

- [ ] **Step 3: Run to verify failure** — `uv run pytest tests/releases/test_calendar.py -v`

- [ ] **Step 4: Implement** — `src/bls_stats/releases/calendar.py`:

```python
"""Release-date calendar: archive/schedule scrape, lapse overlay, gaps, filter_published (ARCH §5.4)."""

from __future__ import annotations

import logging
import re
from datetime import date

import httpx
import polars as pl
from bs4 import BeautifulSoup

from bls_stats.core.periods import Period, ref_date, reference_periods, shift
from bls_stats.registry import REGISTRY, Frequency

log = logging.getLogger(__name__)

CALENDAR_SCHEMA: dict[str, pl.DataType] = {
    "program": pl.Utf8, "ref_date": pl.Date, "release_date": pl.Date,
    "original_release": pl.Date, "is_benchmark": pl.Boolean,
}

_LINK_DATE = re.compile(r"_(\d{2})(\d{2})(\d{4})\.htm")
_MONTHS = {m: i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}
_MONTH_YEAR = re.compile(r"\b(" + "|".join(_MONTHS) + r")\s+(\d{4})\b")
_QUARTER_YEAR = re.compile(r"\b(First|Second|Third|Fourth) Quarter (\d{4})\b")
_QUARTERS = {"First": 1, "Second": 2, "Third": 3, "Fourth": 4}
_ABBR_DATE = re.compile(r"\b([A-Z][a-z]{2})\.?\s+(\d{1,2}),\s+(\d{4})\b")
_ABBR = {m[:3]: i for m, i in _MONTHS.items()}


def parse_ref_from_text(text: str, program: str) -> tuple[int, int] | None:
    freq = REGISTRY[program].frequency
    if freq == Frequency.MONTHLY:
        m = _MONTH_YEAR.search(text)
        return (int(m.group(2)), _MONTHS[m.group(1)]) if m else None
    if freq == Frequency.QUARTERLY:
        m = _QUARTER_YEAR.search(text)
        return (int(m.group(2)), _QUARTERS[m.group(1)]) if m else None
    m = _MONTH_YEAR.search(text)  # annual: "May 2025"
    return (int(m.group(2)), 1) if m else None


def parse_abbr_date(text: str) -> date | None:
    m = _ABBR_DATE.search(text)
    if not m or m.group(1) not in _ABBR:
        return None
    return date(int(m.group(3)), _ABBR[m.group(1)], int(m.group(2)))


def _row(program: str, ry: int, rp: int, release: date | None,
         original: date | None = None) -> dict:
    rule = REGISTRY[program].profile.benchmark_rule
    return {
        "program": program, "ref_date": ref_date(program, ry, rp), "release_date": release,
        "original_release": original,
        "is_benchmark": rule in ("jan_data", "q1_data") and rp == 1,
    }


def _frame(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=CALENDAR_SCHEMA)


def scrape_archive(html: bytes, program: str) -> pl.DataFrame:
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for a in soup.find_all("a", href=_LINK_DATE):
        m = _LINK_DATE.search(a["href"])
        parsed = parse_ref_from_text(a.get_text(" ", strip=True), program)
        if parsed is None:
            continue
        rows.append(_row(program, *parsed, date(int(m.group(3)), int(m.group(1)), int(m.group(2)))))
    return _frame(rows)


def scrape_schedule(html: bytes, program: str) -> pl.DataFrame:
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for tr in soup.find_all("tr"):
        text = tr.get_text(" ", strip=True)
        parsed = parse_ref_from_text(text, program)
        released = parse_abbr_date(text)
        if parsed and released:
            rows.append(_row(program, *parsed, released))
    return _frame(rows)


def apply_lapse_overlay(cal: pl.DataFrame, html: bytes) -> pl.DataFrame:
    soup = BeautifulSoup(html, "lxml")
    for tr in soup.find_all("tr"):
        cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        if len(cells) < 3:
            continue
        name, original_txt, revised_txt = cells[0], cells[1], cells[2]
        original = parse_abbr_date(original_txt)
        if original is None:
            continue
        cancelled = "cancel" in revised_txt.lower()
        revised = None if cancelled else parse_abbr_date(revised_txt)
        if not cancelled and revised is None:
            continue
        match = (pl.col("release_date") == original)
        # Restrict to programs whose release-name text matches this row where possible:
        for program in cal["program"].unique().to_list():
            parsed = parse_ref_from_text(name, program)
            if parsed is None:
                continue
            rd = ref_date(program, *parsed)
            row_match = match & (pl.col("program") == program) & (pl.col("ref_date") == rd)
            cal = cal.with_columns(
                pl.when(row_match).then(pl.col("release_date")).otherwise(pl.col("original_release")).alias("original_release"),
                pl.when(row_match).then(pl.lit(revised, dtype=pl.Date)).otherwise(pl.col("release_date")).alias("release_date"),
            )
    return cal


LAPSE_URLS = (
    "https://www.bls.gov/bls/2025-lapse-revised-release-dates.htm",
    "https://www.bls.gov/bls/updated_release_schedule.htm",
)


def build(client: httpx.Client, programs: list[str]) -> pl.DataFrame:
    from bls_stats.core.http import Throttle, get

    throttle = Throttle(2.0)
    frames: list[pl.DataFrame] = []
    for program in programs:
        spec = REGISTRY[program]
        for kind, url, scraper in (
            ("archive", spec.archive_url, scrape_archive),
            ("schedule", spec.schedule_url, scrape_schedule),
        ):
            if url is None:
                log.warning("%s: no %s page configured — skipped", program, kind)
                continue
            throttle.wait()
            try:
                frames.append(scraper(get(client, url).content, program))
            except httpx.HTTPError as exc:
                log.warning("%s: %s page failed (%s) — skipped", program, kind, exc)
    cal = pl.concat(frames) if frames else _frame([])
    for url in LAPSE_URLS:
        throttle.wait()
        try:
            cal = apply_lapse_overlay(cal, get(client, url).content)
        except httpx.HTTPError as exc:
            log.warning("lapse overlay %s failed (%s) — skipped", url, exc)
    return cal.unique(
        subset=["program", "ref_date", "release_date"], keep="first", maintain_order=True
    ).sort(["program", "ref_date"])


def find_gaps(cal: pl.DataFrame) -> pl.DataFrame:
    out: list[dict] = []
    for program in cal["program"].unique().sort().to_list():
        freq = REGISTRY[program].frequency
        if freq not in (Frequency.MONTHLY, Frequency.QUARTERLY):
            continue
        have = set(cal.filter(pl.col("program") == program)["ref_date"].drop_nulls().to_list())
        if not have:
            continue
        lo, hi = min(have), max(have)
        n = 12 if freq == Frequency.MONTHLY else 4
        y, p = lo.year, (lo.month if n == 12 else (lo.month + 2) // 3)
        while ref_date(program, y, p) <= hi:
            rd = ref_date(program, y, p)
            if rd not in have:
                out.append({"program": program, "ref_date": rd})
            y, p = shift(program, y, p, 1)
    return pl.DataFrame(out, schema={"program": pl.Utf8, "ref_date": pl.Date})


def filter_published(program: str, periods: list[Period], cal: pl.DataFrame) -> list[Period]:
    """ARCH §5.4 pinned semantics: drop only future-of-latest-published and cancelled periods."""
    mine = cal.filter(pl.col("program") == program)
    published = mine.filter(pl.col("release_date").is_not_null())
    if published.is_empty():
        raise ValueError(f"{program}: release calendar is empty — run `calendar build` first")
    max_ref = published["ref_date"].max()
    cancelled = set(
        mine.filter(pl.col("release_date").is_null())["ref_date"].to_list()
    )
    kept = []
    for year, period in periods:
        rd = ref_date(program, year, period)
        if rd is None or (rd <= max_ref and rd not in cancelled):
            kept.append((year, period))
    return kept
```

- [ ] **Step 5: Run to verify pass** — `uv run pytest tests/releases/test_calendar.py -v`

- [ ] **Step 6: Extend `scripts/capture_fixtures.py`** with archive/schedule/lapse page capture (same pattern as feeds: loop specs, save to `tests/fixtures/html/{program}_{kind}.live.html`), and append this network canary to `tests/releases/test_calendar.py`:

```python
import pytest


@pytest.mark.network
def test_live_calendar_build() -> None:
    from bls_stats.core.config import load_settings
    from bls_stats.core.http import build_client
    from bls_stats.releases.calendar import build

    cal = build(build_client(load_settings()), ["ces", "jolts"])
    assert cal.height >= 20
    assert cal.filter(pl.col("is_benchmark")).height >= 1
```

- [ ] **Step 7: Commit** — `git add -A && git commit -m "feat: release calendar scraping, lapse overlay, gaps, filter_published"`

---

### Task 12: Slot ledger (`vintage/ledger.py`)

**Files:**
- Create: `src/bls_stats/vintage/ledger.py`
- Test: `tests/vintage/test_ledger.py`

**Interfaces:**
- Consumes: `VintageStore.append_state` / `read_state` (Task 7).
- Produces (profiles/pipeline consume):
  - `SlotRecord(program: str, ref_date: date | None, release_date: date, revision: int | None, benchmark: int | None, source: str, row_count: int, status: str, ingested_at: datetime)` — `status` ∈ {`ingested`, `deferred`, `missed`}
  - `class Ledger:` — `__init__(self, store)`; `record(self, records: list[SlotRecord]) -> None` (one transactional append); `resolved(self) -> pl.DataFrame` (latest row per slot key by `ingested_at`, null-safe keys); `slot_status(self, program, ref_date, release_date, revision, benchmark) -> str | None`; `prior_benchmark_count(self, program, ref_date) -> int` (max `benchmark` among **ingested** rows for the ref_date; 0 when none — ARCH §2.1 null-base convention)

- [ ] **Step 1: Write the failing tests** — `tests/vintage/test_ledger.py`:

```python
from datetime import date, datetime, timezone

from bls_stats.storage.delta import VintageStore
from bls_stats.vintage.ledger import Ledger, SlotRecord


def ts(hour: int) -> datetime:
    return datetime(2026, 7, 2, hour, tzinfo=timezone.utc)


def rec(status: str, hour: int, *, revision: int | None = 0, benchmark: int | None = 0,
        ref: date = date(2026, 6, 12), rel: date = date(2026, 7, 2)) -> SlotRecord:
    return SlotRecord("ces", ref, rel, revision, benchmark, "increment", 100, status, ts(hour))


def make_ledger(tmp_path) -> Ledger:
    return Ledger(VintageStore(str(tmp_path / "store")))


def test_append_only_status_resolution(tmp_path) -> None:
    led = make_ledger(tmp_path)
    led.record([rec("deferred", 9)])
    led.record([rec("ingested", 11)])  # transition = append, latest wins (ARCH §4.5)
    assert led.slot_status("ces", date(2026, 6, 12), date(2026, 7, 2), 0, 0) == "ingested"
    assert led.resolved().height == 1


def test_slot_status_none_when_absent(tmp_path) -> None:
    led = make_ledger(tmp_path)
    assert led.slot_status("ces", date(2026, 6, 12), date(2026, 7, 2), 0, 0) is None


def test_null_counters_resolve_null_safely(tmp_path) -> None:
    led = make_ledger(tmp_path)
    led.record([rec("ingested", 9, revision=None, benchmark=None)])
    assert led.slot_status("ces", date(2026, 6, 12), date(2026, 7, 2), None, None) == "ingested"
    assert led.slot_status("ces", date(2026, 6, 12), date(2026, 7, 2), 0, None) is None


def test_prior_benchmark_count(tmp_path) -> None:
    led = make_ledger(tmp_path)
    assert led.prior_benchmark_count("ces", date(2020, 3, 12)) == 0  # null base → 0
    led.record([rec("ingested", 9, ref=date(2020, 3, 12), revision=2, benchmark=1,
                    rel=date(2025, 2, 7))])
    led.record([rec("deferred", 10, ref=date(2020, 3, 12), revision=2, benchmark=2,
                    rel=date(2026, 2, 11))])  # deferred does NOT count
    assert led.prior_benchmark_count("ces", date(2020, 3, 12)) == 1
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/vintage/test_ledger.py -v`

- [ ] **Step 3: Implement** — `src/bls_stats/vintage/ledger.py`:

```python
"""Append-only slot ledger (ARCH §4.5): one row per slot, latest-status-wins resolution."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime

import polars as pl

from bls_stats.storage.backend import Store

LEDGER_SCHEMA: dict[str, pl.DataType] = {
    "program": pl.Utf8, "ref_date": pl.Date, "release_date": pl.Date,
    "revision": pl.Int16, "benchmark": pl.Int16, "source": pl.Utf8,
    "row_count": pl.Int64, "status": pl.Utf8, "ingested_at": pl.Datetime("us", "UTC"),
}
SLOT_KEY = ["program", "ref_date", "release_date", "revision", "benchmark"]


@dataclass(frozen=True)
class SlotRecord:
    program: str
    ref_date: date | None
    release_date: date
    revision: int | None
    benchmark: int | None
    source: str
    row_count: int
    status: str  # ingested | deferred | missed
    ingested_at: datetime


class Ledger:
    TABLE = "ledger"

    def __init__(self, store: Store) -> None:
        self.store = store

    def record(self, records: list[SlotRecord]) -> None:
        if not records:
            return
        df = pl.DataFrame([asdict(r) for r in records], schema=LEDGER_SCHEMA)
        self.store.append_state(self.TABLE, df)

    def _raw(self) -> pl.DataFrame:
        raw = self.store.read_state(self.TABLE)
        return raw if raw is not None else pl.DataFrame(schema=LEDGER_SCHEMA)

    def resolved(self) -> pl.DataFrame:
        """Latest row per slot key. Polars group keys treat nulls as equal — null-safe."""
        return (
            self._raw()
            .sort("ingested_at", descending=True)
            .unique(subset=SLOT_KEY, keep="first", maintain_order=True)
        )

    def slot_status(
        self, program: str, ref_date: date | None, release_date: date,
        revision: int | None, benchmark: int | None,
    ) -> str | None:
        hit = self.resolved().filter(
            (pl.col("program") == program)
            & pl.col("ref_date").eq_missing(pl.lit(ref_date))
            & (pl.col("release_date") == release_date)
            & pl.col("revision").eq_missing(pl.lit(revision, dtype=pl.Int16))
            & pl.col("benchmark").eq_missing(pl.lit(benchmark, dtype=pl.Int16))
        )
        return hit["status"][0] if hit.height else None

    def prior_benchmark_count(self, program: str, ref_date: date | None) -> int:
        got = self.resolved().filter(
            (pl.col("program") == program)
            & pl.col("ref_date").eq_missing(pl.lit(ref_date))
            & (pl.col("status") == "ingested")
        )["benchmark"].max()
        return int(got) if got is not None else 0
```

- [ ] **Step 4: Run to verify pass** — `uv run pytest tests/vintage/test_ledger.py -v`

- [ ] **Step 5: Commit** — `git commit -am "feat: append-only slot ledger with status resolution and benchmark counts"`

---

### Task 13: Profile expansion (`releases/profiles.py`)

**Files:**
- Create: `src/bls_stats/releases/profiles.py`
- Test: `tests/releases/test_profiles.py`

**Interfaces:**
- Consumes: `Release` (Task 10), `REGISTRY`/`RevisionProfile` (Task 3), `ref_date`/`shift` (Task 4).
- Produces (pipeline consumes):
  - `Slot(ref_date: date, revision: int, benchmark: int, kind: str)` — `kind` ∈ {`routine`, `benchmark_window`}
  - `expand(release: Release, prior_benchmark: Callable[[date], int]) -> list[Slot]` — implements ARCH §2.2 exactly: routine slots keep `(slot_index, prior_count)`; benchmark releases add the window **minus routine slots**, each `(terminal_revision, prior_count + 1)`; QCEW `year_to_date` covered. `prior_benchmark` is a callable so this module never imports the ledger (dependency rule).

- [ ] **Step 1: Write the failing tests** — `tests/releases/test_profiles.py`:

```python
from datetime import date

from bls_stats.releases.feeds import Release
from bls_stats.releases.profiles import expand


def no_priors(_ref: date) -> int:
    return 0


def test_ces_routine_release_three_slots() -> None:
    slots = expand(Release("ces", date(2026, 7, 2), 2026, 6, False), no_priors)
    assert [(s.ref_date, s.revision, s.benchmark, s.kind) for s in slots] == [
        (date(2026, 6, 12), 0, 0, "routine"),
        (date(2026, 5, 12), 1, 0, "routine"),
        (date(2026, 4, 12), 2, 0, "routine"),
    ]


def test_ces_benchmark_release_one_row_per_ref_date() -> None:  # ARCH §2.2 blocker fix
    slots = expand(Release("ces", date(2026, 2, 11), 2026, 1, True), no_priors)
    by_ref = {s.ref_date: s for s in slots}
    assert len(slots) == len(by_ref)  # no ref_date appears twice
    # routine slots keep (slot, prior_count):
    assert (by_ref[date(2026, 1, 12)].revision, by_ref[date(2026, 1, 12)].benchmark) == (0, 0)
    assert (by_ref[date(2025, 12, 12)].revision, by_ref[date(2025, 12, 12)].benchmark) == (1, 0)
    assert (by_ref[date(2025, 11, 12)].revision, by_ref[date(2025, 11, 12)].benchmark) == (2, 0)
    # window-only slots get terminal revision + benchmark increment:
    oct_2025 = by_ref[date(2025, 10, 12)]
    assert (oct_2025.revision, oct_2025.benchmark, oct_2025.kind) == (2, 1, "benchmark_window")
    # window start: January of (2026 - 5):
    assert min(by_ref) == date(2021, 1, 12)


def test_benchmark_counter_uses_prior_counts() -> None:
    def priors(ref: date) -> int:
        return 3 if ref == date(2025, 6, 12) else 0

    slots = expand(Release("ces", date(2026, 2, 11), 2026, 1, True), priors)
    jun25 = next(s for s in slots if s.ref_date == date(2025, 6, 12))
    assert jun25.benchmark == 4  # prior 3 + 1


def test_jolts_ref_dates_use_last_business_day() -> None:
    slots = expand(Release("jolts", date(2026, 6, 30), 2026, 5, False), no_priors)
    assert slots[0].ref_date == date(2026, 5, 29)  # May 31 2026 is a Sunday


def test_qcew_year_to_date_routine() -> None:  # ARCH §6.2 touched set
    slots = expand(Release("qcew", date(2026, 6, 4), 2025, 4, False), no_priors)
    assert [(s.ref_date, s.revision) for s in slots] == [
        (date(2025, 12, 12), 0), (date(2025, 9, 12), 1),
        (date(2025, 6, 12), 2), (date(2025, 3, 12), 3),
    ]


def test_qcew_q1_benchmark_pulls_prior_year() -> None:
    slots = expand(Release("qcew", date(2026, 9, 3), 2026, 1, True), no_priors)
    by_ref = {s.ref_date: s for s in slots}
    assert (by_ref[date(2026, 3, 12)].revision, by_ref[date(2026, 3, 12)].benchmark) == (0, 0)
    # prior-year quarters: terminal revision 4-q, benchmark+1 (spec lifecycle (3,0)→(3,1)):
    assert (by_ref[date(2025, 3, 12)].revision, by_ref[date(2025, 3, 12)].benchmark) == (3, 1)
    assert (by_ref[date(2025, 12, 12)].revision, by_ref[date(2025, 12, 12)].benchmark) == (0, 1)


def test_cps_single_slot() -> None:
    slots = expand(Release("cps", date(2026, 7, 2), 2026, 6, False), no_priors)
    assert len(slots) == 1 and slots[0].revision == 0
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/releases/test_profiles.py -v`

- [ ] **Step 3: Implement** — `src/bls_stats/releases/profiles.py`:

```python
"""Release → fetch-plan expansion (ARCH §2.2, §5.3). Pure: ledger context is injected."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date

from bls_stats.core.periods import Period, ref_date, shift
from bls_stats.registry import REGISTRY, Frequency
from bls_stats.releases.feeds import Release


@dataclass(frozen=True)
class Slot:
    ref_date: date
    revision: int
    benchmark: int
    kind: str  # "routine" | "benchmark_window"


def _routine_periods(release: Release) -> list[tuple[Period, int]]:
    """[(period, revision)] the release structurally carries (ARCH §2.1)."""
    spec = REGISTRY[release.program]
    newest = (release.ref_year, release.ref_period)
    if spec.profile.routine_rule == "year_to_date":  # QCEW: all quarters of ref year so far
        return [
            (shift(release.program, *newest, -i), i) for i in range(release.ref_period)
        ]
    return [
        (shift(release.program, *newest, -i), i) for i in range(spec.profile.routine_slots)
    ]


def _terminal_revision(program: str, period: Period, release: Release) -> int:
    spec = REGISTRY[program]
    if spec.profile.routine_rule == "year_to_date":
        return 4 - period[1]  # quarter q of the completed prior year: prints at q..Q4 releases
    return spec.profile.routine_slots - 1


def _window_periods(release: Release) -> list[Period]:
    """ARCH §2.2: January/Q1 of (newest_year - window_years) through newest, inclusive."""
    spec = REGISTRY[release.program]
    years = spec.profile.benchmark_window_years or 0
    if spec.profile.routine_rule == "year_to_date":  # QCEW: the prior calendar year(s)
        out: list[Period] = []
        for y in range(release.ref_year - years, release.ref_year):
            out.extend((y, q) for q in range(1, 5))
        return out
    n = 12 if spec.frequency == Frequency.MONTHLY else 4
    start = (release.ref_year - years, 1)
    out = []
    cur = start
    while cur <= (release.ref_year, release.ref_period):
        out.append(cur)
        cur = shift(release.program, *cur, 1)
        if len(out) > years * n + n:  # safety bound
            break
    return out


def expand(release: Release, prior_benchmark: Callable[[date], int]) -> list[Slot]:
    program = release.program
    slots: dict[date, Slot] = {}
    for period, revision in _routine_periods(release):
        rd = ref_date(program, *period)
        assert rd is not None
        slots[rd] = Slot(rd, revision, prior_benchmark(rd), "routine")
    if release.is_benchmark:
        for period in _window_periods(release):
            rd = ref_date(program, *period)
            assert rd is not None
            if rd in slots:  # one row per ref_date per release (ARCH §2.2)
                continue
            slots[rd] = Slot(
                rd,
                _terminal_revision(program, period, release),
                prior_benchmark(rd) + 1,
                "benchmark_window",
            )
    return sorted(slots.values(), key=lambda s: s.ref_date, reverse=True)
```

- [ ] **Step 4: Run to verify pass** — `uv run pytest tests/releases/test_profiles.py -v`

- [ ] **Step 5: Milestone C checkpoint + commit**

Run: `uv run pytest -q && uv run ruff check .` — all green.
Commit: `git commit -am "feat: release-to-fetch-plan expansion with benchmark windows"`

---

### Task 14: LABSTAT engine (`engines/labstat.py`)

**Files:**
- Create: `src/bls_stats/engines/labstat.py`, `tests/fixtures/labstat/ce.data.sample.txt`, `tests/fixtures/labstat/sm.data.sample.txt`
- Test: `tests/engines/test_labstat.py`

**Interfaces:**
- Consumes: `ProgramSpec` (Task 3), `Period`/`ref_date` (Task 4), `download`/`head_last_modified` (Task 6), `Slot` (Task 13).
- Produces (pipeline consumes):
  - `parse_flat_file(path: Path, program: str, periods: list[Period] | None = None, *, downloaded: datetime) -> pl.DataFrame` — native columns + `ref_date` + `downloaded`, `M13`/annual rows dropped, `year`/`period` dropped, whitespace-trimmed, string locks
  - `fetch(client, program: str, url: str, periods: list[Period], dest_dir: Path, downloaded: datetime) -> pl.DataFrame` — download + parse + filter
  - `is_fresh(client, program: str, release_date: date) -> bool` — Last-Modified ≥ scheduled embargo (ET per registry) on the release date (ARCH §6.3)

- [ ] **Step 1: Create fixtures** (tab-separated, real LABSTAT shape: padded columns, M13 rows, footnotes)

`tests/fixtures/labstat/ce.data.sample.txt`:

```
series_id	year	period	       value	footnote_codes
CES0000000001	2026	M04	159123	
CES0000000001	2026	M05	159180	P
CES0000000001	2026	M06	159237	P
CES0000000001	2025	M13	158000	
CES0500000003	2026	M05	  35.42	P
```

`tests/fixtures/labstat/sm.data.sample.txt` (includes M13 — ARCH §6.2 gotcha):

```
series_id	year	period	       value	footnote_codes
SMS01000000000000001	2026	M04	2150.3	
SMS01000000000000001	2026	M05	2151.1	P
SMS01000000000000001	2025	M13	2149.0	
```

(Use literal TAB characters between fields.)

- [ ] **Step 2: Write the failing tests** — `tests/engines/test_labstat.py`:

```python
from datetime import date, datetime, timezone
from pathlib import Path

import httpx
import polars as pl

from bls_stats.engines.labstat import is_fresh, parse_flat_file

FIXTURES = Path(__file__).parent.parent / "fixtures" / "labstat"
TS = datetime(2026, 7, 2, 13, 0, tzinfo=timezone.utc)


def test_parse_drops_m13_and_attaches_ref_date() -> None:
    df = parse_flat_file(FIXTURES / "ce.data.sample.txt", "ces", downloaded=TS)
    assert df.height == 4  # M13 row gone
    assert set(df.columns) == {"series_id", "value", "footnote_codes", "ref_date", "downloaded"}
    assert df.schema["series_id"] == pl.Utf8
    assert df.schema["footnote_codes"] == pl.Utf8
    assert df.schema["ref_date"] == pl.Date
    assert date(2026, 6, 12) in df["ref_date"].to_list()


def test_parse_filters_to_requested_periods() -> None:
    df = parse_flat_file(FIXTURES / "ce.data.sample.txt", "ces", [(2026, 5)], downloaded=TS)
    assert df["ref_date"].unique().to_list() == [date(2026, 5, 12)]
    assert df.height == 2  # two series carry May


def test_values_are_trimmed_floats() -> None:
    df = parse_flat_file(FIXTURES / "ce.data.sample.txt", "ces", [(2026, 5)], downloaded=TS)
    assert df.schema["value"] == pl.Float64
    assert 35.42 in df["value"].to_list()


def test_sm_m13_dropped() -> None:
    df = parse_flat_file(FIXTURES / "sm.data.sample.txt", "sae", downloaded=TS)
    assert df.height == 2


def test_is_fresh_compares_to_embargo() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={"Last-Modified": "Thu, 02 Jul 2026 12:30:00 GMT"})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    assert is_fresh(client, "ces", date(2026, 7, 2)) is True    # 08:30 ET == 12:30 UTC (EDT)
    assert is_fresh(client, "ces", date(2026, 7, 3)) is False   # file older than next release
```

- [ ] **Step 3: Run to verify failure** — `uv run pytest tests/engines/test_labstat.py -v`

- [ ] **Step 4: Implement** — `src/bls_stats/engines/labstat.py`:

```python
"""Flat-file engine for ces/sae/jolts/cps/bed (BEH §2.1, ARCH §6.2–§6.3)."""

from __future__ import annotations

import logging
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import polars as pl

from bls_stats.core.http import download, head_last_modified
from bls_stats.core.periods import Period, ref_date
from bls_stats.registry import REGISTRY, Frequency

log = logging.getLogger(__name__)
_ET = ZoneInfo("America/New_York")


def parse_flat_file(
    path: Path, program: str, periods: list[Period] | None = None, *, downloaded: datetime
) -> pl.DataFrame:
    spec = REGISTRY[program]
    period_re = r"^M(0[1-9]|1[0-2])$" if spec.frequency == Frequency.MONTHLY else r"^Q0[1-4]$"
    lf = (
        pl.scan_csv(
            path, separator="\t", infer_schema=False,
            missing_utf8_is_empty_string=True,
        )
        .rename(lambda c: c.strip())  # LABSTAT headers are space-padded
        .with_columns(pl.col("series_id", "period", "value", "footnote_codes").str.strip_chars())
        .with_columns(pl.col("year").str.strip_chars().cast(pl.Int32))
        .filter(pl.col("period").str.contains(period_re))  # drops M13 (BEH §2.1)
        .with_columns(
            pl.col("period").str.slice(1).cast(pl.Int8).alias("_pnum"),
            pl.col("value").cast(pl.Float64, strict=False),
        )
    )
    if periods is not None:
        allowed = pl.DataFrame(
            {"year": [y for y, _ in periods], "_pnum": [p for _, p in periods]},
            schema={"year": pl.Int32, "_pnum": pl.Int8},
        ).lazy()
        lf = lf.join(allowed, on=["year", "_pnum"], how="semi")
    df = lf.collect(engine="streaming")
    ref_dates = pl.DataFrame(
        [
            {"year": y, "_pnum": p, "ref_date": ref_date(program, y, p)}
            for y, p in df.select("year", "_pnum").unique().iter_rows()
        ],
        schema={"year": pl.Int32, "_pnum": pl.Int8, "ref_date": pl.Date},
    )
    return (
        df.join(ref_dates, on=["year", "_pnum"], how="left")
        .drop("year", "period", "_pnum")
        .with_columns(pl.lit(downloaded).dt.cast_time_unit("us").alias("downloaded"))
    )


def fetch(
    client: httpx.Client, program: str, url: str, periods: list[Period],
    dest_dir: Path, downloaded: datetime,
) -> pl.DataFrame:
    local = download(client, url, dest_dir / url.rsplit("/", 1)[-1])
    try:
        df = parse_flat_file(local, program, periods, downloaded=downloaded)
    finally:
        local.unlink(missing_ok=True)  # scratch discipline (ARCH §10)
    log.info("%s: parsed %d rows for %d period(s)", program, df.height, len(periods))
    return df


def embargo_utc(program: str, release_date: date) -> datetime:
    hh, mm = (REGISTRY[program].release_time_et or "08:30").split(":")
    return datetime.combine(release_date, time(int(hh), int(mm)), tzinfo=_ET).astimezone(
        ZoneInfo("UTC")
    )


def is_fresh(client: httpx.Client, program: str, release_date: date) -> bool:
    """ARCH §6.3 stale-file guard: Last-Modified ≥ scheduled embargo on the release date."""
    url = REGISTRY[program].increment_url
    assert url is not None
    last_modified = head_last_modified(client, url)
    if last_modified is None:
        log.warning("%s: no Last-Modified header — treating as stale", program)
        return False
    return last_modified >= embargo_utc(program, release_date)
```

- [ ] **Step 5: Run to verify pass** — `uv run pytest tests/engines/test_labstat.py -v`

- [ ] **Step 6: Extend capture script + network canary.** Add to `scripts/capture_fixtures.py` a `capture_labstat()` that range-requests the first ~200 lines of each program's increment file (send header `Range: bytes=0-20000`, then trim to whole lines) into `tests/fixtures/labstat/{prefix}.data.live.txt`. Append to `tests/engines/test_labstat.py`:

```python
import pytest


@pytest.mark.network
def test_live_headers_and_freshness_probe() -> None:
    from bls_stats.core.config import load_settings
    from bls_stats.core.http import build_client, head_last_modified
    from bls_stats.registry import REGISTRY

    client = build_client(load_settings(), timeout=60.0)
    lm = head_last_modified(client, REGISTRY["jolts"].increment_url)
    assert lm is not None  # Last-Modified present — the stale guard's precondition
```

- [ ] **Step 7: Commit** — `git add -A && git commit -m "feat: labstat flat-file engine with M13 drop, period filter, stale guard"`

---

### Task 15: QCEW engine (`engines/qcew.py`)

**Files:**
- Create: `src/bls_stats/engines/qcew.py`, `tests/fixtures/qcew/make_fixture.py` (dev helper), `tests/fixtures/qcew/2025_qtrly_singlefile_sample.zip`
- Test: `tests/engines/test_qcew.py`

**Interfaces:**
- Consumes: `download` (Task 6), `ref_date` (Task 4).
- Produces: `parse_year_zip(zip_path: Path, quarters: list[int], *, downloaded: datetime, by_size_zip: Path | None = None) -> pl.DataFrame` — the BEH §2.2 output contract (14 named columns + `ref_date` + `downloaded`, `area_fips` `Utf8`, dedup: singlefile keeps `size_code == 0`, by-size keeps `> 0`); `fetch_year(client, year: int, quarters: list[int], dest_dir: Path, downloaded: datetime, *, with_size: bool = False) -> pl.DataFrame` — **one year at a time** (ARCH §10).

- [ ] **Step 1: Create the fixture generator** — `tests/fixtures/qcew/make_fixture.py`:

```python
"""Builds a miniature QCEW singlefile ZIP with the real column set. Run once; commit the zip."""

import io
import zipfile
from pathlib import Path

COLS = ("area_fips,own_code,industry_code,agglvl_code,size_code,year,qtr,disclosure_code,"
        "qtrly_estabs,month1_emplvl,month2_emplvl,month3_emplvl,total_qtrly_wages,"
        "taxable_qtrly_wages,qtrly_contributions,avg_wkly_wage")
ROWS = [
    '"01001","0","10","70","0","2025","1","","1200","41000","41100","41200","530000000","1000","500","995"',
    '"01001","0","10","70","0","2025","2","","1210","41300","41350","41400","540000000","1000","500","1002"',
    '"C1010","0","10","40","0","2025","1","","800","30000","30100","30200","410000000","800","400","1050"',
    '"01001","5","10","71","3","2025","1","","300","9000","9010","9020","98000000","200","100","840"',
]

buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
    zf.writestr("2025.q1-q2.singlefile.csv", "\n".join([COLS, *ROWS]) + "\n")
Path(__file__).with_name("2025_qtrly_singlefile_sample.zip").write_bytes(buf.getvalue())
print("fixture written")
```

Run: `uv run python tests/fixtures/qcew/make_fixture.py` and commit the zip.

- [ ] **Step 2: Write the failing tests** — `tests/engines/test_qcew.py`:

```python
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

from bls_stats.engines.qcew import parse_year_zip

ZIP = Path(__file__).parent.parent / "fixtures" / "qcew" / "2025_qtrly_singlefile_sample.zip"
TS = datetime(2026, 6, 4, 14, 0, tzinfo=timezone.utc)


def test_area_fips_stays_utf8_with_alpha_codes() -> None:  # BEH §2.2 critical gotcha
    df = parse_year_zip(ZIP, [1], downloaded=TS)
    assert df.schema["area_fips"] == pl.Utf8
    assert "C1010" in df["area_fips"].to_list()
    assert "01001" in df["area_fips"].to_list()  # leading zero intact


def test_quarter_filter_and_ref_date() -> None:
    df = parse_year_zip(ZIP, [2], downloaded=TS)
    assert df["ref_date"].unique().to_list() == [date(2025, 6, 12)]


def test_singlefile_keeps_only_size_code_zero() -> None:
    df = parse_year_zip(ZIP, [1], downloaded=TS)
    assert set(df["size_code"].to_list()) == {"0"}


def test_output_contract_columns() -> None:
    df = parse_year_zip(ZIP, [1], downloaded=TS)
    expected = {
        "area_fips", "own_code", "industry_code", "agglvl_code", "size_code",
        "disclosure_code", "qtrly_estabs", "month1_emplvl", "month2_emplvl", "month3_emplvl",
        "total_qtrly_wages", "taxable_qtrly_wages", "qtrly_contributions", "avg_wkly_wage",
        "ref_date", "downloaded",
    }
    assert set(df.columns) == expected  # year/qtr dropped (BEH §2.2)
```

- [ ] **Step 3: Run to verify failure** — `uv run pytest tests/engines/test_qcew.py -v`

- [ ] **Step 4: Implement** — `src/bls_stats/engines/qcew.py`:

```python
"""QCEW per-year ZIP engine (BEH §2.2). Strictly one year at a time (ARCH §10)."""

from __future__ import annotations

import logging
import zipfile
from datetime import datetime
from pathlib import Path

import httpx
import polars as pl

from bls_stats.core.http import download
from bls_stats.core.periods import ref_date

log = logging.getLogger(__name__)

_CODE_COLS = ("area_fips", "own_code", "industry_code", "agglvl_code", "size_code",
              "disclosure_code")
_VALUE_COLS = ("qtrly_estabs", "month1_emplvl", "month2_emplvl", "month3_emplvl",
               "total_qtrly_wages", "taxable_qtrly_wages", "qtrly_contributions",
               "avg_wkly_wage")
URL = "https://data.bls.gov/cew/data/files/{year}/csv/{year}_qtrly_singlefile.zip"
SIZE_URL = "https://data.bls.gov/cew/data/files/{year}/csv/{year}_q1_by_size.zip"


def _read_zip_csv(zip_path: Path) -> pl.DataFrame:
    with zipfile.ZipFile(zip_path) as zf:
        member = next(n for n in zf.namelist() if n.endswith(".csv"))
        with zf.open(member) as fh:
            return pl.read_csv(
                fh.read(),
                schema_overrides={c: pl.Utf8 for c in _CODE_COLS}
                | {"year": pl.Int32, "qtr": pl.Int8}
                | {c: pl.Float64 for c in _VALUE_COLS},
            )


def parse_year_zip(
    zip_path: Path, quarters: list[int], *, downloaded: datetime,
    by_size_zip: Path | None = None,
) -> pl.DataFrame:
    df = _read_zip_csv(zip_path).filter(
        pl.col("qtr").is_in(quarters) & (pl.col("size_code") == "0")
    )
    if by_size_zip is not None:
        size_df = _read_zip_csv(by_size_zip).filter(
            pl.col("qtr").is_in(quarters) & (pl.col("size_code") != "0")
        )
        df = pl.concat([df, size_df], how="vertical_relaxed")
    ref_dates = pl.DataFrame(
        [
            {"year": y, "qtr": q, "ref_date": ref_date("qcew", y, q)}
            for y, q in df.select("year", "qtr").unique().iter_rows()
        ],
        schema={"year": pl.Int32, "qtr": pl.Int8, "ref_date": pl.Date},
    )
    return (
        df.join(ref_dates, on=["year", "qtr"], how="left")
        .drop("year", "qtr")
        .select(*_CODE_COLS, *_VALUE_COLS, "ref_date")
        .with_columns(pl.lit(downloaded).dt.cast_time_unit("us").alias("downloaded"))
    )


def fetch_year(
    client: httpx.Client, year: int, quarters: list[int], dest_dir: Path,
    downloaded: datetime, *, with_size: bool = False,
) -> pl.DataFrame:
    zip_path = download(client, URL.format(year=year), dest_dir / f"qcew_{year}.zip")
    size_path = None
    if with_size:
        size_path = download(client, SIZE_URL.format(year=year), dest_dir / f"qcew_{year}_size.zip")
    try:
        df = parse_year_zip(zip_path, quarters, downloaded=downloaded, by_size_zip=size_path)
    finally:
        zip_path.unlink(missing_ok=True)
        if size_path:
            size_path.unlink(missing_ok=True)
    log.info("qcew %d: %d rows for quarters %s", year, df.height, quarters)
    return df
```

- [ ] **Step 5: Run to verify pass** — `uv run pytest tests/engines/test_qcew.py -v`

- [ ] **Step 6: Commit** — `git add -A && git commit -m "feat: qcew per-year zip engine with area_fips string lock and size dedup"`

---

### Task 16: OEWS engine (`engines/oews.py`)

**Files:**
- Create: `src/bls_stats/engines/oews.py`, `tests/fixtures/oews/make_fixture.py`, `tests/fixtures/oews/oesm25all_sample.zip`
- Test: `tests/engines/test_oews.py`

**Interfaces:**
- Consumes: `download` (Task 6).
- Produces: `parse_workbook_zip(zip_path: Path, year: int, *, downloaded: datetime) -> pl.DataFrame` — every sheet column (names lowercased/trimmed) + `ref_date` (May 12) + `downloaded`; `fetch_year(client, year, dest_dir, downloaded) -> pl.DataFrame` (URL `oesm{yy}all.zip`, sheet `All May {year} data` — BEH §2.3).

- [ ] **Step 1: Fixture generator** — `tests/fixtures/oews/make_fixture.py`:

```python
"""Miniature OEWS workbook zip: one sheet named like the real one. Run once; commit the zip."""

import io
import zipfile
from pathlib import Path

import xlsxwriter

xbuf = io.BytesIO()
wb = xlsxwriter.Workbook(xbuf, {"in_memory": True})
ws = wb.add_worksheet("All May 2025 data")
for col, name in enumerate(["AREA", "AREA_TITLE", "OCC_CODE ", "TOT_EMP", "A_MEAN"]):
    ws.write(0, col, name)  # note trailing space on OCC_CODE — tests the trim
rows = [["01", "Alabama", "00-0000", 1900000, 58000], ["01", "Alabama", "11-1011", 3000, 210000]]
for r, row in enumerate(rows, start=1):
    for c, v in enumerate(row):
        ws.write(r, c, v)
wb.close()

buf = io.BytesIO()
with zipfile.ZipFile(buf, "w") as zf:
    zf.writestr("oesm25all/all_data_M_2025.xlsx", xbuf.getvalue())
Path(__file__).with_name("oesm25all_sample.zip").write_bytes(buf.getvalue())
print("fixture written")
```

Run: `uv run python tests/fixtures/oews/make_fixture.py`; commit the zip.

- [ ] **Step 2: Write the failing tests** — `tests/engines/test_oews.py`:

```python
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

from bls_stats.engines.oews import parse_workbook_zip

ZIP = Path(__file__).parent.parent / "fixtures" / "oews" / "oesm25all_sample.zip"
TS = datetime(2026, 4, 2, 14, 0, tzinfo=timezone.utc)


def test_columns_lowercased_and_trimmed() -> None:
    df = parse_workbook_zip(ZIP, 2025, downloaded=TS)
    assert "occ_code" in df.columns and "area_title" in df.columns


def test_ref_date_is_may_12() -> None:  # BEH §2.3
    df = parse_workbook_zip(ZIP, 2025, downloaded=TS)
    assert df["ref_date"].unique().to_list() == [date(2025, 5, 12)]


def test_all_rows_present_with_codes_as_strings() -> None:
    df = parse_workbook_zip(ZIP, 2025, downloaded=TS)
    assert df.height == 2
    assert df.schema["area"] == pl.Utf8
```

- [ ] **Step 3: Run to verify failure** — `uv run pytest tests/engines/test_oews.py -v`

- [ ] **Step 4: Implement** — `src/bls_stats/engines/oews.py`:

```python
"""OEWS annual workbook engine (BEH §2.3)."""

from __future__ import annotations

import logging
import tempfile
import zipfile
from datetime import date, datetime
from pathlib import Path

import httpx
import polars as pl

from bls_stats.core.http import download

log = logging.getLogger(__name__)
URL = "https://www.bls.gov/oes/special-requests/oesm{yy}all.zip"


def parse_workbook_zip(zip_path: Path, year: int, *, downloaded: datetime) -> pl.DataFrame:
    with zipfile.ZipFile(zip_path) as zf:
        member = next(n for n in zf.namelist() if n.endswith(".xlsx"))
        with tempfile.TemporaryDirectory() as td:
            xlsx = Path(zf.extract(member, td))
            df = pl.read_excel(
                xlsx, sheet_name=f"All May {year} data",
                schema_overrides={"AREA": pl.Utf8, "OCC_CODE": pl.Utf8},
            )
    df.columns = [c.strip().lower() for c in df.columns]
    return df.with_columns(
        pl.lit(date(year, 5, 12)).alias("ref_date"),
        pl.lit(downloaded).dt.cast_time_unit("us").alias("downloaded"),
    )


def fetch_year(
    client: httpx.Client, year: int, dest_dir: Path, downloaded: datetime
) -> pl.DataFrame:
    url = URL.format(yy=f"{year % 100:02d}")
    zip_path = download(client, url, dest_dir / f"oesm{year % 100:02d}all.zip")
    try:
        df = parse_workbook_zip(zip_path, year, downloaded=downloaded)
    finally:
        zip_path.unlink(missing_ok=True)
    log.info("oews %d: %d rows", year, df.height)
    return df
```

Note: `pl.read_excel` schema_overrides keys use the *original* (pre-lowercase) sheet names; fastexcel matches on the stripped header — if the trailing-space header fails the override, cast after read: `df.with_columns(pl.col("area", "occ_code").cast(pl.Utf8))`. The fixture locks whichever works.

- [ ] **Step 5: Run to verify pass** — `uv run pytest tests/engines/test_oews.py -v`

- [ ] **Step 6: Commit** — `git add -A && git commit -m "feat: oews workbook engine with may-12 ref_date"`

---

### Task 17: EP scraper (`engines/ep.py`)

**Files:**
- Create: `src/bls_stats/engines/ep.py`, `tests/fixtures/ep/index.html`, `tests/fixtures/ep/matrix_11-1011.html`
- Test: `tests/engines/test_ep.py`

**Interfaces:**
- Consumes: `get`/`Throttle` (Task 6).
- Produces: `parse_index(html: bytes) -> list[str]` (SOC codes); `parse_matrix(html: bytes, soc: str) -> pl.DataFrame` (BEH §2.4 columns with year-normalized headers, en-dash → null); `fetch_matrix(client, *, throttle: Throttle | None = None, downloaded: datetime, cache: Path | None = None, refresh: bool = False) -> pl.DataFrame` — parquet cache honored unless `refresh`; per-occupation log-and-continue, raises `EpScrapeError` only if **all** fail.

- [ ] **Step 1: Create fixtures**

`tests/fixtures/ep/index.html`:

```html
<html><body><table>
<tr><td><a href="/projections/nationalMatrix?queryParams=11-1011&ioType=o">Chief executives</a></td></tr>
<tr><td><a href="/projections/nationalMatrix?queryParams=15-1252&ioType=o">Software developers</a></td></tr>
</table></body></html>
```

`tests/fixtures/ep/matrix_11-1011.html` (year-specific headers + en-dash null):

```html
<html><body><table>
<thead><tr>
<th>Industry Title</th><th>Industry Code</th><th>Industry Type</th>
<th>2023 Employment</th><th>2023 Percent of Occupation</th><th>2023 Percent of Industry</th>
<th>Projected 2033 Employment</th><th>Projected 2033 Percent of Occupation</th>
<th>Projected 2033 Percent of Industry</th>
<th>Employment Change, 2023-2033</th><th>Employment Percent Change, 2023-2033</th>
</tr></thead>
<tbody>
<tr><td>Total employment</td><td>TE1000</td><td>Summary</td><td>211,230</td><td>100.0</td>
<td>0.1</td><td>221,900</td><td>100.0</td><td>0.1</td><td>10,670</td><td>5.1</td></tr>
<tr><td>Utilities</td><td>22</td><td>Sector</td><td>1,800</td><td>0.9</td><td>–</td>
<td>1,850</td><td>0.8</td><td>–</td><td>50</td><td>2.8</td></tr>
</tbody></table></body></html>
```

- [ ] **Step 2: Write the failing tests** — `tests/engines/test_ep.py`:

```python
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from bls_stats.engines.ep import parse_index, parse_matrix

FIXTURES = Path(__file__).parent.parent / "fixtures" / "ep"
TS = datetime(2026, 7, 2, tzinfo=timezone.utc)


def test_parse_index_extracts_soc_codes() -> None:
    assert parse_index((FIXTURES / "index.html").read_bytes()) == ["11-1011", "15-1252"]


def test_parse_matrix_normalizes_year_headers() -> None:  # BEH §2.4
    df = parse_matrix((FIXTURES / "matrix_11-1011.html").read_bytes(), "11-1011")
    for col in ("base_year_employment", "projected_year_employment", "employment_change",
                "employment_pct_change", "industry_title", "industry_code", "occupation_code"):
        assert col in df.columns, col


def test_numbers_parsed_endash_null() -> None:
    df = parse_matrix((FIXTURES / "matrix_11-1011.html").read_bytes(), "11-1011")
    assert df.schema["base_year_employment"] == pl.Float64
    assert df["base_year_employment"][0] == 211230.0
    utilities = df.filter(pl.col("industry_code") == "22")
    assert utilities["base_year_pct_of_industry"][0] is None  # en-dash
```

- [ ] **Step 3: Run to verify failure** — `uv run pytest tests/engines/test_ep.py -v`

- [ ] **Step 4: Implement** — `src/bls_stats/engines/ep.py`:

```python
"""EP national matrix scraper (BEH §2.4). No feed — annual/on-demand trigger (ARCH §5.2)."""

from __future__ import annotations

import logging
import re
from datetime import datetime
from pathlib import Path

import httpx
import polars as pl
from bs4 import BeautifulSoup

from bls_stats.core.http import Throttle, get

log = logging.getLogger(__name__)

INDEX_URL = "https://www.bls.gov/emp/tables/industry-occupation-matrix-occupation.htm"
MATRIX_URL = "https://data.bls.gov/projections/nationalMatrix?queryParams={soc}&ioType=o"
_SOC = re.compile(r"queryParams=(\d{2}-\d{4})")

_HEADER_MAP: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"^\d{4} Employment$"), "base_year_employment"),
    (re.compile(r"^\d{4} Percent of Occupation$"), "base_year_pct_of_occupation"),
    (re.compile(r"^\d{4} Percent of Industry$"), "base_year_pct_of_industry"),
    (re.compile(r"^Projected \d{4} Employment$"), "projected_year_employment"),
    (re.compile(r"^Projected \d{4} Percent of Occupation$"), "projected_year_pct_of_occupation"),
    (re.compile(r"^Projected \d{4} Percent of Industry$"), "projected_year_pct_of_industry"),
    (re.compile(r"^Employment Change"), "employment_change"),
    (re.compile(r"^Employment Percent Change"), "employment_pct_change"),
    (re.compile(r"^Industry Title$"), "industry_title"),
    (re.compile(r"^Industry Code$"), "industry_code"),
    (re.compile(r"^Industry Type$"), "industry_type"),
]
_NUMERIC = {
    "base_year_employment", "base_year_pct_of_occupation", "base_year_pct_of_industry",
    "projected_year_employment", "projected_year_pct_of_occupation",
    "projected_year_pct_of_industry", "employment_change", "employment_pct_change",
}


class EpScrapeError(RuntimeError):
    pass


def _normalize(header: str) -> str:
    text = header.strip()
    for pattern, name in _HEADER_MAP:
        if pattern.match(text):
            return name
    return text.lower().replace(" ", "_")


def parse_index(html: bytes) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    socs = []
    for a in soup.find_all("a", href=_SOC):
        socs.append(_SOC.search(a["href"]).group(1))
    return sorted(set(socs), key=socs.index)


def parse_matrix(html: bytes, soc: str) -> pl.DataFrame:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if table is None:
        raise EpScrapeError(f"{soc}: no table in response")
    headers = [_normalize(th.get_text(" ", strip=True)) for th in table.find_all("th")]
    rows = []
    for tr in table.find_all("tr"):
        cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        if len(cells) == len(headers):
            rows.append(dict(zip(headers, cells)))
    df = pl.DataFrame(rows)
    numeric = [c for c in df.columns if c in _NUMERIC]
    return df.with_columns(
        pl.col(numeric)
        .str.replace_all(",", "")
        .str.replace_all(r"^[–—-]$", "")
        .replace("", None)
        .cast(pl.Float64, strict=False),
        pl.lit(soc).alias("occupation_code"),
    )


def fetch_matrix(
    client: httpx.Client, *, throttle: Throttle | None = None, downloaded: datetime,
    cache: Path | None = None, refresh: bool = False,
) -> pl.DataFrame:
    if cache is not None and cache.exists() and not refresh:
        log.info("ep: using cached matrix %s", cache)
        return pl.read_parquet(cache)
    throttle = throttle or Throttle(2.0)
    socs = parse_index(get(client, INDEX_URL).content)
    frames: list[pl.DataFrame] = []
    for soc in socs:
        throttle.wait()
        try:
            frames.append(parse_matrix(get(client, MATRIX_URL.format(soc=soc)).content, soc))
        except (httpx.HTTPError, EpScrapeError) as exc:
            log.warning("ep %s failed (%s) — continuing", soc, exc)  # BEH §2.4
    if not frames:
        raise EpScrapeError("all occupations failed")
    df = pl.concat(frames, how="diagonal").with_columns(
        pl.lit(downloaded).dt.cast_time_unit("us").alias("downloaded_at")
    )
    if cache is not None:
        cache.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(cache)
    return df
```

- [ ] **Step 5: Run to verify pass** — `uv run pytest tests/engines/test_ep.py -v`

- [ ] **Step 6: Commit** — `git add -A && git commit -m "feat: ep matrix scraper with header normalization and cache"`

---

### Task 18: API v2 utility engine (`engines/api_v2.py`)

**Files:**
- Create: `src/bls_stats/engines/api_v2.py`
- Test: `tests/engines/test_api_v2.py`

**Interfaces:**
- Consumes: `Settings.api_key` (Task 2), `Throttle` (Task 6).
- Produces: `fetch_series(client, settings, series_ids: list[str], start_year: int, end_year: int, *, throttle: Throttle | None = None) -> pl.DataFrame` (columns: `series_id, year, period, value, footnote_codes, latest`); `BlsApiError(RuntimeError)`. Batches ≤50 IDs/POST; enforces the 50-req/10s cap via `Throttle(0.25)`; **checks the `message` array** — errors arrive as HTTP 200 `REQUEST_SUCCEEDED` (ARCH §6.1).

- [ ] **Step 1: Write the failing tests** — `tests/engines/test_api_v2.py`:

```python
import json

import httpx
import polars as pl
import pytest

from bls_stats.core.config import Settings
from bls_stats.core.http import Throttle
from bls_stats.engines.api_v2 import BlsApiError, fetch_series

SETTINGS = Settings(api_key="test-key")
NO_THROTTLE = Throttle(0, clock=lambda: 0.0, sleep=lambda _s: None)


def _payload(series: list[dict], messages: list[str] | None = None) -> dict:
    return {"status": "REQUEST_SUCCEEDED", "message": messages or [],
            "Results": {"series": series}}


def _series(sid: str) -> dict:
    return {"seriesID": sid, "data": [
        {"year": "2026", "period": "M05", "value": "159180",
         "footnotes": [{"code": "P", "text": "Preliminary."}], "latest": "true"},
        {"year": "2026", "period": "M04", "value": "159123", "footnotes": [{}]},
    ]}


def test_fetch_parses_rows_and_key_in_payload() -> None:
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        seen.append(body)
        return httpx.Response(200, json=_payload([_series(s) for s in body["seriesid"]]))

    client = httpx.Client(transport=httpx.MockTransport(handler))
    df = fetch_series(client, SETTINGS, ["CES0000000001"], 2026, 2026, throttle=NO_THROTTLE)
    assert seen[0]["registrationkey"] == "test-key"
    assert df.height == 2
    assert df.schema["value"] == pl.Float64
    assert df["footnote_codes"].to_list() == ["P", ""]


def test_batches_of_fifty() -> None:
    batches: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        ids = json.loads(request.content)["seriesid"]
        batches.append(len(ids))
        return httpx.Response(200, json=_payload([_series(s) for s in ids]))

    client = httpx.Client(transport=httpx.MockTransport(handler))
    fetch_series(client, SETTINGS, [f"S{i:03d}" for i in range(120)], 2026, 2026,
                 throttle=NO_THROTTLE)
    assert batches == [50, 50, 20]


def test_hidden_error_in_message_array_raises() -> None:  # ARCH §6.1 quirk
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_payload([], ["Series does not exist for Series XXX"]))

    client = httpx.Client(transport=httpx.MockTransport(handler))
    with pytest.raises(BlsApiError, match="does not exist"):
        fetch_series(client, SETTINGS, ["XXX"], 2026, 2026, throttle=NO_THROTTLE)


def test_missing_api_key_raises() -> None:
    with pytest.raises(BlsApiError, match="BLS_API_KEY"):
        fetch_series(httpx.Client(), Settings(), ["X"], 2026, 2026, throttle=NO_THROTTLE)
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/engines/test_api_v2.py -v`

- [ ] **Step 3: Implement** — `src/bls_stats/engines/api_v2.py`:

```python
"""BLS API v2 utility engine (ARCH §6.1): targeted fetches and spot checks only."""

from __future__ import annotations

import logging

import httpx
import polars as pl

from bls_stats.core.config import Settings
from bls_stats.core.http import Throttle

log = logging.getLogger(__name__)

ENDPOINT = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BATCH = 50  # registered-key limit per query


class BlsApiError(RuntimeError):
    pass


def fetch_series(
    client: httpx.Client, settings: Settings, series_ids: list[str],
    start_year: int, end_year: int, *, throttle: Throttle | None = None,
) -> pl.DataFrame:
    if settings.api_key is None:
        raise BlsApiError("BLS_API_KEY is not configured")
    throttle = throttle if throttle is not None else Throttle(0.25)  # 50 req / 10 s cap
    rows: list[dict] = []
    for i in range(0, len(series_ids), BATCH):
        batch = series_ids[i : i + BATCH]
        throttle.wait()
        resp = client.post(ENDPOINT, json={
            "seriesid": batch, "startyear": str(start_year), "endyear": str(end_year),
            "registrationkey": settings.api_key,
        })
        resp.raise_for_status()
        payload = resp.json()
        messages = [m for m in payload.get("message", []) if m]
        if payload.get("status") != "REQUEST_SUCCEEDED":
            raise BlsApiError(f"API status {payload.get('status')}: {messages}")
        errors = [m for m in messages if "does not exist" in m or "No Data" in m]
        if errors:  # HTTP 200 + REQUEST_SUCCEEDED can still be an error (ARCH §6.1)
            raise BlsApiError("; ".join(errors))
        for series in payload["Results"]["series"]:
            for obs in series.get("data", []):
                rows.append({
                    "series_id": series["seriesID"],
                    "year": int(obs["year"]),
                    "period": obs["period"],
                    "value": float(obs["value"]) if obs["value"] not in ("", "-") else None,
                    "footnote_codes": ",".join(
                        f["code"] for f in obs.get("footnotes", []) if f.get("code")
                    ),
                    "latest": obs.get("latest") == "true",
                })
    return pl.DataFrame(
        rows,
        schema={"series_id": pl.Utf8, "year": pl.Int32, "period": pl.Utf8,
                "value": pl.Float64, "footnote_codes": pl.Utf8, "latest": pl.Boolean},
    )
```

- [ ] **Step 4: Run to verify pass** — `uv run pytest tests/engines/test_api_v2.py -v`

- [ ] **Step 5: Milestone D checkpoint + commit**

Run: `uv run pytest -q && uv run ruff check .` — all green.
Commit: `git commit -am "feat: api v2 utility engine with batching, rate cap, hidden-error detection"`

---

### Task 19: CPS metadata enrichment (`enrich/cps.py`)

**Files:**
- Create: `src/bls_stats/enrich/cps.py`
- Test: `tests/enrich/test_cps.py`

**Interfaces:**
- Consumes: `get`/`download`/`Throttle` (Task 6), `VintageStore.append_state`-style writes via its own paths (uses `Store` for export).
- Produces (BEH §2.5 contract; CLI `metadata` consumes):
  - `list_mapping_files(html: bytes) -> list[str]` — `ln.*` names from the directory listing (source of truth per BEH §2.5), excluding data files and `ln.series`
  - `fetch_metadata(client, dest_dir: Path, *, refresh: bool = False) -> dict[str, pl.DataFrame]` — downloads `ln.series` + every mapping into `dest_dir`, keeps a `manifest.json` of sha256s, skips unchanged files unless `refresh`; returns `{"series": df, "ages": df, ...}`
  - `enrich(obs: pl.DataFrame, meta: dict[str, pl.DataFrame]) -> pl.DataFrame` — left-join catalog on `series_id`, then each mapping on its `<name>_code` column, then footnote text; **row count never changes**
  - `export_metadata(store, meta) -> None` — Delta tables at `cps/metadata/series` and `cps/metadata/mappings/{name}`, each with `program='cps'` column (snapshot-replace: `mode="overwrite"`)

- [ ] **Step 1: Write the failing tests** — `tests/enrich/test_cps.py`:

```python
import polars as pl

from bls_stats.enrich.cps import enrich, list_mapping_files

LISTING = b"""<html><body>
<a href="ln.ages">ln.ages</a> <a href="ln.sexs">ln.sexs</a>
<a href="ln.footnote">ln.footnote</a> <a href="ln.series">ln.series</a>
<a href="ln.data.1.AllData">ln.data.1.AllData</a> <a href="ln.txt">ln.txt</a>
</body></html>"""


def test_list_mapping_files_excludes_series_data_and_txt() -> None:
    assert list_mapping_files(LISTING) == ["ln.ages", "ln.footnote", "ln.sexs"]


def _meta() -> dict[str, pl.DataFrame]:
    return {
        "series": pl.DataFrame({
            "series_id": ["LNS14000000"], "ages_code": ["00"], "series_title": ["Unemployment rate"],
        }),
        "ages": pl.DataFrame({"ages_code": ["00", "16"], "ages_text": ["All ages", "16+"]}),
        "footnote": pl.DataFrame({"footnote_code": ["P"], "footnote_text": ["Preliminary."]}),
    }


def _obs() -> pl.DataFrame:
    return pl.DataFrame({
        "series_id": ["LNS14000000", "LNU99999999"],  # second id NOT in catalog
        "value": [4.2, 1.0],
        "footnote_codes": ["P", ""],
    })


def test_enrich_left_joins_never_drop_rows() -> None:  # BEH §2.5
    out = enrich(_obs(), _meta())
    assert out.height == 2
    assert out.filter(pl.col("series_id") == "LNS14000000")["ages_text"][0] == "All ages"
    assert out.filter(pl.col("series_id") == "LNU99999999")["series_title"][0] is None


def test_enrich_resolves_footnotes() -> None:
    out = enrich(_obs(), _meta())
    assert out.filter(pl.col("series_id") == "LNS14000000")["footnote_text"][0] == "Preliminary."
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/enrich/test_cps.py -v`

- [ ] **Step 3: Implement** — `src/bls_stats/enrich/cps.py`:

```python
"""CPS metadata: series catalog + ln.* mapping tables, enrichment joins (BEH §2.5)."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from pathlib import Path

import httpx
import polars as pl
from bs4 import BeautifulSoup

from bls_stats.core.http import Throttle, download
from bls_stats.storage.backend import Store

log = logging.getLogger(__name__)

BASE = "https://download.bls.gov/pub/time.series/ln/"
_MAPPING = re.compile(r"^ln\.[a-z_]+$")
_EXCLUDE = {"ln.series", "ln.txt", "ln.contacts"}


def list_mapping_files(html: bytes) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    names = {
        a.get_text(strip=True)
        for a in soup.find_all("a")
        if _MAPPING.match(a.get_text(strip=True)) and a.get_text(strip=True) not in _EXCLUDE
    }
    return sorted(names)


def _read_tsv(path: Path) -> pl.DataFrame:
    df = pl.read_csv(path, separator="\t", infer_schema_length=0)  # all Utf8 — code columns
    df.columns = [c.strip() for c in df.columns]
    return df.with_columns(pl.col(pl.Utf8).str.strip_chars())


def fetch_metadata(
    client: httpx.Client, dest_dir: Path, *, refresh: bool = False
) -> dict[str, pl.DataFrame]:
    from bls_stats.core.http import get

    dest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = dest_dir / "manifest.json"
    manifest: dict[str, str] = (
        json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    )
    listing = get(client, BASE).content
    names = ["ln.series", *list_mapping_files(listing)]
    throttle = Throttle(2.0)
    out: dict[str, pl.DataFrame] = {}
    for name in names:
        local = dest_dir / name
        if local.exists() and not refresh and manifest.get(name):
            digest = hashlib.sha256(local.read_bytes()).hexdigest()
            if digest == manifest[name]:
                out[name.removeprefix("ln.")] = _read_tsv(local)
                continue
        throttle.wait()
        download(client, BASE + name, local)
        manifest[name] = hashlib.sha256(local.read_bytes()).hexdigest()
        out[name.removeprefix("ln.")] = _read_tsv(local)
        log.info("cps metadata: fetched %s", name)
    manifest_path.write_text(json.dumps(manifest, indent=1))
    return out


def enrich(obs: pl.DataFrame, meta: dict[str, pl.DataFrame]) -> pl.DataFrame:
    before = obs.height
    out = obs.join(meta["series"], on="series_id", how="left")
    for name, mapping in meta.items():
        if name in ("series", "footnote"):
            continue
        code_col = f"{name}_code" if f"{name}_code" in mapping.columns else mapping.columns[0]
        if code_col in out.columns:
            out = out.join(mapping, on=code_col, how="left")
    if "footnote" in meta and "footnote_codes" in out.columns:
        lookup = dict(meta["footnote"].select("footnote_code", "footnote_text").iter_rows())
        out = out.with_columns(
            pl.col("footnote_codes")
            .str.split(",")
            .list.eval(pl.element().str.strip_chars().replace_strict(lookup, default=None))
            .list.drop_nulls()
            .list.join("; ")
            .replace("", None)
            .alias("footnote_text")
        )
    assert out.height == before, "enrichment must never drop observations (BEH §2.5)"
    return out


def export_metadata(store: Store, meta: dict[str, pl.DataFrame]) -> None:
    for name, df in meta.items():
        tagged = df.with_columns(pl.lit("cps").alias("program"))
        uri = (
            f"{store.uri}/cps/metadata/series" if name == "series"
            else f"{store.uri}/cps/metadata/mappings/{name}"
        )
        tagged.write_delta(uri, mode="overwrite", storage_options=store.storage_options)
        log.info("cps metadata: exported %s (%d rows)", name, df.height)
```

Note: `export_metadata` accesses `store.uri`/`store.storage_options` (VintageStore attributes) — add both to the `Store` protocol in `storage/backend.py` as read-only properties.

- [ ] **Step 4: Run to verify pass** — `uv run pytest tests/enrich/test_cps.py -v`

- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat: cps metadata fetch/enrich/export with integrity manifest"`

---

### Task 20: Pipeline (`pipeline.py`)

**Files:**
- Create: `src/bls_stats/pipeline.py`
- Test: `tests/test_pipeline.py`

**Interfaces:**
- Consumes: everything above. Injectable seams for tests: `clock: Callable[[], datetime]`, `poll_fn` (defaults to `feeds.poll`), `fetch_fn(client, program, slots, dest_dir, downloaded) -> pl.DataFrame` (defaults to `_fetch_event` which dispatches by program), `fresh_fn` (defaults to `labstat.is_fresh`).
- Produces (CLI consumes):
  - `ValidationError(RuntimeError)`
  - `run_ingest(settings, store, programs: list[str] | None = None, *, dry_run: bool = False, clock=None, poll_fn=None, fetch_fn=None, fresh_fn=None) -> int` — exit code 0/1/2 (ARCH §7.4: deferrals alone → 0)
  - `run_backfill(settings, store, program: str, start: str, end: str, *, dry_run: bool = False, clock=None, fetch_fn=None) -> int`
  - `stamp(df: pl.DataFrame, ref_date, release_date, revision, benchmark, source, downloaded) -> pl.DataFrame`

- [ ] **Step 1: Write the failing tests** — `tests/test_pipeline.py`:

```python
from datetime import date, datetime, timezone

import polars as pl
import pytest

from bls_stats.core.config import Settings
from bls_stats.pipeline import run_ingest, stamp
from bls_stats.releases.feeds import Release
from bls_stats.storage.delta import VintageStore
from bls_stats.vintage.ledger import Ledger

NOW = datetime(2026, 7, 2, 13, 0, tzinfo=timezone.utc)
CLOCK = lambda: NOW  # noqa: E731
LATER = datetime(2026, 8, 7, 13, 0, tzinfo=timezone.utc)
LATER_CLOCK = lambda: LATER  # noqa: E731
JUNE_RELEASE = Release("ces", date(2026, 7, 2), 2026, 6, False)


def fake_fetch(refs: list[date] | None = None, rows_per_ref: int = 3):
    def _fetch(client, program, slots, dest_dir, downloaded) -> pl.DataFrame:
        wanted = refs if refs is not None else [s.ref_date for s in slots]
        return pl.DataFrame(
            {
                "series_id": [f"CES{i:010d}" for r in wanted for i in range(rows_per_ref)],
                "value": [1.0] * rows_per_ref * len(wanted),
                "footnote_codes": [""] * rows_per_ref * len(wanted),
                "ref_date": [r for r in wanted for _ in range(rows_per_ref)],
            },
            schema={"series_id": pl.Utf8, "value": pl.Float64,
                    "footnote_codes": pl.Utf8, "ref_date": pl.Date},
        )
    return _fetch


@pytest.fixture()
def store(tmp_path) -> VintageStore:
    return VintageStore(str(tmp_path / "store"))


def _ingest(store, **kw):
    defaults = dict(
        programs=["ces"], clock=CLOCK,
        poll_fn=lambda client, programs: [JUNE_RELEASE],
        fetch_fn=fake_fetch(), fresh_fn=lambda client, program, rd: True,
    )
    return run_ingest(Settings(), store, **(defaults | kw))


def test_happy_path_commits_three_slots_and_records(store) -> None:
    assert _ingest(store) == 0
    obs = store.scan_observations("ces").collect()
    assert obs.height == 9  # 3 slots × 3 rows
    assert set(zip(obs["revision"].to_list(), obs["benchmark"].to_list())) == {(0, 0), (1, 0), (2, 0)}
    led = Ledger(store).resolved()
    assert led.height == 3 and (led["status"] == "ingested").all()


def test_rerun_is_noop(store) -> None:
    _ingest(store)
    _ingest(store)
    assert store.scan_observations("ces").collect().height == 9  # no duplicates


def test_crash_between_commit_and_record_repairs(store, monkeypatch) -> None:
    calls = {"n": 0}
    original = Ledger.record

    def crashing_record(self, records):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("simulated crash after data commit")
        return original(self, records)

    monkeypatch.setattr(Ledger, "record", crashing_record)
    assert _ingest(store) == 1  # first run: event failed after commit
    assert _ingest(store) == 0  # rerun: presence check repairs, no re-append
    assert store.scan_observations("ces").collect().height == 9
    assert (Ledger(store).resolved()["status"] == "ingested").all()


def test_stale_file_defers_and_exits_zero(store) -> None:
    assert _ingest(store, fresh_fn=lambda client, program, rd: False) == 0  # ARCH §7.4
    led = Ledger(store).resolved()
    assert (led["status"] == "deferred").all()
    assert store.scan_observations("ces") is None  # nothing committed


def test_deferred_event_retried_next_run(store) -> None:
    _ingest(store, fresh_fn=lambda client, program, rd: False)
    assert _ingest(store, clock=LATER_CLOCK) == 0  # file now fresh (later run)
    assert (Ledger(store).resolved()["status"] == "ingested").all()


def test_superseded_deferred_becomes_missed(store) -> None:  # ARCH §5.3 transition
    _ingest(store, fresh_fn=lambda client, program, rd: False)  # June deferred
    july = Release("ces", date(2026, 8, 7), 2026, 7, False)
    _ingest(store, clock=LATER_CLOCK, poll_fn=lambda client, programs: [july])  # newer release ingests (later run)
    led = Ledger(store).resolved()
    june = led.filter(pl.col("release_date") == date(2026, 7, 2))
    assert june.height == 3 and (june["status"] == "missed").all()


def test_empty_slice_defers(store) -> None:
    assert _ingest(store, fetch_fn=fake_fetch(refs=[])) == 0
    assert (Ledger(store).resolved()["status"] == "deferred").all()


def test_fetch_error_isolates_and_exits_two_when_all_fail(store) -> None:
    def boom(client, program, slots, dest_dir, downloaded):
        raise RuntimeError("download failed")

    assert _ingest(store, fetch_fn=boom) == 2


def test_dry_run_commits_nothing(store) -> None:
    assert _ingest(store, dry_run=True) == 0
    assert store.scan_observations("ces") is None
    assert store.read_state("ledger") is None


def test_stamp_types() -> None:
    df = stamp(
        pl.DataFrame({"series_id": ["x"], "value": [1.0], "footnote_codes": [""],
                      "ref_date": [date(2026, 6, 12)]}),
        date(2026, 6, 12), date(2026, 7, 2), 0, 0, "increment", NOW,
    )
    assert df.schema["revision"] == pl.Int16 and df.schema["source"] == pl.Utf8
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/test_pipeline.py -v`

- [ ] **Step 3: Implement** — `src/bls_stats/pipeline.py`:

```python
"""Orchestrator (ARCH §7): detect → expand → fetch → validate → commit → record."""

from __future__ import annotations

import logging
import tempfile
from collections.abc import Callable
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

from bls_stats.core.config import Settings, storage_options
from bls_stats.core.http import build_client
from bls_stats.core.periods import Period, ref_date, reference_periods
from bls_stats.registry import REGISTRY
from bls_stats.releases import feeds
from bls_stats.releases.calendar import filter_published
from bls_stats.releases.profiles import Slot, expand
from bls_stats.storage.backend import Store
from bls_stats.vintage.ledger import Ledger, SlotRecord

log = logging.getLogger(__name__)


class ValidationError(RuntimeError):
    pass


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def stamp(
    df: pl.DataFrame, ref: date | None, release: date, revision: int | None,
    benchmark: int | None, source: str, downloaded: datetime,
) -> pl.DataFrame:
    out = df.with_columns(
        pl.lit(release).alias("release_date"),
        pl.lit(revision, dtype=pl.Int16).alias("revision"),
        pl.lit(benchmark, dtype=pl.Int16).alias("benchmark"),
        pl.lit(source).alias("source"),
        pl.lit(downloaded).dt.cast_time_unit("us").alias("downloaded"),
    )
    if "ref_date" not in out.columns:
        out = out.with_columns(pl.lit(ref).alias("ref_date"))
    return out


def validate(df: pl.DataFrame, program: str, comparator_count: int | None) -> None:
    """ARCH §7.3 gates 1 & 3 (gate 2 — emptiness — is handled by the caller as a deferral)."""
    spec = REGISTRY[program]
    for col in spec.unit_columns:
        if col not in df.columns:
            raise ValidationError(f"{program}: missing unit column {col!r}")
        if df.schema[col] != pl.Utf8:
            raise ValidationError(f"{program}: {col} must be Utf8, got {df.schema[col]}")
    if "value" in df.columns:
        null_rate = df["value"].null_count() / max(df.height, 1)
        if null_rate > spec.null_rate_max:
            raise ValidationError(f"{program}: value null-rate {null_rate:.1%} > {spec.null_rate_max:.0%}")
    if comparator_count is not None and comparator_count > 0:
        lo, hi = comparator_count * (1 - spec.row_band), comparator_count * (1 + spec.row_band)
        if not lo <= df.height <= hi:
            raise ValidationError(
                f"{program}: row count {df.height} outside ±{spec.row_band:.0%} of {comparator_count}"
            )


def _fetch_event(client, program: str, slots: list[Slot], dest_dir: Path,
                 downloaded: datetime) -> pl.DataFrame:
    """Default fetch dispatch: one download per event (ARCH §6.3)."""
    spec = REGISTRY[program]
    refs = [s.ref_date for s in slots]
    if program == "qcew":
        from bls_stats.engines.qcew import fetch_year

        frames = []
        for year in sorted({r.year for r in refs}):
            quarters = sorted({(r.month + 2) // 3 for r in refs if r.year == year})
            frames.append(fetch_year(client, year, quarters, dest_dir, downloaded))
        return pl.concat(frames, how="vertical_relaxed")
    if program == "oews":
        from bls_stats.engines.oews import fetch_year as fetch_oews

        return fetch_oews(client, refs[0].year, dest_dir, downloaded)
    if program == "ep":
        from bls_stats.engines.ep import fetch_matrix

        return fetch_matrix(client, downloaded=downloaded)
    from bls_stats.engines.labstat import fetch

    periods: list[Period] = [
        (r.year, r.month if spec.frequency == "monthly" else (r.month + 2) // 3) for r in refs
    ]
    url = spec.benchmark_url if any(s.kind == "benchmark_window" for s in slots) else spec.increment_url
    assert url is not None
    return fetch(client, program, url, periods, dest_dir, downloaded)


def _expire_superseded(ledger: Ledger, program: str, newer: date, now: datetime,
                       dry_run: bool) -> None:
    """ARCH §5.3: a deferred slot whose live-vintage window closed (a newer release for the
    same program ingested) transitions to missed — never by wall-clock timeout."""
    stale = ledger.resolved().filter(
        (pl.col("program") == program) & (pl.col("status") == "deferred")
        & (pl.col("release_date") < newer)
    )
    if stale.height and not dry_run:
        ledger.record([
            SlotRecord(program, r["ref_date"], r["release_date"], r["revision"],
                       r["benchmark"], "increment", 0, "missed", now)
            for r in stale.iter_rows(named=True)
        ])
        log.warning("%s: %d deferred slot(s) superseded by %s -> missed",
                    program, stale.height, newer)


def _comparator(ledger: Ledger, program: str, revision: int | None) -> int | None:
    """ARCH §7.3: most recent ingested row_count for the same program and slot type."""
    got = (
        ledger.resolved()
        .filter(
            (pl.col("program") == program) & (pl.col("status") == "ingested")
            & pl.col("revision").eq_missing(pl.lit(revision, dtype=pl.Int16))
        )
        .sort("ingested_at", descending=True)
    )
    return int(got["row_count"][0]) if got.height else None


def run_ingest(
    settings: Settings, store: Store, programs: list[str] | None = None, *,
    dry_run: bool = False,
    clock: Callable[[], datetime] | None = None,
    poll_fn=None, fetch_fn=None, fresh_fn=None,
) -> int:
    clock = clock or _utcnow
    programs = programs or [p for p in REGISTRY if p != "ep"]  # ep: ARCH §5.2 exception
    poll_fn = poll_fn or feeds.poll
    fetch_fn = fetch_fn or _fetch_event
    if fresh_fn is None:
        from bls_stats.engines.labstat import is_fresh as fresh_fn  # noqa: PLW0127

    client = build_client(settings)
    ledger = Ledger(store)
    outcomes: list[str] = []
    for release in poll_fn(client, programs):
        slots = [
            s for s in expand(release, lambda rd: ledger.prior_benchmark_count(release.program, rd))
            if ledger.slot_status(release.program, s.ref_date, release.release_date,
                                  s.revision, s.benchmark) not in ("ingested", "missed")
        ]
        if not slots:
            continue
        outcome = _process_event(release, slots, settings, store, ledger, client,
                                 dry_run=dry_run, now=clock(), fetch_fn=fetch_fn,
                                 fresh_fn=fresh_fn)
        if outcome == "ok":
            _expire_superseded(ledger, release.program, release.release_date, clock(), dry_run)
        outcomes.append(outcome)
    failed = outcomes.count("failed")
    if failed and failed == len(outcomes):
        return 2
    return 1 if failed or "partial" in outcomes else 0


def _process_event(release, slots, settings, store, ledger, client, *,
                   dry_run: bool, now: datetime, fetch_fn, fresh_fn) -> str:
    program = release.program
    label = f"{program} release {release.release_date}"
    appended = 0

    def _record(status: str, slot: Slot, row_count: int = 0) -> None:
        if not dry_run:
            ledger.record([SlotRecord(program, slot.ref_date, release.release_date,
                                      slot.revision, slot.benchmark, "increment",
                                      row_count, status, now)])

    try:
        spec = REGISTRY[program]
        if spec.increment_url and spec.increment_url.startswith(
            "https://download.bls.gov"
        ) and not fresh_fn(client, program, release.release_date):
            log.warning("%s: file not yet fresh — deferring %d slot(s)", label, len(slots))
            for slot in slots:
                _record("deferred", slot)
            return "deferred"
        with tempfile.TemporaryDirectory() as td:
            df = fetch_fn(client, program, slots, Path(td), now)
        committed = 0
        for slot in slots:
            piece = df.filter(pl.col("ref_date") == slot.ref_date) if "ref_date" in df.columns else df
            if piece.is_empty():
                log.warning("%s: empty slice for %s — deferred", label, slot.ref_date)
                _record("deferred", slot)
                continue
            validate(piece, program, _comparator(ledger, program, slot.revision))
            stamped = stamp(piece, slot.ref_date, release.release_date,
                            slot.revision, slot.benchmark, "increment", now)
            if store.slot_exists(program, slot.ref_date, release.release_date,
                                 slot.revision, slot.benchmark):
                log.warning("%s: slot %s already committed — repairing ledger only", label, slot)
            elif not dry_run:
                store.append_observations(program, stamped)
                appended += 1
            _record("ingested", slot, stamped.height)
            committed += 1
        log.info("%s: %d/%d slots committed", label, committed, len(slots))
        return "ok" if committed else "deferred"
    except Exception:
        log.exception("%s: event failed", label)
        return "partial" if appended else "failed"  # data committed => partial (ARCH §7.4)


def run_backfill(
    settings: Settings, store: Store, program: str, start: str, end: str, *,
    dry_run: bool = False, clock: Callable[[], datetime] | None = None, fetch_fn=None,
) -> int:
    clock = clock or _utcnow
    now = clock()
    snapshot_date = now.date()
    cal = store.read_state("release_calendar")
    if cal is None:
        log.error("release calendar missing — run `bls-stats calendar build` first (ARCH §8)")
        return 2
    periods = filter_published(program, reference_periods(program, start, end), cal)
    if not periods:
        log.warning("%s: no published periods in range", program)
        return 0
    client = build_client(settings)
    ledger = Ledger(store)
    fetch_fn = fetch_fn or _fetch_event
    slots = [Slot(ref_date(program, y, p), None, None, "backfill") for y, p in periods]  # type: ignore[arg-type]
    todo = [s for s in slots
            if ledger.slot_status(program, s.ref_date, snapshot_date, None, None) != "ingested"]
    if not todo:
        log.info("%s: backfill already complete for range", program)
        return 0
    try:
        with tempfile.TemporaryDirectory() as td:
            df = fetch_fn(client, program, todo, Path(td), now)
        records = []
        for slot in todo:
            piece = df.filter(pl.col("ref_date") == slot.ref_date) if "ref_date" in df.columns else df
            if piece.is_empty():
                continue
            stamped = stamp(piece, slot.ref_date, snapshot_date, None, None, "backfill", now)
            if not store.slot_exists(program, slot.ref_date, snapshot_date, None, None):
                if not dry_run:
                    store.append_observations(program, stamped)
            records.append(SlotRecord(program, slot.ref_date, snapshot_date, None, None,
                                      "backfill", stamped.height, "ingested", now))
        if not dry_run:
            ledger.record(records)
        log.info("%s: backfilled %d period(s)", program, len(records))
        return 0
    except Exception:
        log.exception("%s: backfill failed", program)
        return 2
```

Implementation notes for the executor: `Slot.ref_date` is typed non-null; backfill reuses it with `revision=benchmark=None` via the `stamp`/ledger path (the `type: ignore` is deliberate and scoped). QCEW backfill memory discipline: for multi-year backfills the CLI loops years and calls `run_backfill` per year (Task 21) — `_fetch_event` already fetches per year.

- [ ] **Step 4: Run to verify pass** — `uv run pytest tests/test_pipeline.py -v`

- [ ] **Step 5: Commit** — `git commit -am "feat: ingest/backfill pipeline with presence-check repair and deferrals"`

---

### Task 21: CLI (`cli.py`)

**Files:**
- Modify: `src/bls_stats/cli.py` (replace the Task 1 placeholder)
- Test: `tests/test_cli.py`

**Interfaces:**
- Consumes: everything. Produces the ARCH §8 command table exactly:
  `backfill`, `ingest`, `calendar build|refresh|show`, `gaps [--strict]`, `store info|maintain|query`, `metadata fetch|export|enrich`, `doctor`.

- [ ] **Step 1: Write the failing tests** — `tests/test_cli.py`:

```python
from typer.testing import CliRunner

from bls_stats.cli import app

runner = CliRunner()


def test_help_lists_all_commands() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for cmd in ("backfill", "ingest", "calendar", "gaps", "store", "metadata", "doctor"):
        assert cmd in result.output


def test_ingest_dry_run_smoke(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("BLS_STORE_URI", str(tmp_path / "store"))
    monkeypatch.setattr("bls_stats.pipeline.run_ingest", lambda *a, **k: 0)
    result = runner.invoke(app, ["ingest", "--dry-run"])
    assert result.exit_code == 0


def test_ingest_exit_code_propagates(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("BLS_STORE_URI", str(tmp_path / "store"))
    monkeypatch.setattr("bls_stats.pipeline.run_ingest", lambda *a, **k: 1)
    result = runner.invoke(app, ["ingest"])
    assert result.exit_code == 1


def test_backfill_requires_program_and_range() -> None:
    result = runner.invoke(app, ["backfill"])
    assert result.exit_code != 0
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/test_cli.py -v`

- [ ] **Step 3: Implement** — `src/bls_stats/cli.py`:

```python
"""typer CLI (ARCH §8). Thin adapters only — logic lives in pipeline/releases/storage."""

from __future__ import annotations

import logging
import sys
from datetime import date

import typer

from bls_stats.core.config import load_settings, storage_options

app = typer.Typer(help="Vintage-aware BLS data downloads and ingest.")
calendar_app = typer.Typer(help="Release-date calendar.")
store_app = typer.Typer(help="Inspect and maintain the vintage store.")
metadata_app = typer.Typer(help="CPS dimension tables.")
app.add_typer(calendar_app, name="calendar")
app.add_typer(store_app, name="store")
app.add_typer(metadata_app, name="metadata")

PROGRAMS = ["ces", "sae", "jolts", "cps", "bed", "qcew", "oews", "ep"]


def _setup() -> tuple:
    settings = load_settings()
    logging.basicConfig(
        stream=sys.stderr, level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    from bls_stats.storage.delta import VintageStore

    return settings, VintageStore(settings.store_uri, storage_options(settings))


@app.command()
def ingest(
    program: str | None = typer.Option(None, help="One program; default: all feed-driven."),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    """Daily incremental ingest — the one daily crontab line (ARCH §8)."""
    import bls_stats.pipeline as pipeline

    settings, store = _setup()
    programs = [program] if program else None
    raise typer.Exit(pipeline.run_ingest(settings, store, programs, dry_run=dry_run))


@app.command()
def backfill(
    program: str = typer.Option(...),
    start: str = typer.Option(..., help="YYYY/MM, YYYY/Q, or YYYY per program frequency"),
    end: str = typer.Option(...),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    """Stage-1 historical seed (ARCH §8). QCEW runs per year for memory discipline."""
    import bls_stats.pipeline as pipeline
    from bls_stats.core.periods import reference_periods

    settings, store = _setup()
    if program == "qcew":
        years = sorted({y for y, _ in reference_periods("qcew", start, end)})
        codes = [
            pipeline.run_backfill(settings, store, "qcew", f"{y}/1", f"{y}/4", dry_run=dry_run)
            for y in years
        ]
        raise typer.Exit(max(codes))
    raise typer.Exit(pipeline.run_backfill(settings, store, program, start, end, dry_run=dry_run))


@calendar_app.command("build")
def calendar_build() -> None:
    """Full archive+schedule scrape with lapse overlay (ARCH §5.4)."""
    from bls_stats.core.http import build_client
    from bls_stats.releases.calendar import build

    settings, store = _setup()
    cal = build(build_client(settings), [p for p in PROGRAMS if p != "ep"])
    store.append_state("release_calendar", cal)
    typer.echo(f"calendar: {cal.height} rows")


@calendar_app.command("refresh")
def calendar_refresh() -> None:
    """Cheap keep-current poll from the feeds."""
    from bls_stats.core.http import build_client
    from bls_stats.core.periods import ref_date as _rd
    from bls_stats.releases.calendar import CALENDAR_SCHEMA
    from bls_stats.releases.feeds import poll

    import polars as pl

    settings, store = _setup()
    releases = poll(build_client(settings), [p for p in PROGRAMS if p != "ep"])
    rows = [
        {"program": r.program, "ref_date": _rd(r.program, r.ref_year, r.ref_period),
         "release_date": r.release_date, "original_release": None, "is_benchmark": r.is_benchmark}
        for r in releases
    ]
    store.append_state("release_calendar", pl.DataFrame(rows, schema=CALENDAR_SCHEMA))
    typer.echo(f"calendar: appended {len(rows)} rows from feeds")


@calendar_app.command("show")
def calendar_show(program: str = typer.Option(...)) -> None:
    import polars as pl

    _, store = _setup()
    cal = store.read_state("release_calendar")
    if cal is None:
        typer.echo("no calendar — run `bls-stats calendar build`", err=True)
        raise typer.Exit(1)
    typer.echo(str(cal.filter(pl.col("program") == program).sort("ref_date")))


@app.command()
def gaps(
    program: str | None = typer.Option(None),
    strict: bool = typer.Option(False, "--strict", help="missed prints also exit non-zero"),
) -> None:
    """Unexplained gaps exit non-zero; recorded missed/deferred are acknowledged (ARCH §8)."""
    import polars as pl

    from bls_stats.releases.calendar import find_gaps
    from bls_stats.vintage.ledger import Ledger

    _, store = _setup()
    cal = store.read_state("release_calendar")
    if cal is None:
        typer.echo("no calendar — run `bls-stats calendar build`", err=True)
        raise typer.Exit(1)
    if program:
        cal = cal.filter(pl.col("program") == program)
    calendar_gaps = find_gaps(cal)
    ledger = Ledger(store).resolved()
    acknowledged = ledger.filter(pl.col("status").is_in(["missed", "deferred"]))
    unexplained = calendar_gaps.join(
        ledger.select("program", "ref_date").unique(), on=["program", "ref_date"], how="anti"
    )
    typer.echo(f"unexplained: {unexplained.height}  acknowledged: {acknowledged.height}")
    if unexplained.height:
        typer.echo(str(unexplained))
    missed = acknowledged.filter(pl.col("status") == "missed")
    raise typer.Exit(1 if (unexplained.height or (strict and missed.height)) else 0)


@store_app.command("info")
def store_info(program: str | None = typer.Option(None)) -> None:
    import polars as pl

    _, store = _setup()
    for name in [program] if program else PROGRAMS:
        lf = store.scan_observations(name)
        if lf is None:
            typer.echo(f"{name}: (empty)")
            continue
        summary = lf.select(
            pl.len().alias("rows"), pl.col("release_date").min().alias("first_vintage"),
            pl.col("release_date").max().alias("latest_vintage"),
        ).collect()
        typer.echo(f"{name}: {summary.row(0)}")


@store_app.command("maintain")
def store_maintain() -> None:
    """Delta optimize/compact + vacuum — the weekly crontab line (ARCH §4.1)."""
    from deltalake import DeltaTable

    _, store = _setup()
    for name in PROGRAMS:
        if store.scan_observations(name) is None:
            continue
        dt = DeltaTable(store.observations_uri(name), storage_options=store.storage_options)
        dt.optimize.compact()
        dt.vacuum(retention_hours=24 * 7, enforce_retention_duration=True, dry_run=False)
        typer.echo(f"{name}: optimized + vacuumed")


@store_app.command("query")
def store_query(
    program: str = typer.Option(...),
    ref_date: str = typer.Option(..., help="YYYY-MM-DD"),
    as_of: str | None = typer.Option(None, help="YYYY-MM-DD point-in-time (inclusive)"),
    all_vintages: bool = typer.Option(False, "--all-vintages"),
) -> None:
    import polars as pl

    from bls_stats.registry import REGISTRY
    from bls_stats.storage.reads import as_of as as_of_read, latest

    _, store = _setup()
    lf = store.scan_observations(program)
    if lf is None:
        typer.echo(f"{program}: (empty)", err=True)
        raise typer.Exit(1)
    lf = lf.filter(pl.col("ref_date") == date.fromisoformat(ref_date))
    units = list(REGISTRY[program].unit_columns)
    if all_vintages:
        out = lf.sort("release_date").collect()
    elif as_of:
        out = as_of_read(lf, units, date.fromisoformat(as_of)).collect()
    else:
        out = latest(lf, units).collect()
    typer.echo(str(out))


@metadata_app.command("fetch")
def metadata_fetch(refresh: bool = typer.Option(False)) -> None:
    from pathlib import Path

    from bls_stats.core.http import build_client
    from bls_stats.enrich.cps import fetch_metadata

    settings, _ = _setup()
    meta = fetch_metadata(build_client(settings), Path("data/cps_metadata"), refresh=refresh)
    typer.echo(f"fetched {len(meta)} metadata tables")


@metadata_app.command("export")
def metadata_export() -> None:
    from pathlib import Path

    from bls_stats.core.http import build_client
    from bls_stats.enrich.cps import export_metadata, fetch_metadata

    settings, store = _setup()
    meta = fetch_metadata(build_client(settings), Path("data/cps_metadata"))
    export_metadata(store, meta)
    typer.echo("exported")


@metadata_app.command("enrich")
def metadata_enrich(ref_date_opt: str = typer.Option(..., "--ref-date")) -> None:
    """Full BEH §2.5 enrichment of one CPS slice — spot-check view."""
    from pathlib import Path

    import polars as pl

    from bls_stats.core.http import build_client
    from bls_stats.enrich.cps import enrich, fetch_metadata

    settings, store = _setup()
    lf = store.scan_observations("cps")
    if lf is None:
        typer.echo("cps: (empty)", err=True)
        raise typer.Exit(1)
    obs = lf.filter(pl.col("ref_date") == date.fromisoformat(ref_date_opt)).collect()
    meta = fetch_metadata(build_client(settings), Path("data/cps_metadata"))
    typer.echo(str(enrich(obs, meta)))


@app.command()
def doctor() -> None:
    """Pre-flight probes (ARCH §8): green/red checklist; non-zero exit on any failure."""
    from bls_stats.storage.doctor import run_all

    settings, _ = _setup()
    results = run_all(settings)
    for r in results:
        mark = "✓" if r.ok else "✗"
        typer.echo(f"{mark} {r.name}: {r.detail}")
    raise typer.Exit(0 if all(r.ok for r in results) else 1)
```

- [ ] **Step 4: Run to verify pass** — `uv run pytest tests/test_cli.py -v`

- [ ] **Step 5: Commit** — `git commit -am "feat: full typer CLI surface"`

---

### Task 22: Vintage replay integration suite + README + wrap-up

**Files:**
- Create: `tests/test_vintage_replay.py`, `README.md`
- Modify: `specs/plans/1-bls-stats-architecture.md` (mark complete when done)

**Interfaces:** consumes everything; produces the ARCH §9 vintage suite and §11 acceptance evidence.

- [ ] **Step 1: Write the replay test** — `tests/test_vintage_replay.py` (this is the ARCH §11.2 acceptance test):

```python
"""Replay a synthetic CES release sequence and assert the §2.1 lifecycle end-to-end."""

from datetime import date, datetime, timezone

import polars as pl

from bls_stats.core.config import Settings
from bls_stats.pipeline import run_ingest
from bls_stats.releases.feeds import Release
from bls_stats.storage.delta import VintageStore
from bls_stats.storage.reads import as_of, latest

NOW = datetime(2026, 7, 2, 13, 0, tzinfo=timezone.utc)

# CES publishing March..June 2026, then the Feb-2027 benchmark (January-2027 data):
SEQUENCE = [
    Release("ces", date(2026, 4, 3), 2026, 3, False),
    Release("ces", date(2026, 5, 8), 2026, 4, False),
    Release("ces", date(2026, 6, 5), 2026, 5, False),
    Release("ces", date(2026, 7, 2), 2026, 6, False),
    Release("ces", date(2027, 2, 5), 2027, 1, True),
]


def fetch_everything(client, program, slots, dest_dir, downloaded) -> pl.DataFrame:
    refs = [s.ref_date for s in slots]
    return pl.DataFrame({
        "series_id": ["CES0000000001"] * len(refs),
        "value": [float(r.toordinal()) for r in refs],  # value encodes the ref_date
        "footnote_codes": [""] * len(refs),
        "ref_date": refs,
    })


def replay(store: VintageStore) -> None:
    for release in SEQUENCE:
        run_ingest(
            Settings(), store, ["ces"], clock=lambda: NOW,
            poll_fn=lambda client, programs, r=release: [r],
            fetch_fn=fetch_everything, fresh_fn=lambda client, program, rd: True,
        )


def test_march_2026_lifecycle_matches_spec(tmp_path) -> None:  # ARCH §2.1 table
    store = VintageStore(str(tmp_path / "store"))
    replay(store)
    march = (
        store.scan_observations("ces")
        .filter(pl.col("ref_date") == date(2026, 3, 12))
        .sort("release_date").collect()
    )
    lifecycle = list(zip(march["revision"].to_list(), march["benchmark"].to_list()))
    assert lifecycle == [(0, 0), (1, 0), (2, 0), (2, 1)]  # the user's founding example


def test_benchmark_day_one_row_per_ref_date(tmp_path) -> None:  # ARCH §2.2 blocker fix
    store = VintageStore(str(tmp_path / "store"))
    replay(store)
    bench_day = (
        store.scan_observations("ces")
        .filter(pl.col("release_date") == date(2027, 2, 5)).collect()
    )
    assert bench_day["ref_date"].n_unique() == bench_day.height


def test_candidate_key_unique(tmp_path) -> None:  # ARCH §4.3
    store = VintageStore(str(tmp_path / "store"))
    replay(store)
    obs = store.scan_observations("ces").collect()
    key = ["series_id", "ref_date", "release_date"]
    assert obs.unique(subset=key).height == obs.height


def test_as_of_no_future_leakage_across_replay(tmp_path) -> None:  # ARCH §9 crown jewel
    store = VintageStore(str(tmp_path / "store"))
    replay(store)
    lf = store.scan_observations("ces")
    for when in [date(2026, 4, 30), date(2026, 7, 2), date(2027, 12, 31)]:
        out = as_of(lf, ["series_id"], when).collect()
        assert (out["release_date"] <= when).all()
    # and the latest view reflects the benchmark for March 2026:
    march_latest = latest(lf, ["series_id"]).filter(
        pl.col("ref_date") == date(2026, 3, 12)
    ).collect()
    assert march_latest["benchmark"][0] == 1
```

- [ ] **Step 2: Run** — `uv run pytest tests/test_vintage_replay.py -v` — Expected: 4 passed (these exercise code from Tasks 7–20 together; failures here are integration bugs — debug, don't weaken the assertions).

- [ ] **Step 3: Write README.md** — quickstart (no employer names): what it is (one paragraph), install (`uv sync`), configure (`.project.env` table from ARCH §10), bootstrap order (`bls-stats doctor` → `bls-stats calendar build` → `bls-stats backfill --program jolts --start 2015/01 --end 2026/05` → crontab lines `bls-stats ingest` daily + `bls-stats store maintain` weekly), the vintage model in three sentences (release_date + (revision, benchmark), as-of reads, missed prints are permanent), and a pointer to `specs/bls-stats-architecture.md`.

- [ ] **Step 4: Full verification**

Run: `uv run pytest -q && uv run ruff check . && uv run bls-stats --help`
Expected: entire default suite green, ruff clean, CLI help shows all commands.
Then run the network smoke once (laptop): `uv run pytest -m network -v` — live feeds/pages parse.

- [ ] **Step 5: Final commit + plan retirement**

```bash
git add -A && git commit -m "feat: vintage replay integration suite and README"
```

Mark this plan complete at the top and move it to `specs/plans/completed/`. Remaining post-plan items live in ARCH §12 (corporate doctor run, MinIO bucket, QCEW empirical verification, benchmark-window verification).
