"""The eight programs as data (ARCH §3): specs, source URLs, layouts, revision profiles."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Frequency(StrEnum):
    """Cadence of a program's reference periods, driving `core.periods` period math.

    `NONE` marks EP, which is not periodic (ARCH §4.3): it has no `ref_date` and is keyed by
    `release_date` alone.
    """

    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUAL = "annual"
    NONE = "none"


class RefDateRule(StrEnum):
    """How `core.periods.ref_date` maps a `(year, period)` pair to a canonical `ref_date`.

    `NONE` marks EP, whose `ref_date` is always `None` (ARCH §4.3).
    """

    DAY_12 = "day_12"
    LAST_BUSINESS_DAY = "last_business_day"
    QUARTER_END_12 = "quarter_end_12"
    MAY_12 = "may_12"
    NONE = "none"


@dataclass(frozen=True)
class RevisionProfile:
    """A program's routine/benchmark print lifecycle (ARCH §2.1, §5.3).

    Drives `releases.profiles.expand`, which maps a detected `Release` to its fetch-plan
    slots using these fields.

    Attributes:
        routine_slots: Number of prints a routine release structurally carries — e.g. CES
            carries prints for t, t-1, t-2 (`routine_slots=3`), yielding revisions 0/1/2.
            Ignored when `routine_rule` is `"year_to_date"`.
        routine_rule: `"fixed"` (routine slots = `routine_slots`, the common case) or
            `"year_to_date"` — QCEW's rule (ARCH §6.2): a routine release carries every quarter
            of the reference year up to and including the newly published one.
        benchmark_rule: Structural rule identifying a benchmark event from the release's own
            reference period, never from feed title text (ARCH §5.2): `"jan_data"` (reference
            month is January), `"q1_data"` (reference quarter is Q1), or `None` for programs
            with no benchmark event (`oews`, `ep`).
        benchmark_window_years: Span of the benchmark re-snapshot window (ARCH §2.2): from
            January (or Q1) of `year(newest ref_date) - benchmark_window_years` through the
            newest `ref_date`, inclusive. `None` when `benchmark_rule` is `None`. Defaults are
            empirical placeholders pending verification against real events (ARCH §12).
    """

    routine_slots: int
    routine_rule: str = "fixed"  # "fixed" | "year_to_date" (QCEW, ARCH §6.2)
    benchmark_rule: str | None = None  # "jan_data" | "q1_data" | None (ARCH §5.3)
    benchmark_window_years: int | None = None  # ARCH §2.2 defaults; verify per §12


@dataclass(frozen=True)
class ProgramSpec:
    """Everything the pipeline needs to know about one of the eight BLS programs.

    Programs are data, not subclasses (ARCH §3): an engine reads a `ProgramSpec` to know which
    URLs to fetch, how to derive `ref_date`, and which columns identify a unit within the
    program's observations table.

    Attributes:
        name: Registry key, e.g. `"ces"`; matches the dict key in `REGISTRY`.
        frequency: Reference-period cadence; selects the period-math branch in `core.periods`.
        ref_date_rule: Rule mapping `(year, period)` to the canonical `ref_date`.
        series_prefix: Two-letter series-ID prefix (key into `SERIES_LAYOUTS`), or `None` for
            programs with no LABSTAT-style series ID (`qcew`).
        unit_columns: Column(s) identifying a unit within this program's observations table
            (ARCH §4.3) — `("series_id",)` for the five LABSTAT programs, the QCEW establishment
            key, or the OEWS/EP area-occupation key.
        backfill_url: Stage-1 full-history source (ARCH §6.2), possibly a `str.format`
            template (QCEW/OEWS use `{year}`/`{yy}`; EP uses `{soc}`).
        increment_url: Stage-2 routine-fetch source; often the same file as `backfill_url`
            when the program has no `.Current` variant.
        benchmark_url: Source for a benchmark-window re-snapshot (ARCH §2.2), or `None` for
            programs with no benchmark event.
        feed_url: Atom feed URL polled for release detection (ARCH §5.1), or `None` for EP,
            which has no feed (ARCH §5.2 exception).
        archive_url: Archive page scraped for historical release dates (ARCH §5.4), or `None`.
        schedule_url: Schedule page scraped for upcoming release dates, or `None` when absent
            or unreliable (QCEW's 404s — tolerated per ARCH §5.4).
        release_time_et: Scheduled embargo time, `"08:30"` or `"10:00"` ET, used as the
            stale-file guard floor for `Last-Modified` (ARCH §6.3); `None` for EP.
        profile: This program's `RevisionProfile`.
        row_band: Sanity-check tolerance (ARCH §7.3): a fetched frame's row count must fall
            within this fraction of its comparator's row count.
        null_rate_max: Sanity-check ceiling (ARCH §7.3) on the `value` column's null rate.
    """

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
    "SM": (
        ("prefix", 2),
        ("seasonal", 1),
        ("state", 2),
        ("area", 5),
        ("supersector", 2),
        ("industry", 6),
        ("data_type", 2),
    ),
    "BD": (
        ("prefix", 2),
        ("seasonal", 1),
        ("area_code", 10),
        ("industry_code", 6),
        ("unit_analysis", 1),
        ("data_element", 1),
        ("size_class", 2),
        ("data_class", 2),
        ("rate_level", 1),
        ("record_type", 1),
        ("ownership", 1),
    ),
    "JT": (
        ("prefix", 2),
        ("seasonal", 1),
        ("industry_code", 6),
        ("state_code", 2),
        ("area_code", 5),
        ("size_class", 2),
        ("data_element", 2),
        ("rate_level", 1),
    ),
    "LN": (("prefix", 2), ("seasonal", 1), ("series_code", 8)),
    "OE": (
        ("prefix", 2),
        ("seasonal", 1),
        ("state_code", 2),
        ("area_code", 7),
        ("industry_code", 6),
        ("occupation_code", 6),
        ("datatype_code", 2),
    ),
    "EP": (("prefix", 2), ("seasonal", 1), ("occupation_code", 6), ("industry_code", 6)),
}

_TS = "https://download.bls.gov/pub/time.series"

REGISTRY: dict[str, ProgramSpec] = {
    "ces": ProgramSpec(
        name="ces",
        frequency=Frequency.MONTHLY,
        ref_date_rule=RefDateRule.DAY_12,
        series_prefix="CE",
        unit_columns=("series_id",),
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
        name="sae",
        frequency=Frequency.MONTHLY,
        ref_date_rule=RefDateRule.DAY_12,
        series_prefix="SM",
        unit_columns=("series_id",),
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
        name="jolts",
        frequency=Frequency.MONTHLY,
        ref_date_rule=RefDateRule.LAST_BUSINESS_DAY,
        series_prefix="JT",
        unit_columns=("series_id",),
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
        name="cps",
        frequency=Frequency.MONTHLY,
        ref_date_rule=RefDateRule.DAY_12,
        series_prefix="LN",
        unit_columns=("series_id",),
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
        name="bed",
        frequency=Frequency.QUARTERLY,
        ref_date_rule=RefDateRule.QUARTER_END_12,
        series_prefix="BD",
        unit_columns=("series_id",),
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
        name="qcew",
        frequency=Frequency.QUARTERLY,
        ref_date_rule=RefDateRule.QUARTER_END_12,
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
        name="oews",
        frequency=Frequency.ANNUAL,
        ref_date_rule=RefDateRule.MAY_12,
        series_prefix="OE",
        unit_columns=("area", "occ_code"),
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
        name="ep",
        frequency=Frequency.NONE,
        ref_date_rule=RefDateRule.NONE,
        series_prefix="EP",
        unit_columns=("occupation_code", "industry_code"),
        backfill_url="https://www.bls.gov/emp/tables/industry-occupation-matrix-occupation.htm",
        increment_url="https://data.bls.gov/projections/nationalMatrix?queryParams={soc}&ioType=o",
        benchmark_url=None,
        feed_url=None,
        archive_url=None,
        schedule_url=None,  # ARCH §5.2 exception
        release_time_et=None,
        profile=RevisionProfile(1, "fixed", None, None),
    ),
}
