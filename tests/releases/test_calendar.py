from datetime import date
from pathlib import Path

import polars as pl
import pytest

from bls_stats.releases.calendar import (
    apply_lapse_overlay,
    filter_published,
    find_gaps,
    scrape_archive,
    scrape_schedule,
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
    assert cal.filter(pl.col("ref_date") == date(2026, 7, 12))["release_date"][0] == date(
        2026, 8, 7
    )


def test_lapse_overlay_revises_and_cancels() -> None:
    base = scrape_archive((HTML / "empsit_archive.html").read_bytes(), "ces")
    extra = pl.DataFrame(
        {
            "program": ["ces", "ces"],
            "ref_date": [date(2025, 9, 12), date(2025, 10, 12)],
            "release_date": [date(2025, 10, 3), date(2025, 11, 7)],
            "original_release": pl.Series([None, None], dtype=pl.Date),
            "is_benchmark": [False, False],
        }
    )
    cal = apply_lapse_overlay(pl.concat([base, extra]), (HTML / "lapse.html").read_bytes())
    sept = cal.filter(pl.col("ref_date") == date(2025, 9, 12))
    assert sept["release_date"][0] == date(2025, 11, 20)
    assert sept["original_release"][0] == date(2025, 10, 3)
    octr = cal.filter(pl.col("ref_date") == date(2025, 10, 12))
    assert octr["release_date"][0] is None  # cancelled


def _mini_cal() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "program": ["ces"] * 3,
            "ref_date": [date(2026, 3, 12), date(2026, 4, 12), date(2026, 6, 12)],
            "release_date": [date(2026, 4, 3), date(2026, 5, 8), date(2026, 7, 2)],
            "original_release": pl.Series([None] * 3, dtype=pl.Date),
            "is_benchmark": [False] * 3,
        }
    )


def test_find_gaps_reports_missing_month() -> None:
    gaps = find_gaps(_mini_cal())
    assert gaps.to_dicts() == [{"program": "ces", "ref_date": date(2026, 5, 12)}]


def test_filter_published_pinned_semantics() -> None:  # ARCH §5.4
    periods = [(1948, 1), (2026, 5), (2026, 6), (2026, 7)]
    kept = filter_published("ces", periods, _mini_cal())
    assert (1948, 1) in kept  # pre-calendar coverage passes through
    assert (2026, 6) in kept
    assert (2026, 7) not in kept  # beyond latest published
    assert (2026, 5) in kept  # gap within coverage is NOT dropped (it may exist in bulk)


def test_filter_published_drops_cancelled() -> None:
    cal = _mini_cal().with_columns(
        pl.when(pl.col("ref_date") == date(2026, 4, 12))
        .then(pl.lit(None, dtype=pl.Date))
        .otherwise(pl.col("release_date"))
        .alias("release_date")
    )
    assert (2026, 4) not in filter_published("ces", [(2026, 4)], cal)


@pytest.mark.network
def test_live_calendar_build() -> None:
    from bls_stats.core.config import load_settings
    from bls_stats.core.http import build_client
    from bls_stats.releases.calendar import build

    cal = build(build_client(load_settings()), ["ces", "jolts"])
    assert cal.height >= 20
    assert cal.filter(pl.col("is_benchmark")).height >= 1
