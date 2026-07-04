from datetime import date
from pathlib import Path

import pytest

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
    assert releases[(2026, 1)].is_benchmark is True  # January data ⇒ CES benchmark
    assert releases[(2026, 6)].is_benchmark is False


def test_cps_shares_empsit_with_own_events() -> None:
    ces = parse_feed(_empsit(), "ces")
    cps = parse_feed(_empsit(), "cps")
    assert len(ces) == len(cps) == 3
    assert all(r.program == "cps" for r in cps)


def test_quarterly_parse_uses_link_date_despite_edited_entry() -> None:
    releases = parse_feed((FIXTURES / "cewbd.xml").read_bytes(), "bed")
    assert releases == [Release("bed", date(2026, 4, 29), 2025, 3, False)]


def test_unparseable_entry_skipped_not_fatal() -> None:
    xml = b"""<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom">
      <entry><title>Nothing useful here</title>
      <link href="https://www.bls.gov/news.release/archives/empsit_01022026.htm"/>
      <published>2026-01-02T08:30:00-05:00</published></entry></feed>"""
    assert parse_feed(xml, "ces") == []


@pytest.mark.network
def test_live_feeds_parse() -> None:
    from bls_stats.core.config import load_settings
    from bls_stats.core.http import build_client
    from bls_stats.releases.feeds import poll

    releases = poll(
        build_client(load_settings()), ["ces", "cps", "jolts", "sae", "bed", "qcew", "oews"]
    )
    assert len(releases) >= 20  # 6 feeds × ~12 entries, minus unparseable
