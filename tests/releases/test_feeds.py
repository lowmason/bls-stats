from datetime import date
from pathlib import Path

import pytest

from bls_stats.releases.feeds import Release, parse_feed, poll

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


_ENTRY = (
    '<entry xmlns="http://www.w3.org/2005/Atom">'
    "<title>Employment Situation for June</title>"
    '<link href="https://www.bls.gov/news.release/archives/empsit_{date}.htm"/>'
    "</entry>"
)


def _feed(*entries: str) -> bytes:
    body = "".join(entries)
    return f'<feed xmlns="http://www.w3.org/2005/Atom">{body}</feed>'.encode()


def test_parse_feed_skips_impossible_embedded_date() -> None:  # C-2
    good = _ENTRY.format(date="07032026")
    bad = _ENTRY.format(date="02292023")  # Feb 29 in a non-leap year
    out = parse_feed(_feed(bad, good), "ces")
    assert [r.release_date.isoformat() for r in out] == ["2026-07-03"]  # bad skipped, good kept


def test_poll_tolerates_non_xml_body() -> None:  # C-2
    import httpx

    def handler(request: httpx.Request) -> httpx.Response:
        if "empsit" in str(request.url):  # ces/cps feed serves an HTML maintenance page
            # Deliberately not well-formed XML (undefined entity) so it triggers
            # ElementTree.ParseError — a real maintenance page with `<html><body>down
            # </body></html>` alone is valid XML and would not exercise this path.
            return httpx.Response(200, text="<html><body>down &nbsp; for maintenance</body></html>")
        return httpx.Response(200, content=_feed(_ENTRY.format(date="07022026")))

    client = httpx.Client(transport=httpx.MockTransport(handler))
    out = poll(client, ["ces", "jolts"])  # empsit broken; jolts healthy
    assert {r.program for r in out} == {"jolts"}  # jolts survived, no exception raised
