"""Tests for the Atom feed poller."""

from datetime import date
from unittest.mock import MagicMock, patch

import polars as pl

from bls_stats.release_dates.feed import poll_feed, poll_all
from bls_stats.release_dates.scraper import SCHEMA

SAMPLE_ATOM = """\
<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>BLS: The Employment Situation</title>
  <entry>
    <title>Payroll employment increases by 178,000 in March 2026</title>
    <link type="text/html" rel="alternate"
          href="https://www.bls.gov/news.release/archives/empsit_04032026.htm"/>
    <published>2026-04-03T08:30:00Z</published>
  </entry>
  <entry>
    <title>Payroll employment edges down 92,000 in February 2026</title>
    <link type="text/html" rel="alternate"
          href="https://www.bls.gov/news.release/archives/empsit_03062026.htm"/>
    <published>2026-03-06T08:30:00Z</published>
  </entry>
</feed>
"""

EMPTY_ATOM = """\
<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Empty</title>
</feed>
"""


def _mock_client(text: str):
    mock_resp = MagicMock()
    mock_resp.text = text

    client = MagicMock()
    client.get.return_value = mock_resp
    client.__enter__ = lambda s: s
    client.__exit__ = MagicMock(return_value=False)
    return client


class TestPollFeed:
    def test_parses_entries(self):
        with patch("bls_stats.release_dates.feed.httpx.Client", return_value=_mock_client(SAMPLE_ATOM)):
            df = poll_feed("ces")

        assert isinstance(df, pl.DataFrame)
        assert len(df) == 2
        assert df["program"][0] == "ces"
        assert df["release_date"][0] == date(2026, 4, 3)
        assert df["ref_date"][0] == date(2026, 3, 12)

    def test_empty_feed(self):
        with patch("bls_stats.release_dates.feed.httpx.Client", return_value=_mock_client(EMPTY_ATOM)):
            df = poll_feed("ces")

        assert isinstance(df, pl.DataFrame)
        assert len(df) == 0
        assert set(df.columns) == set(SCHEMA.keys())

    def test_accepts_publication_string(self):
        with patch("bls_stats.release_dates.feed.httpx.Client", return_value=_mock_client(SAMPLE_ATOM)):
            df = poll_feed("jolts")

        assert len(df) == 2
        assert df["program"][0] == "jolts"


class TestPollAll:
    def test_combines_all_programs(self):
        with patch("bls_stats.release_dates.feed.httpx.Client", return_value=_mock_client(SAMPLE_ATOM)):
            df = poll_all()

        assert isinstance(df, pl.DataFrame)
        assert len(df) == 10  # 2 entries x 5 programs
        assert set(df["program"].unique().to_list()) == {"ces", "sae", "qcew", "bed", "jolts"}

    def test_handles_failures_gracefully(self):
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 1:
                raise ConnectionError("timeout")
            return _mock_client(SAMPLE_ATOM)

        with patch("bls_stats.release_dates.feed.httpx.Client", side_effect=side_effect):
            df = poll_all()

        assert isinstance(df, pl.DataFrame)
        assert len(df) > 0
