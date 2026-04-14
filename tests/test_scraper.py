"""Tests for the release-dates scraper."""

from datetime import date
from unittest.mock import MagicMock, patch

import polars as pl
from bs4 import BeautifulSoup

from bls_stats.release_dates.scraper import (
    _extract_embargo_date,
    _extract_ref_period,
    _extract_release_links,
    _last_business_day,
    _parse_ref_date,
    scrape_archive,
    scrape_schedule,
    SCHEMA,
)


class TestLastBusinessDay:
    def test_friday(self):
        # Jan 2025 ends on a Friday (31st)
        assert _last_business_day(2025, 1) == date(2025, 1, 31)

    def test_saturday_rollback(self):
        # May 2025 ends on Saturday (31st) -> Friday 30th
        assert _last_business_day(2025, 5) == date(2025, 5, 30)

    def test_sunday_rollback(self):
        # Aug 2025 ends on Sunday (31st) -> Friday 29th
        assert _last_business_day(2025, 8) == date(2025, 8, 29)

    def test_weekday_end(self):
        # Apr 2025 ends on Wednesday (30th)
        assert _last_business_day(2025, 4) == date(2025, 4, 30)


class TestParseRefDate:
    def test_monthly_ces(self):
        assert _parse_ref_date("ces", "THE EMPLOYMENT SITUATION - MARCH 2026") == date(
            2026, 3, 12
        )

    def test_monthly_sae(self):
        assert _parse_ref_date("sae", "STATE EMPLOYMENT AND UNEMPLOYMENT - JANUARY 2025") == date(
            2025, 1, 12
        )

    def test_monthly_jolts(self):
        result = _parse_ref_date("jolts", "JOB OPENINGS AND LABOR TURNOVER - MARCH 2025")
        assert result == _last_business_day(2025, 3)

    def test_quarterly_first(self):
        assert _parse_ref_date("qcew", "QUARTERLY CENSUS - First Quarter 2025") == date(
            2025, 3, 12
        )

    def test_quarterly_fourth(self):
        assert _parse_ref_date("bed", "BUSINESS EMPLOYMENT DYNAMICS - Fourth Quarter 2024") == (
            date(2024, 12, 12)
        )

    def test_no_match(self):
        assert _parse_ref_date("ces", "No period here") is None


class TestExtractEmbargoDate:
    def test_standard_embargo(self):
        html = (
            '<html><body><p>Transmission of material in this news release '
            'is embargoed until 8:30 a.m. (ET) Friday, April 3, 2026</p></body></html>'
        )
        soup = BeautifulSoup(html, "html.parser")
        assert _extract_embargo_date(soup) == date(2026, 4, 3)

    def test_no_embargo(self):
        html = "<html><body><p>No embargo text here</p></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        assert _extract_embargo_date(soup) is None

    def test_different_day_of_week(self):
        html = (
            '<html><body><p>embargoed until 10:00 a.m. (ET) Tuesday, '
            'February 11, 2025</p></body></html>'
        )
        soup = BeautifulSoup(html, "html.parser")
        assert _extract_embargo_date(soup) == date(2025, 2, 11)


class TestExtractRefPeriod:
    def test_monthly_title(self):
        html = "<html><body><h2>THE EMPLOYMENT SITUATION - MARCH 2026</h2></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        result = _extract_ref_period(soup)
        assert result is not None
        assert "MARCH 2026" in result

    def test_quarterly_title(self):
        html = (
            "<html><body><h2>QUARTERLY CENSUS OF EMPLOYMENT AND WAGES - "
            "First Quarter 2025</h2></body></html>"
        )
        soup = BeautifulSoup(html, "html.parser")
        result = _extract_ref_period(soup)
        assert result is not None
        assert "First Quarter 2025" in result

    def test_no_period(self):
        html = "<html><body><h2>About BLS</h2></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        assert _extract_ref_period(soup) is None


class TestExtractReleaseLinks:
    def test_extracts_htm_links(self):
        html = """
        <ul>
            <li><a href="/news/empsit_01.htm">January 2025 (HTML)</a></li>
            <li><a href="/news/empsit_02.pdf">February 2025 (PDF)</a></li>
            <li><a href="/news/empsit_03.html">March 2025</a></li>
        </ul>
        """
        soup = BeautifulSoup(html, "html.parser")
        links = _extract_release_links(soup, "https://www.bls.gov/archive/")
        assert len(links) == 2
        assert links[0][0] == "January 2025"
        assert links[0][1].startswith("https://")

    def test_skips_empty_links(self):
        html = '<ul><li><a href="">  </a></li></ul>'
        soup = BeautifulSoup(html, "html.parser")
        links = _extract_release_links(soup, "https://www.bls.gov/")
        assert len(links) == 0


class TestScrapeArchive:
    def test_returns_dataframe(self):
        archive_html = """
        <html><body><ul>
            <li><a href="/release1.htm">March 2026 (HTML)</a></li>
        </ul></body></html>
        """
        release_html = """
        <html><body>
            <p>Transmission of material in this news release is embargoed until
            8:30 a.m. (ET) Friday, April 3, 2026</p>
            <h2>THE EMPLOYMENT SITUATION - MARCH 2026</h2>
        </body></html>
        """

        mock_archive_resp = MagicMock()
        mock_archive_resp.text = archive_html

        mock_release_resp = MagicMock()
        mock_release_resp.text = release_html

        def mock_get(url):
            if "release1" in url:
                return mock_release_resp
            return mock_archive_resp

        mock_client = MagicMock()
        mock_client.get = mock_get
        mock_client.__enter__ = lambda s: s
        mock_client.__exit__ = MagicMock(return_value=False)

        with patch("bls_stats.release_dates.scraper._make_client", return_value=mock_client):
            df = scrape_archive("ces")

        assert isinstance(df, pl.DataFrame)
        assert set(df.columns) >= {"program", "release_date", "ref_date"}
        assert len(df) == 1
        assert df["program"][0] == "ces"
        assert df["release_date"][0] == date(2026, 4, 3)
        assert df["ref_date"][0] == date(2026, 3, 12)

    def test_empty_archive(self):
        mock_resp = MagicMock()
        mock_resp.text = "<html><body><p>No links</p></body></html>"

        mock_client = MagicMock()
        mock_client.get.return_value = mock_resp
        mock_client.__enter__ = lambda s: s
        mock_client.__exit__ = MagicMock(return_value=False)

        with patch("bls_stats.release_dates.scraper._make_client", return_value=mock_client):
            df = scrape_archive("ces")

        assert isinstance(df, pl.DataFrame)
        assert len(df) == 0
        assert set(df.columns) == set(SCHEMA.keys())


class TestScrapeSchedule:
    def test_returns_dataframe(self):
        html = """
        <html><body><ul>
            <li>The Employment Situation for March 2026, scheduled for
                April 3, 2026</li>
            <li>The Employment Situation for April 2026, scheduled for
                May 2, 2026</li>
        </ul></body></html>
        """
        mock_resp = MagicMock()
        mock_resp.text = html

        mock_client = MagicMock()
        mock_client.get.return_value = mock_resp
        mock_client.__enter__ = lambda s: s
        mock_client.__exit__ = MagicMock(return_value=False)

        with patch("bls_stats.release_dates.scraper._make_client", return_value=mock_client):
            df = scrape_schedule("ces")

        assert isinstance(df, pl.DataFrame)
        assert len(df) == 2
        assert df["program"][0] == "ces"
