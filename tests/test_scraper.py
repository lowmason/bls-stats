"""Tests for release date scraper."""

from datetime import date
from unittest.mock import patch, MagicMock

from bs4 import BeautifulSoup

from bls_stats.release_dates.scraper import (
    ReleaseDate,
    _extract_release_links,
    _scrape_release_date,
    _DATE_RE,
)


class TestDateRegex:
    def test_matches_standard_date(self):
        assert _DATE_RE.search("Released March 11, 2025") is not None

    def test_matches_single_digit_day(self):
        assert _DATE_RE.search("February 7, 2024") is not None

    def test_no_match_on_numeric(self):
        assert _DATE_RE.search("03/11/2025") is None


class TestExtractReleaseLinks:
    def test_extracts_html_links(self):
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
        assert links[1][0] == "March 2025"

    def test_skips_empty_links(self):
        html = '<ul><li><a href="">  </a></li></ul>'
        soup = BeautifulSoup(html, "html.parser")
        links = _extract_release_links(soup, "https://www.bls.gov/")
        assert len(links) == 0


class TestScrapeReleaseDate:
    def test_extracts_date_from_header(self):
        mock_resp = MagicMock()
        mock_resp.text = '<html><body><p class="sub">March 11, 2025</p></body></html>'
        mock_client = MagicMock()
        mock_client.get.return_value = mock_resp

        result = _scrape_release_date(mock_client, "https://example.com/release.htm")
        assert result == date(2025, 3, 11)

    def test_returns_none_for_no_date(self):
        mock_resp = MagicMock()
        mock_resp.text = "<html><body><p>No date here</p></body></html>"
        mock_client = MagicMock()
        mock_client.get.return_value = mock_resp

        result = _scrape_release_date(mock_client, "https://example.com/release.htm")
        assert result is None

    def test_returns_none_on_http_error(self):
        mock_client = MagicMock()
        mock_client.get.side_effect = Exception("connection failed")

        result = _scrape_release_date(mock_client, "https://example.com/release.htm")
        assert result is None


class TestReleaseDate:
    def test_dataclass_fields(self):
        rd = ReleaseDate(
            publication="ces",
            title="January 2025",
            release_date=date(2025, 1, 10),
            url="https://www.bls.gov/news/empsit_01.htm",
        )
        assert rd.publication == "ces"
        assert rd.release_date == date(2025, 1, 10)
