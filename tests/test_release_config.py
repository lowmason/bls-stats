"""Tests for release dates configuration."""

from bls_stats.release_dates.config import (
    BLS_FEED_BASE,
    LAPSE_URL,
    PUBLICATIONS,
    Publication,
    CES_PUB,
    SAE_PUB,
    QCEW_PUB,
    BED_PUB,
    JOLTS_PUB,
)


class TestPublications:
    def test_all_registered(self):
        expected = {"ces", "sae", "qcew", "bed", "jolts"}
        assert set(PUBLICATIONS.keys()) == expected

    def test_ces(self):
        assert CES_PUB.name == "ces"
        assert CES_PUB.frequency == "monthly"
        assert "empsit" in CES_PUB.archive_url

    def test_qcew(self):
        assert QCEW_PUB.name == "qcew"
        assert QCEW_PUB.frequency == "quarterly"
        assert QCEW_PUB.schedule_url != ""

    def test_bed(self):
        assert BED_PUB.name == "bed"
        assert BED_PUB.frequency == "quarterly"
        assert BED_PUB.schedule_url != ""

    def test_jolts(self):
        assert JOLTS_PUB.name == "jolts"
        assert JOLTS_PUB.frequency == "monthly"
        assert JOLTS_PUB.schedule_url != ""

    def test_sae(self):
        assert SAE_PUB.name == "sae"
        assert SAE_PUB.frequency == "monthly"

    def test_all_have_schedule_urls(self):
        for pub in PUBLICATIONS.values():
            assert pub.schedule_url != "", f"{pub.name} missing schedule_url"


class TestPublication:
    def test_frozen(self):
        import pytest

        with pytest.raises(AttributeError):
            CES_PUB.name = "changed"

    def test_archive_urls_are_absolute(self):
        for pub in PUBLICATIONS.values():
            assert pub.archive_url.startswith("https://")

    def test_feed_url_property(self):
        assert CES_PUB.feed_url == f"{BLS_FEED_BASE}/empsit.rss"
        assert JOLTS_PUB.feed_url == f"{BLS_FEED_BASE}/jolts.rss"
        assert QCEW_PUB.feed_url == f"{BLS_FEED_BASE}/cewqtr.rss"

    def test_feed_urls_are_absolute(self):
        for pub in PUBLICATIONS.values():
            assert pub.feed_url.startswith("https://")
            assert pub.feed_url.endswith(".rss")


class TestConstants:
    def test_lapse_url(self):
        assert LAPSE_URL.startswith("https://")
        assert "lapse" in LAPSE_URL
