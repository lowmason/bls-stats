"""Publication definitions for BLS release-date scraping."""

from __future__ import annotations

from dataclasses import dataclass

BLS_ARCHIVE_BASE = "https://www.bls.gov/bls/news-release"
BLS_SCHEDULE_BASE = "https://www.bls.gov/schedule/news_release"
BLS_FEED_BASE = "https://www.bls.gov/feed"
LAPSE_URL = "https://www.bls.gov/bls/2025-lapse-revised-release-dates.htm"


@dataclass(frozen=True)
class Publication:
    name: str
    series: str
    frequency: str  # "monthly" or "quarterly"
    archive_url: str
    schedule_url: str = ""

    @property
    def feed_url(self) -> str:
        return f"{BLS_FEED_BASE}/{self.series}.rss"


PUBLICATIONS: dict[str, Publication] = {}


def _register(pub: Publication) -> Publication:
    PUBLICATIONS[pub.name] = pub
    return pub


CES_PUB = _register(
    Publication(
        name="ces",
        series="empsit",
        frequency="monthly",
        archive_url=f"{BLS_ARCHIVE_BASE}/empsit.htm",
        schedule_url=f"{BLS_SCHEDULE_BASE}/empsit.htm",
    )
)

SAE_PUB = _register(
    Publication(
        name="sae",
        series="laus",
        frequency="monthly",
        archive_url=f"{BLS_ARCHIVE_BASE}/laus.htm",
        schedule_url=f"{BLS_SCHEDULE_BASE}/laus.htm",
    )
)

QCEW_PUB = _register(
    Publication(
        name="qcew",
        series="cewqtr",
        frequency="quarterly",
        archive_url=f"{BLS_ARCHIVE_BASE}/cewqtr.htm",
        schedule_url=f"{BLS_SCHEDULE_BASE}/cewbd.htm",
    )
)

BED_PUB = _register(
    Publication(
        name="bed",
        series="cewbd",
        frequency="quarterly",
        archive_url=f"{BLS_ARCHIVE_BASE}/cewbd.htm",
        schedule_url=f"{BLS_SCHEDULE_BASE}/cewbd.htm",
    )
)

JOLTS_PUB = _register(
    Publication(
        name="jolts",
        series="jolts",
        frequency="monthly",
        archive_url=f"{BLS_ARCHIVE_BASE}/jolts.htm",
        schedule_url=f"{BLS_SCHEDULE_BASE}/jolts.htm",
    )
)
