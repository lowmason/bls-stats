"""Capture live BLS payloads as test fixtures (run manually)."""

from pathlib import Path

from bls_stats.core.config import load_settings
from bls_stats.core.http import Throttle, build_client, get
from bls_stats.registry import REGISTRY
from bls_stats.releases.calendar import LAPSE_URLS

FIXTURES = Path("tests/fixtures")


def capture_feeds() -> None:
    client = build_client(load_settings())
    throttle = Throttle(2.0)
    for url in sorted({s.feed_url for s in REGISTRY.values() if s.feed_url}):
        throttle.wait()
        name = url.rsplit("/", 1)[-1].replace(".rss", ".live.xml")
        (FIXTURES / "feeds" / name).write_bytes(get(client, url).content)
        print("captured", name)


def capture_calendar_pages() -> None:
    client = build_client(load_settings())
    throttle = Throttle(2.0)
    for program, spec in sorted(REGISTRY.items()):
        for kind, url in (("archive", spec.archive_url), ("schedule", spec.schedule_url)):
            if url is None:
                continue
            throttle.wait()
            name = f"{program}_{kind}.live.html"
            (FIXTURES / "html" / name).write_bytes(get(client, url).content)
            print("captured", name)
    for i, url in enumerate(LAPSE_URLS):
        throttle.wait()
        name = f"lapse_{i}.live.html"
        (FIXTURES / "html" / name).write_bytes(get(client, url).content)
        print("captured", name)


if __name__ == "__main__":
    FIXTURES.joinpath("feeds").mkdir(parents=True, exist_ok=True)
    FIXTURES.joinpath("html").mkdir(parents=True, exist_ok=True)
    capture_feeds()
    capture_calendar_pages()
