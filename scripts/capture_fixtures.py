"""Capture live BLS payloads as test fixtures (run manually)."""

from pathlib import Path

from bls_stats.core.config import load_settings
from bls_stats.core.http import Throttle, build_client, get
from bls_stats.registry import REGISTRY

FIXTURES = Path("tests/fixtures")


def capture_feeds() -> None:
    client = build_client(load_settings())
    throttle = Throttle(2.0)
    for url in sorted({s.feed_url for s in REGISTRY.values() if s.feed_url}):
        throttle.wait()
        name = url.rsplit("/", 1)[-1].replace(".rss", ".live.xml")
        (FIXTURES / "feeds" / name).write_bytes(get(client, url).content)
        print("captured", name)


if __name__ == "__main__":
    FIXTURES.joinpath("feeds").mkdir(parents=True, exist_ok=True)
    capture_feeds()
