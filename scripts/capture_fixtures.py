"""Capture live BLS payloads as test fixtures (run manually)."""

from pathlib import Path

from bls_stats.core.config import load_settings
from bls_stats.core.http import Throttle, build_client, get
from bls_stats.registry import REGISTRY
from bls_stats.releases.calendar import LAPSE_URLS

FIXTURES = Path("tests/fixtures")

_LABSTAT_PROGRAMS = ("ces", "sae", "jolts", "cps", "bed")


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


def capture_labstat() -> None:
    """Range-request the first ~200 lines of each LABSTAT program's increment file."""
    client = build_client(load_settings())
    throttle = Throttle(2.0)
    for program in _LABSTAT_PROGRAMS:
        spec = REGISTRY[program]
        url = spec.increment_url
        if url is None:
            continue
        throttle.wait()
        resp = client.get(url, headers={"Range": "bytes=0-20000"})
        resp.raise_for_status()
        text = resp.content.decode("utf-8", errors="replace")
        lines = text.splitlines(keepends=True)
        if lines and not lines[-1].endswith("\n"):
            lines = lines[:-1]  # trim the partial trailing line from the byte-range cut
        prefix = spec.series_prefix.lower()
        name = f"{prefix}.data.live.txt"
        (FIXTURES / "labstat" / name).write_text("".join(lines))
        print("captured", name)


if __name__ == "__main__":
    FIXTURES.joinpath("feeds").mkdir(parents=True, exist_ok=True)
    FIXTURES.joinpath("html").mkdir(parents=True, exist_ok=True)
    FIXTURES.joinpath("labstat").mkdir(parents=True, exist_ok=True)
    capture_feeds()
    capture_calendar_pages()
    capture_labstat()
