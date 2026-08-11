# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx[http2]>=0.27"]
# ///
"""§20 issue 2: does the compliant flatfile profile pass on the flat-file hosts?

HEAD + tiny GET + ranged GET only. The one bulk download lives in qcew_sizes.py.
"""
import datetime as dt
import time

import httpx

from _lib import POLITE_DELAY_S, make_client, probe, write_results

REQUESTS = [
    ("GET", "https://download.bls.gov/robots.txt", "stated robot policy, dated"),
    ("HEAD", "https://download.bls.gov/pub/time.series/ce/ce.data.0.AllCESSeries",
     "large CES file: Last-Modified/ETag/Content-Length surface"),
    ("GET", "https://download.bls.gov/pub/time.series/ce/ce.period",
     "tiny mapping file: GET delivers bytes"),
    ("HEAD", "https://download.bls.gov/pub/time.series/jt/jt.data.1.AllItems",
     "second LABSTAT prefix (JOLTS)"),
    ("HEAD", "https://data.bls.gov/cew/data/files/2024/csv/2024_qtrly_singlefile.zip",
     "QCEW host data.bls.gov + singlefile URL-pattern confirmation"),
]

RANGED_URL = "https://download.bls.gov/pub/time.series/ce/ce.data.0.AllCESSeries"


def ranged_probe(client: httpx.Client, url: str, nbytes: int = 1024) -> dict:
    """Ranged GET that never drains the body if the server ignores Range."""
    rec: dict = {
        "probed_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
        "method": "GET",
        "url": url,
        "range": f"bytes=0-{nbytes - 1}",
    }
    try:
        with client.stream("GET", url, headers={"Range": f"bytes=0-{nbytes - 1}"}) as r:
            first = next(r.iter_bytes(nbytes), b"")
            rec |= {
                "status": r.status_code,
                "http_version": r.http_version,
                "headers": dict(r.headers),
                "first_chunk_bytes": len(first),
            }
    except httpx.HTTPError as e:
        rec |= {"status": None, "error": f"{type(e).__name__}: {e}"}
    time.sleep(POLITE_DELAY_S)
    return rec


def ingest_verdict(records: list[dict]) -> tuple[bool, object]:
    """Gate on the four data-bearing REQUESTS entries plus the ranged GET.

    robots.txt (REQUESTS[0]) is ancillary — this host serving no robots.txt at that
    path is not an ingest-channel failure (see findings §1) — so it is excluded from
    the gate and reported separately instead. Indices are read off REQUESTS rather
    than assumed, so this stays correct if REQUESTS' order or length ever changes.
    """
    statuses = [r.get("status") for r in records]
    robots_idx = next(i for i, (_, url, _) in enumerate(REQUESTS) if url.endswith("/robots.txt"))
    data_idxs = [i for i in range(len(REQUESTS)) if i != robots_idx]
    ranged_idx = len(REQUESTS)  # ranged_probe's record is appended right after the REQUESTS loop
    ok = all(statuses[i] == 200 for i in data_idxs) and statuses[ranged_idx] in (200, 206)
    return ok, statuses[robots_idx]


def main() -> None:
    records = []
    with make_client() as client:
        for method, url, why in REQUESTS:
            rec = probe(client, method, url) | {"why": why}
            print(f"{rec.get('status')}  {rec.get('http_version', '-'):8} {method:4} {url}")
            records.append(rec)
        rec = ranged_probe(client, RANGED_URL) | {"why": "is resumable streaming supported?"}
        print(f"{rec.get('status')}  ranged GET first_chunk={rec.get('first_chunk_bytes')}")
        records.append(rec)

    out = write_results("transport_flatfile", records)
    statuses = [r.get("status") for r in records]
    ok, robots_status = ingest_verdict(records)
    print(f"\nresults: {out}")
    print(f"robots.txt: status={robots_status} (informational — not part of the ingest-channel gate)")
    print(f"INGEST CHANNEL {'CONFIRMED' if ok else 'NOT CONFIRMED'}: statuses={statuses}")


if __name__ == "__main__":
    main()
