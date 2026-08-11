# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx[http2]>=0.27"]
# ///
"""§20 issue 4 (sizes half): actual QCEW quarterly-singlefile sizes, measured.

HEADs the singlefile zip per year, downloads ONE (the newest available, streaming,
1 MiB chunks), records compressed/decompressed sizes, line counts, and this
process's peak RSS. The download is a measurement, not a capture — no archive
bucket exists yet, by design (§7.3) — and the scratch file is deleted. Diff COST
is measured at Stage 5 (roadmap, issue 4).
"""
import argparse
import datetime as dt
import resource
import sys
import tempfile
import zipfile
from pathlib import Path

from _lib import make_client, probe, write_results

URL = "https://data.bls.gov/cew/data/files/{year}/csv/{year}_qtrly_singlefile.zip"
YEARS = range(2019, 2027)


def _probed_at() -> str:
    """Matches the timestamp format _lib.probe() already uses."""
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds")


def peak_rss_bytes() -> int:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss if sys.platform == "darwin" else rss * 1024  # macOS: bytes; Linux: KiB


def measure_zip(path: Path) -> list[dict]:
    rows = []
    with zipfile.ZipFile(path) as zf:
        for info in zf.infolist():
            n_lines = 0
            with zf.open(info) as member:
                while chunk := member.read(1 << 20):
                    n_lines += chunk.count(b"\n")
            rows.append({
                "member": info.filename,
                "compressed_bytes": info.compress_size,
                "decompressed_bytes": info.file_size,
                "lines": n_lines,
            })
    return rows


def measure_then_delete(dest: Path) -> list[dict]:
    """measure_zip(dest), then remove the scratch file — unconditionally, even if
    measurement raises, so a failure never strands the ~287 MB download."""
    try:
        return measure_zip(dest)
    finally:
        dest.unlink()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dest", type=Path, default=Path(tempfile.gettempdir()),
                    help="scratch dir for the one measured download (deleted after)")
    args = ap.parse_args()

    records: list[dict] = []
    newest = None
    with make_client(timeout=120.0) as client:
        for year in YEARS:
            rec = probe(client, "HEAD", URL.format(year=year)) | {"year": year}
            clen = rec.get("headers", {}).get("content-length")
            print(f"{rec.get('status')}  {year}  Content-Length={clen}")
            records.append(rec)
            if rec.get("status") == 200:
                newest = year

        if newest is None:
            write_results("qcew_sizes", records)
            raise SystemExit(
                "No singlefile zip found for any probed year — URL pattern wrong?\n"
                "Check https://www.bls.gov/cew/downloadable-data-files.htm, fix URL, re-run."
            )

        url = URL.format(year=newest)
        dest = args.dest / f"qcew_{newest}_qtrly_singlefile.zip"
        print(f"downloading {url} -> {dest} (streaming, 1 MiB chunks)")
        with client.stream("GET", url) as r:
            r.raise_for_status()  # fail loudly before any bytes are written to disk
            download_rec = {
                "probed_at": _probed_at(),
                "downloaded": url,
                "status": r.status_code,
                "http_version": r.http_version,
            }
            if (clen := r.headers.get("content-length")) is not None:
                download_rec["content_length"] = clen
            n = 0
            with dest.open("wb") as f:
                for chunk in r.iter_bytes(1 << 20):
                    f.write(chunk)
                    n += len(chunk)
        download_rec["bytes_on_disk"] = n
        records.append(download_rec)

    members = measure_then_delete(dest)
    records.extend({"probed_at": _probed_at(), "year": newest} | m for m in members)
    records.append({"probed_at": _probed_at(), "peak_rss_bytes": peak_rss_bytes()})

    out = write_results("qcew_sizes", records)
    print(f"\nresults: {out}")
    for m in members:
        print(f"{m['member']}: {m['compressed_bytes']:,} -> "
              f"{m['decompressed_bytes']:,} bytes, {m['lines']:,} lines")
    print(f"peak RSS of this measurement: {peak_rss_bytes():,} bytes")


if __name__ == "__main__":
    main()
