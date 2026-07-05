"""Fixed-width series-ID decoding (BEH §2.1 layouts, exposed as a registry)."""

from __future__ import annotations

from bls_stats.registry import SERIES_LAYOUTS


class SeriesIdError(ValueError):
    pass


def decode(series_id: str) -> dict[str, str]:
    sid = series_id.strip()
    layout = SERIES_LAYOUTS.get(sid[:2])
    if layout is None:
        raise SeriesIdError(f"unknown series prefix: {sid[:2]!r}")
    total = sum(w for _, w in layout)
    if len(sid) != total:
        raise SeriesIdError(f"{sid!r}: expected length {total}, got {len(sid)}")
    out: dict[str, str] = {}
    pos = 0
    for field, width in layout:
        out[field] = sid[pos : pos + width]
        pos += width
    return out
