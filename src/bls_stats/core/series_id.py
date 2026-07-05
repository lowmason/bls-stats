"""Fixed-width series-ID decoding (BEH §2.1 layouts, exposed as a registry)."""

from __future__ import annotations

from bls_stats.registry import SERIES_LAYOUTS


class SeriesIdError(ValueError):
    """An unrecognized series-ID prefix or a length mismatch against its layout."""


def decode(series_id: str) -> dict[str, str]:
    """Split a fixed-width BLS series ID into its named fields (BEH §2.1).

    The two-letter prefix (e.g. `"CE"`, `"LN"`) selects a positional layout from
    `SERIES_LAYOUTS`; each field occupies a fixed column range, decoded as `Utf8` slices — no
    numeric coercion, so leading zeros survive (dtype contract, ARCH §4.3).

    Args:
        series_id: The full series ID, e.g. `"CES0500000003"`. Leading/trailing whitespace is
            stripped before decoding.

    Returns:
        A dict mapping each layout field name (e.g. `"prefix"`, `"seasonal"`, `"industry"`) to
        its raw string slice, in layout order.

    Raises:
        SeriesIdError: The first two characters don't match a known prefix in
            `SERIES_LAYOUTS`, or the ID's length doesn't equal the layout's total width.
    """
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
