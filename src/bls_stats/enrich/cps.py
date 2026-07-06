"""CPS metadata: series catalog + ln.* mapping tables, enrichment joins (BEH §2.5).

CPS ships its series catalog (`ln.series`) and a set of code→label mapping tables
(`ln.ages`, `ln.sexs`, etc. — anything matching `ln.[a-z_]+` except the data/series/contacts
files) as separate flat files under the LABSTAT `ln/` directory. This module fetches and
caches them, then joins them onto CPS observations to attach human-readable labels. Unlike the
vintage-columned observation tables, these are dimension tables: snapshot-replaced on each
fetch, no `revision`/`benchmark` columns (ARCH §4.2, §8).
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from pathlib import Path

import httpx
import polars as pl
from bs4 import BeautifulSoup

from bls_stats.core.http import Throttle, download
from bls_stats.storage.backend import Store

log = logging.getLogger(__name__)

BASE = "https://download.bls.gov/pub/time.series/ln/"
_MAPPING = re.compile(r"^ln\.[a-z_]+$")
_EXCLUDE = {"ln.series", "ln.txt", "ln.contacts"}


def list_mapping_files(html: bytes) -> list[str]:
    """Extract mapping-table filenames from the `ln/` directory listing page.

    Matches names of the form `ln.<label>` (e.g. `ln.ages`, `ln.footnote`) and excludes the
    series catalog and full-history data files, which are fetched separately.

    Args:
        html: Raw bytes of the LABSTAT `ln/` directory listing page.

    Returns:
        Sorted, deduplicated mapping-table filenames (e.g. `["ln.ages", "ln.footnote", ...]`).
    """
    soup = BeautifulSoup(html, "lxml")
    names = {
        a.get_text(strip=True)
        for a in soup.find_all("a")
        if _MAPPING.match(a.get_text(strip=True)) and a.get_text(strip=True) not in _EXCLUDE
    }
    return sorted(names)


def _read_tsv(path: Path) -> pl.DataFrame:
    """Read a CPS metadata TSV as all-`Utf8` (code columns) with headers and values trimmed."""
    df = pl.read_csv(path, separator="\t", infer_schema_length=0)  # all Utf8 — code columns
    df.columns = [c.strip() for c in df.columns]
    return df.with_columns(pl.col(pl.Utf8).str.strip_chars())


def fetch_metadata(
    client: httpx.Client, dest_dir: Path, *, refresh: bool = False
) -> dict[str, pl.DataFrame]:
    """Fetch the CPS series catalog and all mapping tables, skipping unchanged files.

    A `manifest.json` in `dest_dir` records each fetched file's sha256; a file is re-downloaded
    only if it's missing locally, `refresh` is `True`, or its on-disk hash no longer matches the
    manifest (e.g. a stale/corrupted local copy). Otherwise the cached copy is reused. Requests
    are throttled (`Throttle(2.0)`) to be polite to the LABSTAT server.

    Args:
        client: Shared `httpx.Client`.
        dest_dir: Local cache directory; holds the manifest and every fetched TSV. Not durable
            storage — this is a fetch-time cache, distinct from the store's exported copy
            (`export_metadata`).
        refresh: If `True`, ignore the manifest and re-download every file.

    Returns:
        A dict keyed by table name with the `ln.` prefix stripped (e.g. `"series"`, `"ages"`,
        `"footnote"`), each value a `pl.DataFrame` from `_read_tsv`.
    """
    from bls_stats.core.http import get

    dest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = dest_dir / "manifest.json"
    manifest: dict[str, str] = (
        json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    )
    listing = get(client, BASE).content
    names = ["ln.series", *list_mapping_files(listing)]
    throttle = Throttle(2.0)
    out: dict[str, pl.DataFrame] = {}
    for name in names:
        local = dest_dir / name
        if local.exists() and not refresh and manifest.get(name):
            digest = hashlib.sha256(local.read_bytes()).hexdigest()
            if digest == manifest[name]:
                out[name.removeprefix("ln.")] = _read_tsv(local)
                continue
        throttle.wait()
        download(client, BASE + name, local)
        manifest[name] = hashlib.sha256(local.read_bytes()).hexdigest()
        out[name.removeprefix("ln.")] = _read_tsv(local)
        log.info("cps metadata: fetched %s", name)
    manifest_path.write_text(json.dumps(manifest, indent=1))
    return out


def enrich(obs: pl.DataFrame, meta: dict[str, pl.DataFrame]) -> pl.DataFrame:
    """Left-join CPS metadata onto observations: series catalog, code mappings, footnote text.

    Joins `meta["series"]` on `series_id`, then each remaining mapping table on whichever of
    its columns is named `"{name}_code"` (falling back to the mapping's first column) when
    that column is present in the accumulating output — so a mapping table is silently skipped
    if the observation frame doesn't carry its code column. Footnote resolution is separate: a
    `footnote_codes` value can hold multiple comma-separated codes (e.g. `"P,C"`), each resolved
    independently against `meta["footnote"]` and rejoined with `"; "`; unresolvable or blank
    codes become null rather than an empty string.

    Every join is a left join — **row count never changes**. An observation whose `series_id`
    or code isn't in the catalog keeps its row with nulls for the unmatched columns rather than
    being dropped (BEH §2.5); the function asserts this invariant before returning.

    Args:
        obs: CPS observations, must include `series_id`; may include code columns matching
            mapping tables and `footnote_codes`.
        meta: Metadata tables as returned by `fetch_metadata` (keys without the `ln.` prefix).
            Must include `"series"`; `"footnote"` is optional (skipped if absent).

    Returns:
        `obs` with the series catalog, matching code-mapping labels, and a resolved
        `footnote_text` (`Utf8`, null if `footnote_codes` was empty or unresolvable) joined on.

    Raises:
        AssertionError: The join changed the row count — an enrichment bug, never expected.
    """
    before = obs.height
    out = obs.join(meta["series"], on="series_id", how="left")
    for name, mapping in meta.items():
        if name in ("series", "footnote"):
            continue
        code_col = f"{name}_code" if f"{name}_code" in mapping.columns else mapping.columns[0]
        if code_col in out.columns:
            out = out.join(mapping, on=code_col, how="left")
    if "footnote" in meta and "footnote_codes" in out.columns:
        lookup = dict(meta["footnote"].select("footnote_code", "footnote_text").iter_rows())
        out = out.with_columns(
            pl.col("footnote_codes")
            .str.split(",")
            .list.eval(pl.element().str.strip_chars().replace_strict(lookup, default=None))
            .list.drop_nulls()
            .list.join("; ")
            .replace("", None)
            .alias("footnote_text")
        )
    assert out.height == before, "enrichment must never drop observations (BEH §2.5)"
    return out


def export_metadata(store: Store, meta: dict[str, pl.DataFrame]) -> None:
    """Write CPS metadata tables to the store, snapshot-replacing each one.

    Tags every table with `program="cps"` and overwrites its Delta table in full — metadata
    tables carry no vintage columns and no history; each fetch simply replaces the prior
    snapshot (ARCH §4.2, §8). The series catalog lands at `cps/metadata/series`; every other
    table lands at `cps/metadata/mappings/{name}`.

    Args:
        store: Target `Store` (backend + URI + storage options).
        meta: Metadata tables as returned by `fetch_metadata`.
    """
    for name, df in meta.items():
        tagged = df.with_columns(pl.lit("cps").alias("program"))
        relative = "cps/metadata/series" if name == "series" else f"cps/metadata/mappings/{name}"
        store.replace_table(relative, tagged)
        log.info("cps metadata: exported %s (%d rows)", name, df.height)
