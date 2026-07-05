"""CPS metadata: series catalog + ln.* mapping tables, enrichment joins (BEH §2.5)."""

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
    soup = BeautifulSoup(html, "lxml")
    names = {
        a.get_text(strip=True)
        for a in soup.find_all("a")
        if _MAPPING.match(a.get_text(strip=True)) and a.get_text(strip=True) not in _EXCLUDE
    }
    return sorted(names)


def _read_tsv(path: Path) -> pl.DataFrame:
    df = pl.read_csv(path, separator="\t", infer_schema_length=0)  # all Utf8 — code columns
    df.columns = [c.strip() for c in df.columns]
    return df.with_columns(pl.col(pl.Utf8).str.strip_chars())


def fetch_metadata(
    client: httpx.Client, dest_dir: Path, *, refresh: bool = False
) -> dict[str, pl.DataFrame]:
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
    for name, df in meta.items():
        tagged = df.with_columns(pl.lit("cps").alias("program"))
        uri = (
            f"{store.uri}/cps/metadata/series"
            if name == "series"
            else f"{store.uri}/cps/metadata/mappings/{name}"
        )
        tagged.write_delta(uri, mode="overwrite", storage_options=store.storage_options)
        log.info("cps metadata: exported %s (%d rows)", name, df.height)
