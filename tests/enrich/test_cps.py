import polars as pl

from bls_stats.enrich.cps import enrich, list_mapping_files

LISTING = b"""<html><body>
<a href="ln.ages">ln.ages</a> <a href="ln.sexs">ln.sexs</a>
<a href="ln.footnote">ln.footnote</a> <a href="ln.series">ln.series</a>
<a href="ln.data.1.AllData">ln.data.1.AllData</a> <a href="ln.txt">ln.txt</a>
</body></html>"""


def test_list_mapping_files_excludes_series_data_and_txt() -> None:
    assert list_mapping_files(LISTING) == ["ln.ages", "ln.footnote", "ln.sexs"]


def _meta() -> dict[str, pl.DataFrame]:
    return {
        "series": pl.DataFrame(
            {
                "series_id": ["LNS14000000"],
                "ages_code": ["00"],
                "series_title": ["Unemployment rate"],
            }
        ),
        "ages": pl.DataFrame({"ages_code": ["00", "16"], "ages_text": ["All ages", "16+"]}),
        "footnote": pl.DataFrame({"footnote_code": ["P"], "footnote_text": ["Preliminary."]}),
    }


def _obs() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "series_id": ["LNS14000000", "LNU99999999"],  # second id NOT in catalog
            "value": [4.2, 1.0],
            "footnote_codes": ["P", ""],
        }
    )


def test_enrich_left_joins_never_drop_rows() -> None:  # BEH §2.5
    out = enrich(_obs(), _meta())
    assert out.height == 2
    assert out.filter(pl.col("series_id") == "LNS14000000")["ages_text"][0] == "All ages"
    assert out.filter(pl.col("series_id") == "LNU99999999")["series_title"][0] is None


def test_enrich_resolves_footnotes() -> None:
    out = enrich(_obs(), _meta())
    assert out.filter(pl.col("series_id") == "LNS14000000")["footnote_text"][0] == "Preliminary."


def test_enrich_resolves_multi_code_footnotes() -> None:
    obs = pl.DataFrame({"series_id": ["LNS14000000"], "value": [4.2], "footnote_codes": ["P,C"]})
    meta = _meta()
    meta["footnote"] = pl.DataFrame(
        {"footnote_code": ["P", "C"], "footnote_text": ["Preliminary.", "Corrected."]}
    )
    out = enrich(obs, meta)
    assert out.height == 1
    assert out["footnote_text"][0] == "Preliminary.; Corrected."


def test_enrich_empty_footnote_codes_resolve_null() -> None:
    out = enrich(_obs(), _meta())
    assert out.filter(pl.col("series_id") == "LNU99999999")["footnote_text"][0] is None
