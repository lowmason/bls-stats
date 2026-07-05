from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from bls_stats.engines.ep import parse_index, parse_matrix

FIXTURES = Path(__file__).parent.parent / "fixtures" / "ep"
TS = datetime(2026, 7, 2, tzinfo=UTC)


def test_parse_index_extracts_soc_codes() -> None:
    assert parse_index((FIXTURES / "index.html").read_bytes()) == ["11-1011", "15-1252"]


def test_parse_matrix_normalizes_year_headers() -> None:  # BEH §2.4
    df = parse_matrix((FIXTURES / "matrix_11-1011.html").read_bytes(), "11-1011")
    for col in (
        "base_year_employment",
        "projected_year_employment",
        "employment_change",
        "employment_pct_change",
        "industry_title",
        "industry_code",
        "occupation_code",
    ):
        assert col in df.columns, col


def test_numbers_parsed_endash_null() -> None:
    df = parse_matrix((FIXTURES / "matrix_11-1011.html").read_bytes(), "11-1011")
    assert df.schema["base_year_employment"] == pl.Float64
    assert df["base_year_employment"][0] == 211230.0
    utilities = df.filter(pl.col("industry_code") == "22")
    assert utilities["base_year_pct_of_industry"][0] is None  # en-dash
