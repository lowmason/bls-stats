from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

from bls_stats.engines.qcew import parse_year_zip

ZIP = Path(__file__).parent.parent / "fixtures" / "qcew" / "2025_qtrly_singlefile_sample.zip"
TS = datetime(2026, 6, 4, 14, 0, tzinfo=UTC)


def test_area_fips_stays_utf8_with_alpha_codes() -> None:  # BEH §2.2 critical gotcha
    df = parse_year_zip(ZIP, [1], downloaded=TS)
    assert df.schema["area_fips"] == pl.Utf8
    assert "C1010" in df["area_fips"].to_list()
    assert "01001" in df["area_fips"].to_list()  # leading zero intact


def test_quarter_filter_and_ref_date() -> None:
    df = parse_year_zip(ZIP, [2], downloaded=TS)
    assert df["ref_date"].unique().to_list() == [date(2025, 6, 12)]


def test_singlefile_keeps_only_size_code_zero() -> None:
    df = parse_year_zip(ZIP, [1], downloaded=TS)
    assert set(df["size_code"].to_list()) == {"0"}


def test_output_contract_columns() -> None:
    df = parse_year_zip(ZIP, [1], downloaded=TS)
    expected = {
        "area_fips",
        "own_code",
        "industry_code",
        "agglvl_code",
        "size_code",
        "disclosure_code",
        "qtrly_estabs",
        "month1_emplvl",
        "month2_emplvl",
        "month3_emplvl",
        "total_qtrly_wages",
        "taxable_qtrly_wages",
        "qtrly_contributions",
        "avg_wkly_wage",
        "ref_date",
        "downloaded",
    }
    assert set(df.columns) == expected  # year/qtr dropped (BEH §2.2)
