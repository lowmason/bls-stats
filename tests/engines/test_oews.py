from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

from bls_stats.engines.oews import parse_workbook_zip

ZIP = Path(__file__).parent.parent / "fixtures" / "oews" / "oesm25all_sample.zip"
TS = datetime(2026, 4, 2, 14, 0, tzinfo=UTC)


def test_columns_lowercased_and_trimmed() -> None:
    df = parse_workbook_zip(ZIP, 2025, downloaded=TS)
    assert "occ_code" in df.columns and "area_title" in df.columns


def test_ref_date_is_may_12() -> None:  # BEH §2.3
    df = parse_workbook_zip(ZIP, 2025, downloaded=TS)
    assert df["ref_date"].unique().to_list() == [date(2025, 5, 12)]


def test_all_rows_present_with_codes_as_strings() -> None:
    df = parse_workbook_zip(ZIP, 2025, downloaded=TS)
    assert df.height == 2
    assert df.schema["area"] == pl.Utf8
