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


def test_oews_locks_all_code_columns_to_utf8(tmp_path) -> None:  # C-18
    import zipfile

    import xlsxwriter

    xlsx = tmp_path / "book.xlsx"
    wb = xlsxwriter.Workbook(str(xlsx))
    ws = wb.add_worksheet("All May 2025 data")
    for col, name in enumerate(["AREA", "OCC_CODE", "NAICS", "OWN_CODE", "TOT_EMP"]):
        ws.write(0, col, name)
    ws.write(1, 0, "0000000")
    ws.write(1, 1, "00-0000")
    ws.write_number(1, 2, 11990)  # NAICS as a NUMBER → Excel engine infers Int64 (the C-18 trap)
    ws.write(1, 3, "05")  # own_code, text with a leading zero
    ws.write_number(1, 4, 100)
    wb.close()
    zpath = tmp_path / "oesm25all.zip"
    with zipfile.ZipFile(zpath, "w") as zf:
        zf.write(xlsx, "oesm25all.xlsx")

    df = parse_workbook_zip(zpath, 2025, downloaded=datetime(2025, 5, 12, tzinfo=UTC))
    for c in ("area", "occ_code", "naics", "own_code"):
        assert df.schema[c] == pl.Utf8
    assert df.filter(pl.col("own_code") == "05").height == 1  # leading zero preserved
