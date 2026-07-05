"""Miniature OEWS workbook zip: one sheet named like the real one. Run once; commit the zip."""

import io
import zipfile
from pathlib import Path

import xlsxwriter

xbuf = io.BytesIO()
wb = xlsxwriter.Workbook(xbuf, {"in_memory": True})
ws = wb.add_worksheet("All May 2025 data")
for col, name in enumerate(["AREA", "AREA_TITLE", "OCC_CODE ", "TOT_EMP", "A_MEAN"]):
    ws.write(0, col, name)  # note trailing space on OCC_CODE — tests the trim
rows = [["01", "Alabama", "00-0000", 1900000, 58000], ["01", "Alabama", "11-1011", 3000, 210000]]
for r, row in enumerate(rows, start=1):
    for c, v in enumerate(row):
        ws.write(r, c, v)
wb.close()

buf = io.BytesIO()
with zipfile.ZipFile(buf, "w") as zf:
    zf.writestr("oesm25all/all_data_M_2025.xlsx", xbuf.getvalue())
Path(__file__).with_name("oesm25all_sample.zip").write_bytes(buf.getvalue())
print("fixture written")
