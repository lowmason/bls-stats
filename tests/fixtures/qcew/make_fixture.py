"""Builds a miniature QCEW singlefile ZIP with the real column set. Run once; commit the zip."""

import io
import zipfile
from pathlib import Path

COLS = (
    "area_fips,own_code,industry_code,agglvl_code,size_code,year,qtr,disclosure_code,"
    "qtrly_estabs,month1_emplvl,month2_emplvl,month3_emplvl,total_qtrly_wages,"
    "taxable_qtrly_wages,qtrly_contributions,avg_wkly_wage"
)
ROWS = [
    '"01001","0","10","70","0","2025","1","","1200","41000","41100","41200","530000000","1000","500","995"',
    '"01001","0","10","70","0","2025","2","","1210","41300","41350","41400","540000000","1000","500","1002"',
    '"C1010","0","10","40","0","2025","1","","800","30000","30100","30200","410000000","800","400","1050"',
    '"01001","5","10","71","3","2025","1","","300","9000","9010","9020","98000000","200","100","840"',
]

buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
    zf.writestr("2025.q1-q2.singlefile.csv", "\n".join([COLS, *ROWS]) + "\n")
Path(__file__).with_name("2025_qtrly_singlefile_sample.zip").write_bytes(buf.getvalue())
print("fixture written")
