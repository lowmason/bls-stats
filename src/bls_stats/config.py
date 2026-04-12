"""Shared configuration — data directories and output filenames."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

DATA_DIR: Path = Path(os.environ.get("BLS_DATA_DIR", "data"))

QCEW_DIR: Path = DATA_DIR / "qcew"
CES_DIR: Path = DATA_DIR / "ces"
SAE_DIR: Path = DATA_DIR / "sae"
BED_DIR: Path = DATA_DIR / "bed"
JOLTS_DIR: Path = DATA_DIR / "jolts"

QCEW_ESTIMATES_FILE: str = "qcew_estimates.parquet"
CES_ESTIMATES_FILE: str = "ces_estimates.parquet"
SAE_ESTIMATES_FILE: str = "sae_estimates.parquet"
BED_ESTIMATES_FILE: str = "bed_estimates.parquet"
JOLTS_ESTIMATES_FILE: str = "jolts_estimates.parquet"
