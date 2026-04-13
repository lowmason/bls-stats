# Getting Started

## Installation

Install from source (editable):

```bash
pip install -e ".[docs]"
```

## Configuration

`bls-stats` reads configuration from environment variables:

| Variable             | Description                                  | Default                |
|----------------------|----------------------------------------------|------------------------|
| `BLS_DATA_DIR`       | Root directory for downloaded data            | `data`                 |
| `BLS_CONTACT_EMAIL`  | Contact email sent in the HTTP `User-Agent`   | `research@example.com` |

!!! note
    No API key is required. All programs download public data from `download.bls.gov`.
    QCEW uses a bulk annual zip; CES, SAE, BED, and JOLTS use tab-delimited flat files.

## Downloading data

### Via CLI

```bash
# Download QCEW data for all of 2024
bls-stats download --program qcew --year 2024

# Download CES data for Jan–Jun 2024
bls-stats download --program ces --start-date 2024-01 --end-date 2024-06

# Download a multi-year range
bls-stats download --program sae --year 2022-2024

# Enable debug logging
bls-stats -v download --program bed --year 2024
```

### Via Python

```python
from datetime import date
from bls_stats.download import download_qcew, download_ces

# QCEW — bulk annual zip download
df = download_qcew(date(2024, 1, 1), date(2024, 6, 1))

# CES — public flat file download
df = download_ces(date(2024, 1, 1), date(2024, 6, 1))
```

## Scraping release dates

```bash
# Scrape all publications
bls-stats release-dates

# Scrape a single publication, limit to 5 releases
bls-stats release-dates --program ces --max-releases 5
```

```python
from bls_stats.release_dates.scraper import scrape_all

dates = scrape_all(max_per_pub=5)
for rd in dates:
    print(f"{rd.publication:8s}  {rd.release_date}  {rd.title}")
```

## BLS program registry

The `bls_stats.bls` module provides a structured registry of BLS program definitions,
including series-ID field layouts for positional parsing:

```python
from bls_stats.bls import PROGRAMS

ces = PROGRAMS["CE"]
print(ces.name)             # Current Employment Statistics
print(ces.series_id_length) # 13

for name, offset, length in ces.field_slices():
    print(f"  {name}: offset={offset}, length={length}")
```
