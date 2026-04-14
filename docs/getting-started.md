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

Each download function accepts a list of `(year, period)` tuples — months for
CES/SAE/JOLTS, quarters for QCEW/BED. Use `reference_periods` to generate
these from human-readable strings:

```python
from bls_stats.bls import reference_periods
from bls_stats.download import download_qcew, download_ces

# QCEW — quarterly periods
periods = reference_periods("qcew", "2024/Q1", "2024/Q2")
df = download_qcew(periods)

# CES — monthly periods
periods = reference_periods("ces", "2024/01", "2024/06")
df = download_ces(periods)
```

## Scraping release dates

### Via CLI

```bash
# Scrape all publications (archive + schedule HTML)
bls-stats release-dates

# Scrape a single publication
bls-stats release-dates --program ces

# Poll Atom feeds instead of scraping HTML
bls-stats release-dates --feed
```

### Via Python

```python
from bls_stats.release_dates import scrape_all, poll_all

# Scrape archive + schedule pages, merge lapse revisions
df = scrape_all()
print(df)

# Or poll Atom feeds for the latest releases
df = poll_all()
print(df)
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

### Reference periods

The `reference_periods` helper generates the `(year, period)` tuples that the
download functions expect:

```python
from bls_stats.bls import reference_periods

# Monthly programs (CES, SAE, JOLTS) — "YYYY/MM"
reference_periods("ces", "2024/01", "2024/06")
# [(2024, 1), (2024, 2), (2024, 3), (2024, 4), (2024, 5), (2024, 6)]

# Quarterly programs (QCEW, BED) — "YYYY/QN"
reference_periods("qcew", "2023/Q3", "2024/Q2")
# [(2023, 3), (2023, 4), (2024, 1), (2024, 2)]
```
