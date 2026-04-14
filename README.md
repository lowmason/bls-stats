# bls-stats

BLS release-date and download pipeline for **QCEW**, **CES**, **SAE**, **BED**, and **JOLTS**.

A Python library and CLI for downloading employment data from the
U.S. Bureau of Labor Statistics and scraping publication release dates from their archive pages.
All downloads return tidy [Polars](https://pola.rs/) DataFrames.

## Features

- **Multi-program downloads** — QCEW (bulk zip), CES, SAE, BED, and JOLTS (public flat files).
- **BLS program registry** — structured series-ID field definitions for positional parsing.
- **Release-date scraping** — extract publication dates from BLS archive pages or Atom feeds.
- **Reference-period helpers** — generate `(year, period)` tuples for monthly and quarterly programs.
- **Polars DataFrames** — all downloads return tidy DataFrames with a common schema.
- **CLI** — `bls-stats download` and `bls-stats release-dates` commands.

## Installation

```bash
pip install bls-stats
```

For development:

```bash
pip install -e ".[docs]"
```

## Configuration

| Variable             | Description                                  | Default              |
|----------------------|----------------------------------------------|----------------------|
| `BLS_DATA_DIR`       | Root directory for downloaded data            | `data`               |
| `BLS_CONTACT_EMAIL`  | Contact email sent in the HTTP `User-Agent`   | `research@example.com` |

No API key is required. All programs download public data from `download.bls.gov`.

## Quick start

### CLI

```bash
# Download QCEW data for all of 2024
bls-stats download --program qcew --year 2024

# Download CES data for Jan–Jun 2024
bls-stats download --program ces --start-date 2024-01 --end-date 2024-06

# Scrape release dates
bls-stats release-dates --program jolts

# Poll Atom feeds instead of scraping HTML
bls-stats release-dates --feed
```

### Python

```python
from bls_stats.bls import reference_periods
from bls_stats.download import download_qcew, download_ces

# QCEW — quarterly periods
periods = reference_periods("qcew", "2024/Q1", "2024/Q2")
df = download_qcew(periods)
print(df.head())

# CES — monthly periods
periods = reference_periods("ces", "2024/01", "2024/06")
df = download_ces(periods)
```

## Documentation

Full docs (including API reference) are built with [MkDocs Material](https://squidfunk.github.io/mkdocs-material/):

```bash
pip install -e ".[docs]"
mkdocs serve
```
