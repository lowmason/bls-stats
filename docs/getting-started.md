# Getting Started

## Installation

Install from source (editable):

```bash
pip install -e ".[docs]"
```

## Configuration

`bls-stats` reads configuration from environment variables:

| Variable        | Description                            | Default  |
|-----------------|----------------------------------------|----------|
| `BLS_API_KEY`   | BLS V2 API registration key            | *(none)* |
| `BLS_DATA_DIR`  | Root directory for downloaded data      | `data`   |

!!! note
    The **QCEW** program uses a public CSV endpoint and does not require an API key.
    All other programs (CES, SAE, BED, JOLTS) use the V2 JSON API and **do** require a key.

Register for a free API key at <https://data.bls.gov/registrationEngine/>.

## Downloading data

### Via CLI

```bash
# Download QCEW data for Q1 2024
bls-stats download --program qcew --ref-date 2024-01

# Download CES data for January 2024
bls-stats download --program ces --ref-date 2024-01

# Enable debug logging
bls-stats -v download --program bed --ref-date 2024-01
```

### Via Python

```python
from datetime import date
from bls_stats.download import download_qcew, download_ces

# QCEW — no API key needed
df = download_qcew(date(2024, 1, 1))

# CES — uses BLS_API_KEY env var
df = download_ces(date(2024, 1, 1), api_key="your-key")
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

## Building series IDs

```python
from bls_stats.series import ce_series_id, jt_series_id

# CES: Total nonfarm, all employees
sid = ce_series_id(supersector="00", industry="000000", data_type="01")
print(sid)  # CES0000000001

# JOLTS: Total nonfarm, job openings, national
sid = jt_series_id(data_element="JO")
print(sid)  # JTS00000000000JOJOL
```
