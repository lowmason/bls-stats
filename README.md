# bls-stats

BLS release-date and download pipeline for **QCEW**, **CES**, **SAE**, **BED**, and **JOLTS**.

A Python library and CLI for downloading employment data from the
U.S. Bureau of Labor Statistics and scraping publication release dates from their archive pages.
All downloads return tidy [Polars](https://pola.rs/) DataFrames.

## Features

- **Multi-program downloads** — QCEW (flat-file API), CES, SAE, BED, and JOLTS (V2 JSON API).
- **Series ID builders** — programmatic construction of BLS series identifiers for each survey.
- **Release-date scraping** — extract publication dates from BLS archive pages.
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

| Variable       | Description                       | Default  |
|----------------|-----------------------------------|----------|
| `BLS_API_KEY`  | BLS V2 API registration key       | *(none)* |
| `BLS_DATA_DIR` | Root directory for downloaded data | `data`   |

QCEW uses a public CSV endpoint and does not require an API key.
All other programs (CES, SAE, BED, JOLTS) use the V2 JSON API and require a key.
Register for free at <https://data.bls.gov/registrationEngine/>.

## Quick start

### CLI

```bash
# Download QCEW data for all of 2024
bls-stats download --program qcew --year 2024

# Download CES data for Jan–Jun 2024
bls-stats download --program ces --start-date 2024-01 --end-date 2024-06

# Scrape release dates
bls-stats release-dates --program jolts --max-releases 10
```

### Python

```python
from datetime import date
from bls_stats.download import download_qcew, download_ces

df = download_qcew(date(2024, 1, 1), date(2024, 6, 1))
print(df.head())
```

## Documentation

Full docs (including API reference) are built with [MkDocs Material](https://squidfunk.github.io/mkdocs-material/):

```bash
pip install -e ".[docs]"
mkdocs serve
```

## License

See [LICENSE](LICENSE) for details.
