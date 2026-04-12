# bls-stats

BLS release-dates and download pipeline for **QCEW**, **CES**, **SAE**, **BED**, and **JOLTS**.

`bls-stats` provides a Python library and CLI for downloading employment data from the
U.S. Bureau of Labor Statistics and scraping publication release dates from their archive pages.

## Features

- **Multi-program downloads** — QCEW (flat-file API), CES, SAE, BED, and JOLTS (V2 JSON API).
- **Series ID builders** — programmatic construction of BLS series identifiers for each survey.
- **Release-date scraping** — extract publication dates from BLS archive pages.
- **Polars DataFrames** — all downloads return tidy Polars DataFrames with a common schema.
- **CLI** — `bls-stats download` and `bls-stats release-dates` commands.

## Quick start

```bash
pip install bls-stats
```

Set your BLS API key (required for V2 API programs — CES, SAE, BED, JOLTS):

```bash
export BLS_API_KEY="your-key-here"
```

Download QCEW data for a reference quarter:

```bash
bls-stats download --program qcew --ref-date 2024-01
```

Or use the library directly:

```python
from datetime import date
from bls_stats.download import download_qcew

df = download_qcew(date(2024, 1, 1))
print(df.head())
```

See the [Getting Started](getting-started.md) guide for more details, or jump to the
[API Reference](api/index.md).
