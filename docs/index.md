# bls-stats

BLS release-dates and download pipeline for **QCEW**, **CES**, **SAE**, **BED**, and **JOLTS**.

`bls-stats` provides a Python library and CLI for downloading employment data from the
U.S. Bureau of Labor Statistics and scraping publication release dates from their archive pages.

## Features

- **Multi-program downloads** — QCEW (bulk zip), CES, SAE, BED, and JOLTS (public flat files).
- **BLS program registry** — structured series-ID field definitions for positional parsing.
- **Release-date scraping** — extract publication dates from BLS archive pages.
- **Polars DataFrames** — all downloads return tidy Polars DataFrames with a common schema.
- **CLI** — `bls-stats download` and `bls-stats release-dates` commands.

## Quick start

```bash
pip install bls-stats
```

Download QCEW data for 2024:

```bash
bls-stats download --program qcew --year 2024
```

Or for a specific month range:

```bash
bls-stats download --program qcew --start-date 2024-01 --end-date 2024-06
```

Or use the library directly:

```python
from datetime import date
from bls_stats.download import download_qcew

df = download_qcew(date(2024, 1, 1), date(2024, 6, 1))
print(df.head())
```

See the [Getting Started](getting-started.md) guide for more details, or jump to the
[API Reference](api/index.md).
