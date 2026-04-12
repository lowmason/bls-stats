# CLI Reference

`bls-stats` exposes a Click-based CLI with two subcommands.

## Usage

```
bls-stats [OPTIONS] COMMAND [ARGS]...
```

### Global options

| Option          | Description          |
|-----------------|----------------------|
| `-v, --verbose` | Enable debug logging |

## `download`

Download bulk data for a single BLS program over a date range.

```
bls-stats download --program PROGRAM [--year YYYY | YYYY-YYYY]
                    [--start-date YYYY-MM] [--end-date YYYY-MM]
```

Provide **either** `--year` or `--start-date`/`--end-date`, not both.

| Option          | Required | Description                                       |
|-----------------|----------|---------------------------------------------------|
| `--program`     | Yes      | One of `qcew`, `ces`, `sae`, `bed`, `jolts`       |
| `--year`        | No*      | Year (`YYYY`) or year range (`YYYY-YYYY`)          |
| `--start-date`  | No*      | Start date in `YYYY-MM` format                     |
| `--end-date`    | No       | End date in `YYYY-MM` format (defaults to start)   |

*One of `--year` or `--start-date` is required.

### Examples

```bash
# Full year
bls-stats download --program qcew --year 2024

# Multi-year range
bls-stats download --program sae --year 2020-2024

# Month range
bls-stats download --program ces --start-date 2024-01 --end-date 2024-06

# Single month
bls-stats download --program bed --start-date 2024-03
```

## `release-dates`

Scrape BLS archive pages for publication release dates.

```
bls-stats release-dates [--program PROGRAM] [--max-releases N]
```

| Option            | Required | Description                                        |
|-------------------|----------|----------------------------------------------------|
| `--program`       | No       | Scrape a single program (default: all)              |
| `--max-releases`  | No       | Max releases to scrape per publication               |

### Examples

```bash
bls-stats release-dates
bls-stats release-dates --program jolts --max-releases 10
```
