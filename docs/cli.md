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

Scrape BLS archive pages (or poll Atom feeds) for publication release dates.

```
bls-stats release-dates [--program PROGRAM] [--feed]
```

| Option      | Required | Description                                              |
|-------------|----------|----------------------------------------------------------|
| `--program` | No       | Scrape a single program (default: all)                    |
| `--feed`    | No       | Use Atom feed polling instead of HTML scraping            |

By default the command scrapes BLS archive and schedule HTML pages.
Pass `--feed` to poll the Atom RSS feeds instead.

### Examples

```bash
# Scrape all programs (archive + schedule HTML)
bls-stats release-dates

# Scrape a single program
bls-stats release-dates --program jolts

# Poll Atom feeds instead of scraping
bls-stats release-dates --feed

# Poll a single program's feed
bls-stats release-dates --program ces --feed
```
