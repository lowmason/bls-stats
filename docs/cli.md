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

Download data for a single BLS program and reference date.

```
bls-stats download --program PROGRAM --ref-date YYYY-MM [--api-key KEY]
```

| Option        | Required | Description                                       |
|---------------|----------|---------------------------------------------------|
| `--program`   | Yes      | One of `qcew`, `ces`, `sae`, `bed`, `jolts`       |
| `--ref-date`  | Yes      | Reference date in `YYYY-MM` format                 |
| `--api-key`   | No       | BLS API key (overrides `BLS_API_KEY` env var)       |

### Examples

```bash
bls-stats download --program qcew --ref-date 2024-01
bls-stats download --program ces --ref-date 2024-06 --api-key abc123
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
