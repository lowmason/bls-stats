"""CLI entry point for bls-stats."""

from __future__ import annotations

import logging
import sys
from datetime import date, datetime

import click

logger = logging.getLogger("bls_stats")


def _parse_month(value: str) -> date:
    """Parse YYYY-MM into a date (1st of month)."""
    try:
        return datetime.strptime(value, "%Y-%m").date()
    except ValueError:
        raise click.BadParameter(f"Invalid date format '{value}', expected YYYY-MM")


def _parse_year_range(value: str) -> tuple[date, date]:
    """Parse a year or year range like '2024' or '2020-2024'."""
    parts = value.split("-")
    if len(parts) == 1:
        y = int(parts[0])
        return date(y, 1, 1), date(y, 12, 1)
    if len(parts) == 2:
        return date(int(parts[0]), 1, 1), date(int(parts[1]), 12, 1)
    raise click.BadParameter(f"Invalid year range '{value}', expected YYYY or YYYY-YYYY")


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging.")
def cli(verbose: bool) -> None:
    """BLS release-dates and download pipeline."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        stream=sys.stderr,
    )


@cli.command()
@click.option(
    "--program",
    required=True,
    type=click.Choice(["qcew", "ces", "sae", "bed", "jolts"]),
    help="BLS program to download.",
)
@click.option(
    "--year",
    default=None,
    type=str,
    help="Year or year range (YYYY or YYYY-YYYY).",
)
@click.option(
    "--start-date",
    default=None,
    type=str,
    help="Start date in YYYY-MM format.",
)
@click.option(
    "--end-date",
    default=None,
    type=str,
    help="End date in YYYY-MM format.",
)
def download(
    program: str,
    year: str | None,
    start_date: str | None,
    end_date: str | None,
) -> None:
    """Download bulk data for a single BLS program."""
    if year and (start_date or end_date):
        raise click.UsageError("Use either --year or --start-date/--end-date, not both.")

    if year:
        sd, ed = _parse_year_range(year)
    elif start_date and end_date:
        sd = _parse_month(start_date)
        ed = _parse_month(end_date)
    elif start_date:
        sd = _parse_month(start_date)
        ed = sd
    else:
        raise click.UsageError("Provide --year or --start-date/--end-date.")

    if program == "qcew":
        from bls_stats.download.qcew import download_qcew
        df = download_qcew(sd, ed)
    elif program == "ces":
        from bls_stats.download.ces import download_ces
        df = download_ces(sd, ed)
    elif program == "sae":
        from bls_stats.download.sae import download_sae
        df = download_sae(sd, ed)
    elif program == "bed":
        from bls_stats.download.bed import download_bed
        df = download_bed(sd, ed)
    elif program == "jolts":
        from bls_stats.download.jolts import download_jolts
        df = download_jolts(sd, ed)
    else:
        raise click.UsageError(f"Unknown program: {program}")

    click.echo(f"Downloaded {len(df)} rows for {program} ({sd} to {ed})")


@cli.command("release-dates")
@click.option(
    "--program",
    default=None,
    type=click.Choice(["ces", "sae", "qcew", "bed", "jolts"]),
    help="Scrape a single publication (default: all).",
)
@click.option(
    "--max-releases",
    default=None,
    type=int,
    help="Max releases to scrape per publication.",
)
def release_dates(program: str | None, max_releases: int | None) -> None:
    """Scrape BLS archive pages for publication release dates."""
    from bls_stats.release_dates.scraper import scrape_all, scrape_archive

    if program:
        results = scrape_archive(program, max_releases=max_releases)
    else:
        results = scrape_all(max_per_pub=max_releases)

    for rd in results:
        click.echo(f"{rd.publication:8s}  {rd.release_date}  {rd.title}")

    click.echo(f"\nTotal: {len(results)} release dates scraped.")


if __name__ == "__main__":
    cli()
