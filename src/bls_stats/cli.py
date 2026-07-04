import typer

app = typer.Typer(help="Vintage-aware BLS data downloads and ingest.")


@app.callback()
def main() -> None:
    """bls-stats CLI."""
