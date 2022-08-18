"""Main script for the Catalyst Dashboard."""
import typer

from commands import challenges

app = typer.Typer()

app.add_typer(
    challenges.app,
    name="challenges",
    help="Generate challenge specific data."
)

if __name__ == "__main__":
    app()
