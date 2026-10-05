from typing import Annotated

import typer
from alembic import command

from payments.cli.common import load_migration_settings, new_typer, setup_logging
from payments.db.migrate import alembic_config

app = new_typer("Database migrations (Alembic).")


@app.callback()
def _configure() -> None:
    setup_logging(load_migration_settings())


@app.command()
def upgrade(
    revision: Annotated[str, typer.Argument(help="Target revision.")] = "head",
) -> None:
    """Apply migrations up to REVISION."""
    command.upgrade(alembic_config(), revision)


@app.command()
def downgrade(
    revision: Annotated[str, typer.Argument(help='Target revision, e.g. "-1" or "base".')],
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Do not ask for confirmation.")] = False,
) -> None:
    """Roll migrations back to REVISION."""
    if not yes:
        typer.confirm(f"Downgrade the database to {revision!r}? Data may be lost", abort=True)
    command.downgrade(alembic_config(), revision)


@app.command()
def current(
    verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False,
) -> None:
    """Show the revision applied to the database."""
    command.current(alembic_config(), verbose=verbose)


@app.command()
def history(
    verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False,
) -> None:
    """List all migrations."""
    command.history(alembic_config(), verbose=verbose)


@app.command()
def revision(
    message: Annotated[str, typer.Option("--message", "-m", help="Migration description.")],
    autogenerate: Annotated[
        bool, typer.Option(help="Detect changes by comparing models with the database.")
    ] = True,
) -> None:
    """Create a new migration (for development)."""
    command.revision(alembic_config(), message=message, autogenerate=autogenerate)
