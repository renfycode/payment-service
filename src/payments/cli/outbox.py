import asyncio
from datetime import datetime, timedelta
from typing import Annotated

import typer

from payments.cli.common import console, load_maintenance_settings, new_typer, setup_logging
from payments.db.session import create_engine, create_session_factory
from payments.messaging import outbox_cleanup

app = new_typer("Outbox table maintenance.")


@app.command()
def cleanup(
    older_than_days: Annotated[
        int, typer.Option(min=1, help="Delete published events created more than N days ago.")
    ] = 180,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Do not ask for confirmation.")] = False,
) -> None:
    """Delete published outbox events created more than N days ago.

    Unpublished events are never deleted.
    """
    settings = load_maintenance_settings()
    setup_logging(settings)
    cutoff = outbox_cleanup.cutoff_for(timedelta(days=older_than_days))
    asyncio.run(_cleanup(settings.database.url, cutoff, ask=not yes))


async def _cleanup(database_url: str, cutoff: datetime, *, ask: bool) -> None:
    engine = create_engine(database_url)
    session_factory = create_session_factory(engine)
    try:
        total = await outbox_cleanup.count_published_before(session_factory, cutoff)
        when = f"created before {cutoff:%Y-%m-%d %H:%M} UTC"
        if total == 0:
            console.print(f"Nothing to delete: no outbox events {when}")
            return
        if ask:
            typer.confirm(f"Delete {total} outbox event(s) {when}?", abort=True)
        deleted = await outbox_cleanup.delete_published_before(session_factory, cutoff)
        console.print(f"[green]✓[/] Deleted {deleted} outbox event(s) {when}")
    finally:
        await engine.dispose()
