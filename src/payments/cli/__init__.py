"""CLI сервиса: payments --help."""

import asyncio
from typing import Annotated

import typer
import uvicorn
from alembic import command

from payments.cli import config, db, dlq
from payments.cli.common import load_settings, new_typer, setup_logging
from payments.consumer.app import create_app as create_consumer
from payments.containers import ConsumerContainer
from payments.db.migrate import alembic_config

app = new_typer("Payments service: run processes, manage migrations, configuration and DLQ.")
app.add_typer(db.app, name="db")
app.add_typer(config.app, name="config")
app.add_typer(dlq.app, name="dlq")


@app.command()
def api(
    host: Annotated[str, typer.Option(help="Bind address.")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Bind port.")] = 8000,
    workers: Annotated[int, typer.Option(min=1, help="Number of worker processes.")] = 1,
    reload: Annotated[bool, typer.Option(help="Reload on code changes (development).")] = False,
    migrate: Annotated[bool, typer.Option(help="Apply database migrations before start.")] = False,
) -> None:
    """Run the HTTP API (with the outbox relay)."""
    settings = load_settings()
    setup_logging(settings)
    if migrate:
        command.upgrade(alembic_config(), "head")
    # log_config=None: uvicorn не ставит свои обработчики, его записи оформляет loguru.
    uvicorn.run(
        "payments.api.app:create_app",
        factory=True,
        host=host,
        port=port,
        workers=workers,
        reload=reload,
        log_config=None,
    )


@app.command()
def consumer() -> None:
    """Run the payments.new consumer."""
    settings = load_settings()
    asyncio.run(create_consumer(ConsumerContainer(settings=settings)).run())
