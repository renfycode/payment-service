"""CLI сервиса: payments --help."""

from typing import Annotated

import typer
import uvicorn
from alembic import command

from payments.cli import config, db, dlq, outbox
from payments.cli.common import load_settings, new_typer, setup_logging
from payments.consumer.app import create_app as create_consumer
from payments.containers import ConsumerContainer
from payments.db.migrate import alembic_config

app = new_typer(
    "Payments service: run processes, manage migrations, configuration, DLQ and outbox."
)
app.add_typer(db.app, name="db")
app.add_typer(config.app, name="config")
app.add_typer(dlq.app, name="dlq")
app.add_typer(outbox.app, name="outbox")


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
def consumer(
    host: Annotated[str, typer.Option(help="Bind address of the health endpoint.")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Port of the health endpoint (GET /health).")] = 8001,
) -> None:
    """Run the payments.new consumer with a health endpoint for container checks."""
    settings = load_settings()
    setup_logging(settings)
    uvicorn.run(
        create_consumer(ConsumerContainer(settings=settings)),
        host=host,
        port=port,
        log_config=None,
        access_log=False,  # healthcheck дёргается каждые несколько секунд
    )
