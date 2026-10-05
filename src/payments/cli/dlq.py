import asyncio
from typing import Annotated
from uuid import UUID

import typer
from rich.table import Table

from payments.cli.common import console, load_settings, new_typer, setup_logging
from payments.messaging import dead_letters
from payments.messaging.topology import DEAD_LETTER_QUEUE_NAME, NEW_PAYMENTS_QUEUE_NAME

app = new_typer(f"Inspect and requeue dead letters ({DEAD_LETTER_QUEUE_NAME}).")


@app.command("list")
def list_(
    limit: Annotated[int, typer.Option(min=1, help="Maximum number of messages to show.")] = 20,
) -> None:
    """Show dead letters with failure reasons. Messages stay in the queue."""
    settings = load_settings()
    setup_logging(settings)
    total = asyncio.run(dead_letters.count(settings.rabbitmq.url))
    letters = asyncio.run(dead_letters.peek(settings.rabbitmq.url, limit))

    table = Table(title=f"{DEAD_LETTER_QUEUE_NAME}: {total} message(s)", title_justify="left")
    table.add_column("payment_id", no_wrap=True)
    table.add_column("stage", no_wrap=True)
    table.add_column("attempts", justify="right", no_wrap=True)
    table.add_column("reason", overflow="fold")
    for letter in letters:
        table.add_row(
            letter.payment_id or "—",
            letter.stage or "—",
            str(letter.attempts) if letter.attempts is not None else "—",
            letter.reason,
        )
    console.print(table)
    if total > len(letters):
        console.print(f"[dim]… and {total - len(letters)} more (use --limit)[/]")


@app.command("requeue")
def requeue(
    payment_id: Annotated[
        UUID | None, typer.Option(help="Requeue only messages of this payment.")
    ] = None,
    limit: Annotated[
        int | None, typer.Option(min=1, help="Maximum number of messages to requeue.")
    ] = None,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Do not ask for confirmation.")] = False,
) -> None:
    """Move dead letters back to payments.new; attempt counters start over."""
    settings = load_settings()
    setup_logging(settings)
    total = asyncio.run(dead_letters.count(settings.rabbitmq.url))
    if total == 0:
        console.print(f"{DEAD_LETTER_QUEUE_NAME} is empty")
        return

    scope = f"payment {payment_id}" if payment_id else f"up to {limit or total} message(s)"
    if not yes:
        typer.confirm(
            f"Requeue {scope} from {DEAD_LETTER_QUEUE_NAME} ({total} total) "
            f"to {NEW_PAYMENTS_QUEUE_NAME}?",
            abort=True,
        )
    moved = asyncio.run(
        dead_letters.requeue(settings.rabbitmq.url, limit=limit, payment_id=payment_id)
    )
    console.print(f"[green]✓[/] Requeued {len(moved)} message(s) to {NEW_PAYMENTS_QUEUE_NAME}")
