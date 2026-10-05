"""CLI против настоящих Postgres и RabbitMQ.

Команды CLI сами вызывают asyncio.run, поэтому тесты синхронные,
а подготовка состояния выполняется через asyncio.run.
"""

import asyncio
import json
from uuid import uuid4

import aio_pika
import pytest
from faststream.rabbit import RabbitBroker
from typer.testing import CliRunner

from payments.cli import app
from payments.config import Settings
from payments.messaging import dead_letters
from payments.messaging.topology import (
    DEAD_LETTER_ATTEMPTS_HEADER,
    DEAD_LETTER_EXCHANGE,
    DEAD_LETTER_QUEUE_NAME,
    DEAD_LETTER_REASON_HEADER,
    DEAD_LETTER_STAGE_HEADER,
    NEW_PAYMENTS_QUEUE_NAME,
    declare_topology,
)

pytestmark = pytest.mark.integration

runner = CliRunner()


async def _prepare_dead_letters(settings: Settings, payment_ids: list[str]) -> None:
    broker = RabbitBroker(settings.rabbitmq.url, logger=None)
    await broker.connect()
    try:
        await declare_topology(broker, (200, 400))
        for payment_id in payment_ids:
            await broker.publish(
                {"payment_id": payment_id},
                exchange=DEAD_LETTER_EXCHANGE,
                routing_key=DEAD_LETTER_QUEUE_NAME,
                headers={
                    DEAD_LETTER_STAGE_HEADER: "webhook",
                    DEAD_LETTER_ATTEMPTS_HEADER: 3,
                    DEAD_LETTER_REASON_HEADER: "WebhookDeliveryError('responded 500')",
                },
                persist=True,
            )
    finally:
        await broker.stop()


async def _drain(rabbitmq_url: str, queue_name: str) -> list[aio_pika.abc.AbstractIncomingMessage]:
    connection = await aio_pika.connect(rabbitmq_url)
    async with connection:
        queue = await (await connection.channel()).get_queue(queue_name)
        messages = []
        while (message := await queue.get(no_ack=True, fail=False)) is not None:
            messages.append(message)
        return messages


def test_db_current_shows_head_revision(cli_env: dict[str, str]) -> None:
    result = runner.invoke(app, ["db", "current"], env=cli_env)

    assert result.exit_code == 0, result.output
    assert "0001 (head)" in result.output


def test_dlq_list_shows_reasons_without_consuming(
    settings: Settings, cli_env: dict[str, str]
) -> None:
    payment_id = str(uuid4())
    asyncio.run(_prepare_dead_letters(settings, [payment_id]))

    # Широкий терминал: иначе rich переносит длинную причину по строкам.
    result = runner.invoke(app, ["dlq", "list"], env={**cli_env, "COLUMNS": "200"})

    assert result.exit_code == 0, result.output
    assert "1 message(s)" in result.output
    assert payment_id in result.output
    assert "WebhookDeliveryError" in result.output
    assert asyncio.run(dead_letters.count(settings.rabbitmq.url)) == 1


def test_dlq_requeue_by_payment_moves_only_matching_message(
    settings: Settings, cli_env: dict[str, str]
) -> None:
    target, other = str(uuid4()), str(uuid4())
    asyncio.run(_prepare_dead_letters(settings, [other, target]))

    result = runner.invoke(app, ["dlq", "requeue", "--payment-id", target, "--yes"], env=cli_env)

    assert result.exit_code == 0, result.output
    assert "Requeued 1 message(s)" in result.output
    (requeued,) = asyncio.run(_drain(settings.rabbitmq.url, NEW_PAYMENTS_QUEUE_NAME))
    assert json.loads(requeued.body) == {"payment_id": target}
    # Диагностические заголовки не переносятся: счётчик попыток начнётся заново.
    assert DEAD_LETTER_ATTEMPTS_HEADER not in (requeued.headers or {})
    (left,) = asyncio.run(_drain(settings.rabbitmq.url, DEAD_LETTER_QUEUE_NAME))
    assert json.loads(left.body) == {"payment_id": other}


def test_dlq_requeue_asks_for_confirmation(settings: Settings, cli_env: dict[str, str]) -> None:
    asyncio.run(_prepare_dead_letters(settings, [str(uuid4())]))

    result = runner.invoke(app, ["dlq", "requeue"], env=cli_env, input="n\n")

    assert result.exit_code == 1
    assert asyncio.run(dead_letters.count(settings.rabbitmq.url)) == 1
