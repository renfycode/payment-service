"""Просмотр и возврат сообщений из DLQ (payments.new.dlq)."""

import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import aio_pika
from aio_pika.abc import AbstractIncomingMessage

from payments.messaging.topology import (
    DEAD_LETTER_ATTEMPTS_HEADER,
    DEAD_LETTER_QUEUE_NAME,
    DEAD_LETTER_REASON_HEADER,
    DEAD_LETTER_STAGE_HEADER,
    NEW_PAYMENTS_QUEUE_NAME,
    PAYMENTS_EXCHANGE,
)


@dataclass(frozen=True, slots=True)
class DeadLetterInfo:
    message_id: str | None
    payment_id: str | None
    stage: str | None
    attempts: int | None
    reason: str


def describe(message: AbstractIncomingMessage) -> DeadLetterInfo:
    headers: dict[str, Any] = dict(message.headers or {})
    try:
        payment_id = str(json.loads(message.body)["payment_id"])
    except (ValueError, KeyError, TypeError):
        payment_id = None
    reason = headers.get(DEAD_LETTER_REASON_HEADER)
    if reason is None:
        # Отклонено брокером через DLX очереди: тело не удалось разобрать.
        reason = "rejected by consumer (unprocessable message)"
    attempts = headers.get(DEAD_LETTER_ATTEMPTS_HEADER)
    return DeadLetterInfo(
        message_id=message.message_id,
        payment_id=payment_id,
        stage=headers.get(DEAD_LETTER_STAGE_HEADER),
        attempts=int(attempts) if attempts is not None else None,
        reason=str(reason),
    )


async def count(rabbitmq_url: str) -> int:
    connection = await aio_pika.connect(rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(DEAD_LETTER_QUEUE_NAME, passive=True)
        return queue.declaration_result.message_count or 0


async def peek(rabbitmq_url: str, limit: int) -> list[DeadLetterInfo]:
    """Читает до limit сообщений, не удаляя их из очереди.

    Сообщения забираются без подтверждения и возвращаются в очередь при закрытии канала.
    """
    connection = await aio_pika.connect(rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(DEAD_LETTER_QUEUE_NAME, passive=True)
        result: list[DeadLetterInfo] = []
        while len(result) < limit:
            message = await queue.get(no_ack=False, fail=False)
            if message is None:
                break
            result.append(describe(message))
        return result


async def requeue(
    rabbitmq_url: str, *, limit: int | None = None, payment_id: UUID | None = None
) -> list[DeadLetterInfo]:
    """Возвращает сообщения из DLQ в payments.new и возвращает список перенесённых.

    Счётчик попыток начинается заново: диагностические заголовки DLQ не переносятся.
    Сначала публикация с подтверждением брокера, потом ack в DLQ — при сбое между
    шагами получим дубль (consumer идемпотентен), но не потеряем сообщение.
    Неподходящие под фильтр сообщения остаются в DLQ.
    """
    moved: list[DeadLetterInfo] = []
    connection = await aio_pika.connect(rabbitmq_url)
    async with connection:
        channel = await connection.channel(publisher_confirms=True)
        queue = await channel.declare_queue(DEAD_LETTER_QUEUE_NAME, passive=True)
        exchange = await channel.get_exchange(PAYMENTS_EXCHANGE.name)
        while limit is None or len(moved) < limit:
            message = await queue.get(no_ack=False, fail=False)
            if message is None:
                break
            info = describe(message)
            if payment_id is not None and info.payment_id != str(payment_id):
                continue  # останется неподтверждённым и вернётся в DLQ при закрытии канала
            await exchange.publish(
                aio_pika.Message(
                    body=message.body,
                    content_type=message.content_type,
                    message_id=message.message_id,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key=NEW_PAYMENTS_QUEUE_NAME,
                mandatory=True,
            )
            await message.ack()
            moved.append(info)
    return moved
