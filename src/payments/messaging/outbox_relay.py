import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime

from faststream.rabbit import RabbitBroker
from loguru import logger

from payments.db.models import OutboxMessage
from payments.db.session import SessionFactory
from payments.db.uow import UnitOfWork
from payments.messaging.topology import EVENT_ROUTES


@dataclass(frozen=True, slots=True)
class BatchResult:
    published: int
    error: Exception | None = None


class OutboxRelay:
    """Публикует события из таблицы outbox в RabbitMQ.

    Запись помечается опубликованной только после подтверждения брокера
    (publisher confirms; сообщение, которое некуда доставить, тоже считается ошибкой),
    поэтому гарантия доставки — at-least-once: при сбое между публикацией и commit
    событие уйдёт повторно. Consumer идемпотентен и переживает такие дубли.

    Пока брокер недоступен, пауза между попытками растёт экспоненциально до max_backoff.
    """

    def __init__(
        self,
        session_factory: SessionFactory,
        broker: RabbitBroker,
        *,
        batch_size: int,
        poll_interval: float,
        max_backoff: float,
    ) -> None:
        self._session_factory = session_factory
        self._broker = broker
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self._max_backoff = max_backoff

    async def run(self) -> None:
        logger.info("Outbox relay started")
        failures = 0
        while True:
            try:
                result = await self.publish_batch()
            except Exception as exc:  # например, недоступна БД
                result = BatchResult(published=0, error=exc)

            if result.error is not None:
                failures += 1
                delay = self.backoff(failures)
                logger.warning(
                    "Outbox publishing failed (attempt {}), retry in {:.1f}s: {!r}",
                    failures,
                    delay,
                    result.error,
                )
                await asyncio.sleep(delay)
                continue

            if failures:
                logger.info("Outbox publishing recovered after {} failed attempt(s)", failures)
                failures = 0
            # Полная пачка — вероятно, есть ещё записи, забираем сразу.
            if result.published < self._batch_size:
                await asyncio.sleep(self._poll_interval)

    def backoff(self, failures: int) -> float:
        return min(self._poll_interval * 2.0**failures, self._max_backoff)

    async def publish_batch(self) -> BatchResult:
        """Публикует одну пачку. На первой ошибке останавливается: брокер, скорее всего,
        недоступен, и остаток пачки упадёт так же."""
        published = 0
        error: Exception | None = None
        async with UnitOfWork(self._session_factory) as uow:
            for message in await uow.outbox.lock_unpublished(self._batch_size):
                try:
                    await self._publish(message)
                except Exception as exc:
                    message.attempts += 1
                    message.last_error = repr(exc)
                    error = exc
                    break
                message.published_at = datetime.now(UTC)
                published += 1
            await uow.commit()
        if published:
            logger.info("Published {} outbox message(s)", published)
        return BatchResult(published=published, error=error)

    async def _publish(self, message: OutboxMessage) -> None:
        exchange, routing_key = EVENT_ROUTES[message.event_type]
        await self._broker.publish(
            message.payload,
            exchange=exchange,
            routing_key=routing_key,
            message_id=str(message.id),
            message_type=message.event_type,
            persist=True,
            mandatory=True,
        )
