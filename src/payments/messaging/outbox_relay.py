import asyncio
from datetime import UTC, datetime

from faststream.rabbit import RabbitBroker
from loguru import logger

from payments.db.models import OutboxMessage
from payments.db.session import SessionFactory
from payments.db.uow import UnitOfWork
from payments.messaging.topology import EVENT_ROUTES


class OutboxRelay:
    """Публикует события из таблицы outbox в RabbitMQ.

    Запись помечается опубликованной только после подтверждения брокера
    (publisher confirms), поэтому гарантия доставки — at-least-once:
    при сбое между публикацией и commit событие уйдёт повторно.
    Consumer идемпотентен и переживает такие дубли.
    """

    def __init__(
        self,
        session_factory: SessionFactory,
        broker: RabbitBroker,
        *,
        batch_size: int,
        poll_interval: float,
    ) -> None:
        self._session_factory = session_factory
        self._broker = broker
        self._batch_size = batch_size
        self._poll_interval = poll_interval

    async def run(self) -> None:
        logger.info("Outbox relay started")
        while True:
            try:
                published = await self.publish_batch()
            except Exception:
                logger.exception("Outbox relay iteration failed")
                published = 0
            # Полная пачка — вероятно, есть ещё записи, забираем сразу.
            if published < self._batch_size:
                await asyncio.sleep(self._poll_interval)

    async def publish_batch(self) -> int:
        """Публикует одну пачку и возвращает число опубликованных сообщений."""
        published = 0
        async with UnitOfWork(self._session_factory) as uow:
            for message in await uow.outbox.lock_unpublished(self._batch_size):
                try:
                    await self._publish(message)
                except Exception as exc:
                    message.attempts += 1
                    message.last_error = repr(exc)
                    logger.bind(outbox_id=str(message.id)).warning(
                        "Failed to publish outbox message: {!r}", exc
                    )
                    # Брокер, скорее всего, недоступен: не долбим его остатком пачки.
                    break
                message.published_at = datetime.now(UTC)
                published += 1
            await uow.commit()
        if published:
            logger.info("Published {} outbox message(s)", published)
        return published

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
