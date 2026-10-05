import logging
from collections.abc import Mapping
from typing import Any

from faststream import AckPolicy
from faststream.asgi import AsgiFastStream, make_ping_asgi
from faststream.rabbit import Channel
from faststream.rabbit.annotations import RabbitMessage

from payments.config import Settings
from payments.consumer.processor import Ack, DeadLetter, Retry, Stage
from payments.containers import ConsumerContainer, init_resources, shutdown_resources
from payments.logging_config import configure_logging
from payments.messaging.topology import (
    ATTEMPT_HEADER,
    DEAD_LETTER_ATTEMPTS_HEADER,
    DEAD_LETTER_EXCHANGE,
    DEAD_LETTER_QUEUE_NAME,
    DEAD_LETTER_REASON_HEADER,
    DEAD_LETTER_STAGE_HEADER,
    PAYMENTS_EXCHANGE,
    RETRY_EXCHANGE,
    STAGE_HEADER,
    declare_topology,
    new_payments_queue,
    retry_queue_name,
)
from payments.schemas import PaymentCreatedEvent


def parse_retry_headers(headers: Mapping[str, Any]) -> tuple[Stage | None, int]:
    raw_stage = headers.get(STAGE_HEADER)
    stage = Stage(raw_stage) if raw_stage in set(Stage) else None
    try:
        attempt = max(int(headers.get(ATTEMPT_HEADER, 1)), 1)
    except (TypeError, ValueError):
        attempt = 1
    return stage, attempt


# Проверка здоровья: consumer подключён к RabbitMQ (ping брокера не дольше 5 с).
HEALTH_PATH = "/health"


def create_app(container: ConsumerContainer | None = None) -> AsgiFastStream:
    if container is None:
        # Обязательные секреты приходят из окружения — pyright этого не видит.
        container = ConsumerContainer(settings=Settings())  # pyright: ignore[reportCallIssue]
    settings = container.settings()
    configure_logging(settings.logging.level, json=settings.logging.format == "json")

    broker = container.broker()

    # Если обработчик упал необработанным исключением (например, тело не прошло валидацию),
    # сообщение отклоняется и через DLX очереди попадает в DLQ, а не крутится бесконечно.
    # prefetch ограничивает число сообщений, которые экземпляр обрабатывает одновременно.
    @broker.subscriber(
        new_payments_queue(),
        PAYMENTS_EXCHANGE,
        channel=Channel(prefetch_count=settings.consumer.prefetch),
        ack_policy=AckPolicy.REJECT_ON_ERROR,
    )
    async def handle_payment_created(event: PaymentCreatedEvent, message: RabbitMessage) -> None:
        stage, attempt = parse_retry_headers(message.headers)
        # FastStream разбирает сигнатуру обработчика сам (fast_depends), маркеры Provide
        # из dependency-injector он не понимает — берём зависимость из контейнера явно.
        # Processor зависит от async-ресурсов, поэтому провайдер возвращает awaitable.
        processor = await container.processor.async_()
        outcome = await processor.process(event.payment_id, stage, attempt)
        body = event.model_dump(mode="json")

        match outcome:
            case Ack():
                pass
            case Retry(stage=next_stage, attempt=next_attempt, delay_ms=delay_ms):
                # Сначала публикуем копию в очередь задержки, потом ack оригинала:
                # при падении между шагами получим дубль, но не потеряем сообщение.
                await broker.publish(
                    body,
                    exchange=RETRY_EXCHANGE,
                    routing_key=retry_queue_name(delay_ms),
                    headers={STAGE_HEADER: next_stage.value, ATTEMPT_HEADER: next_attempt},
                    message_id=message.message_id,
                    persist=True,
                )
            case DeadLetter(stage=failed_stage, attempt=failed_attempt, reason=reason):
                await broker.publish(
                    body,
                    exchange=DEAD_LETTER_EXCHANGE,
                    routing_key=DEAD_LETTER_QUEUE_NAME,
                    headers={
                        DEAD_LETTER_STAGE_HEADER: failed_stage.value,
                        DEAD_LETTER_ATTEMPTS_HEADER: failed_attempt,
                        DEAD_LETTER_REASON_HEADER: reason,
                    },
                    message_id=message.message_id,
                    persist=True,
                )
        await message.ack()

    async def on_startup() -> None:
        # До старта брокера: подписчик начнёт получать сообщения уже с готовыми ресурсами.
        await init_resources(container)
        await broker.connect()
        # payments.new объявит подписчик — со своим каналом и prefetch.
        await declare_topology(
            broker, container.core.retry_policy().delays_ms, include_new_payments_queue=False
        )

    async def after_shutdown() -> None:
        # После остановки брокера: обработчики завершены, ресурсы можно закрывать.
        await shutdown_resources(container)

    # ASGI-приложение: тот же FastStream плюс HTTP-эндпоинт для healthcheck контейнера.
    return AsgiFastStream(
        broker,
        asgi_routes=[(HEALTH_PATH, make_ping_asgi(broker, timeout=5.0, include_in_schema=False))],
        logger=logging.getLogger("faststream.app"),
        on_startup=[on_startup],
        after_shutdown=[after_shutdown],
    )
