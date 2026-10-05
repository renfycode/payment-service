import logging
from collections.abc import Mapping
from typing import Any

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage

from payments.config import Settings
from payments.consumer.gateway import EmulatedPaymentGateway
from payments.consumer.processor import (
    Ack,
    DeadLetter,
    PaymentProcessor,
    Retry,
    SqlPaymentStore,
    Stage,
)
from payments.consumer.webhooks import WebhookNotifier, WebhookSigner
from payments.db.session import create_engine, create_session_factory
from payments.logging_config import configure_logging
from payments.messaging.retry import RetryPolicy
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


def create_app(settings: Settings | None = None) -> FastStream:
    settings = settings or Settings()
    configure_logging(settings.log_level, json=settings.log_json)

    retry_policy = RetryPolicy(settings.max_attempts, settings.retry_base_delay)
    # Логгеры стандартного logging без своих обработчиков: записи FastStream
    # всплывают в корневой логгер и оформляются loguru (см. logging_config).
    broker = RabbitBroker(settings.rabbitmq_url, logger=logging.getLogger("faststream.rabbit"))
    engine = create_engine(settings.database_url)
    http_client = httpx.AsyncClient(timeout=settings.webhook_timeout, follow_redirects=False)
    processor = PaymentProcessor(
        SqlPaymentStore(create_session_factory(engine)),
        EmulatedPaymentGateway(
            min_delay=settings.gateway_min_delay,
            max_delay=settings.gateway_max_delay,
            success_rate=settings.gateway_success_rate,
            decline_rate=settings.gateway_decline_rate,
        ),
        WebhookNotifier(http_client, WebhookSigner(settings.webhook_secret.get_secret_value())),
        retry_policy,
    )

    # Если обработчик упал необработанным исключением (например, тело не прошло валидацию),
    # сообщение отклоняется и через DLX очереди попадает в DLQ, а не крутится бесконечно.
    @broker.subscriber(
        new_payments_queue(), PAYMENTS_EXCHANGE, ack_policy=AckPolicy.REJECT_ON_ERROR
    )
    async def handle_payment_created(event: PaymentCreatedEvent, message: RabbitMessage) -> None:
        stage, attempt = parse_retry_headers(message.headers)
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
        await broker.connect()
        await declare_topology(broker, retry_policy.delays_ms)

    async def after_shutdown() -> None:
        await http_client.aclose()
        await engine.dispose()

    return FastStream(
        broker,
        logger=logging.getLogger("faststream.app"),
        on_startup=[on_startup],
        after_shutdown=[after_shutdown],
    )
