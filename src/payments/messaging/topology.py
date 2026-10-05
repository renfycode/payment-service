"""Топология RabbitMQ.

payments (direct) ──payments.new──▶ [payments.new] ──▶ consumer
                                         │
         consumer: техническая ошибка    │ reject / исчерпаны попытки
                    │                    ▼
                    ▼                payments.dlx ──▶ [payments.new.dlq]
payments.retry ──▶ [payments.new.retry.<delay>ms]  (TTL = delay)
                    │ по истечении TTL: dead-letter
                    └──────────────▶ payments ──payments.new──▶ [payments.new]

Под каждую задержку — своя очередь: в одной очереди с разными TTL сообщения
истекают только из головы, и короткие задержки ждали бы длинные.
"""

from typing import TYPE_CHECKING

from faststream.rabbit import ExchangeType, QueueType, RabbitBroker, RabbitExchange, RabbitQueue

if TYPE_CHECKING:
    from faststream.rabbit.schemas.queue import QuorumQueueArgs

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
RETRY_EXCHANGE = RabbitExchange("payments.retry", type=ExchangeType.DIRECT, durable=True)
DEAD_LETTER_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

NEW_PAYMENTS_QUEUE_NAME = "payments.new"
DEAD_LETTER_QUEUE_NAME = "payments.new.dlq"

PAYMENT_CREATED_EVENT = "payment.created"
# Тип события из outbox -> (exchange, routing key).
EVENT_ROUTES: dict[str, tuple[RabbitExchange, str]] = {
    PAYMENT_CREATED_EVENT: (PAYMENTS_EXCHANGE, NEW_PAYMENTS_QUEUE_NAME),
}

# Заголовки сообщения с состоянием повторов.
STAGE_HEADER = "x-stage"
ATTEMPT_HEADER = "x-attempt"
# Диагностика в DLQ. Имена отличаются от x-stage/x-attempt намеренно: сообщение,
# возвращённое из DLQ в payments.new, начинает отсчёт попыток заново.
DEAD_LETTER_STAGE_HEADER = "x-dead-letter-stage"
DEAD_LETTER_ATTEMPTS_HEADER = "x-dead-letter-attempts"
DEAD_LETTER_REASON_HEADER = "x-dead-letter-reason"


def _dead_lettering(exchange: str, routing_key: str) -> "QuorumQueueArgs":
    # Quorum-очереди по умолчанию делают dead-lettering в режиме at-most-once;
    # at-least-once требует x-overflow=reject-publish.
    return {
        "x-dead-letter-exchange": exchange,
        "x-dead-letter-routing-key": routing_key,
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    }


def new_payments_queue() -> RabbitQueue:
    # DLX здесь — страховка для сообщений, которые consumer отклонил (reject),
    # например, если тело не прошло валидацию.
    return RabbitQueue(
        NEW_PAYMENTS_QUEUE_NAME,
        queue_type=QueueType.QUORUM,
        routing_key=NEW_PAYMENTS_QUEUE_NAME,
        arguments=_dead_lettering(DEAD_LETTER_EXCHANGE.name, DEAD_LETTER_QUEUE_NAME),
    )


def dead_letter_queue() -> RabbitQueue:
    return RabbitQueue(
        DEAD_LETTER_QUEUE_NAME,
        queue_type=QueueType.QUORUM,
        routing_key=DEAD_LETTER_QUEUE_NAME,
    )


def retry_queue_name(delay_ms: int) -> str:
    return f"{NEW_PAYMENTS_QUEUE_NAME}.retry.{delay_ms}ms"


def retry_queue(delay_ms: int) -> RabbitQueue:
    name = retry_queue_name(delay_ms)
    return RabbitQueue(
        name,
        queue_type=QueueType.QUORUM,
        routing_key=name,
        arguments={
            **_dead_lettering(PAYMENTS_EXCHANGE.name, NEW_PAYMENTS_QUEUE_NAME),
            "x-message-ttl": delay_ms,
        },
    )


async def declare_topology(broker: RabbitBroker, retry_delays_ms: tuple[int, ...]) -> None:
    """Идемпотентно объявляет exchange, очереди и привязки. Брокер должен быть подключён."""
    for exchange in (PAYMENTS_EXCHANGE, RETRY_EXCHANGE, DEAD_LETTER_EXCHANGE):
        await broker.declare_exchange(exchange)

    bindings = [
        (new_payments_queue(), PAYMENTS_EXCHANGE),
        (dead_letter_queue(), DEAD_LETTER_EXCHANGE),
        *((retry_queue(delay), RETRY_EXCHANGE) for delay in sorted(set(retry_delays_ms))),
    ]
    for queue, exchange in bindings:
        declared = await broker.declare_queue(queue)
        await declared.bind(exchange.name, routing_key=queue.routing())
