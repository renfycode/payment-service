import asyncio
from typing import Any
from uuid import UUID, uuid4

import aio_pika
import httpx
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from payments.messaging.topology import DEAD_LETTER_QUEUE_NAME
from tests.integration.conftest import ROOT, StartConsumer
from tests.integration.helpers import (
    DECLINING_GATEWAY,
    UNAVAILABLE_GATEWAY,
    WebhookReceiver,
    eventually,
)

pytestmark = pytest.mark.integration


def payment_body(webhook_url: str, **overrides: Any) -> dict[str, Any]:
    return {
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": {"order_id": 42},
        "webhook_url": webhook_url,
        **overrides,
    }


async def create_payment(api: httpx.AsyncClient, body: dict[str, Any], key: str = "") -> UUID:
    response = await api.post(
        "/api/v1/payments", json=body, headers={"Idempotency-Key": key or str(uuid4())}
    )
    assert response.status_code == 202, response.text
    return UUID(response.json()["payment_id"])


async def get_status(api: httpx.AsyncClient, payment_id: UUID) -> str:
    response = await api.get(f"/api/v1/payments/{payment_id}")
    response.raise_for_status()
    status: str = response.json()["status"]
    return status


async def dead_letters(rabbitmq_url: str) -> list[aio_pika.abc.AbstractIncomingMessage]:
    connection = await aio_pika.connect(rabbitmq_url)
    async with connection:
        queue = await (await connection.channel()).get_queue(DEAD_LETTER_QUEUE_NAME)
        messages = []
        while (message := await queue.get(no_ack=True, fail=False)) is not None:
            messages.append(message)
        return messages


async def test_successful_payment_is_processed_and_webhook_delivered(
    api: httpx.AsyncClient,
    start_consumer: StartConsumer,
    receiver: WebhookReceiver,
    database_url: str,
) -> None:
    await start_consumer()
    payment_id = await create_payment(api, payment_body(receiver.url("success")))

    deliveries = await eventually(lambda: receiver.deliveries("success"), lambda d: len(d) == 1)

    (delivery,) = deliveries
    assert delivery["status_code"] == 204
    assert delivery["signature_valid"] is True
    assert delivery["headers"]["webhook-id"] == f"msg_{payment_id.hex}"
    assert delivery["body"]["type"] == "payment.succeeded"
    assert delivery["body"]["data"]["id"] == str(payment_id)
    assert delivery["body"]["data"]["amount"] == "1500.00"

    payment = (await api.get(f"/api/v1/payments/{payment_id}")).json()
    assert payment["status"] == "succeeded"
    assert payment["processed_at"] is not None
    assert payment["metadata"] == {"order_id": 42}

    engine = create_async_engine(database_url)
    async with engine.connect() as conn:
        unpublished = await conn.scalar(
            text("SELECT count(*) FROM outbox WHERE published_at IS NULL")
        )
    await engine.dispose()
    assert unpublished == 0


async def test_declined_payment_is_failed_and_notified(
    api: httpx.AsyncClient, start_consumer: StartConsumer, receiver: WebhookReceiver
) -> None:
    await start_consumer(gateway=DECLINING_GATEWAY)
    payment_id = await create_payment(api, payment_body(receiver.url("declined")))

    (delivery,) = await eventually(lambda: receiver.deliveries("declined"), lambda d: len(d) == 1)

    assert delivery["body"]["type"] == "payment.failed"
    assert await get_status(api, payment_id) == "failed"


async def test_webhook_is_retried_until_delivered(
    api: httpx.AsyncClient,
    start_consumer: StartConsumer,
    receiver: WebhookReceiver,
    rabbitmq_url: str,
) -> None:
    await start_consumer()
    await create_payment(api, payment_body(receiver.url("flaky", fail=2)))

    deliveries = await eventually(lambda: receiver.deliveries("flaky"), lambda d: len(d) == 3)

    assert [d["status_code"] for d in deliveries] == [500, 500, 204]
    # Один и тот же webhook-id во всех попытках — получатель может дедуплицировать.
    assert len({d["headers"]["webhook-id"] for d in deliveries}) == 1
    # Экспоненциальная задержка: вторая пауза примерно вдвое длиннее первой.
    first_gap = deliveries[1]["received_at"] - deliveries[0]["received_at"]
    second_gap = deliveries[2]["received_at"] - deliveries[1]["received_at"]
    assert first_gap >= 0.2
    assert second_gap >= 0.4
    assert await dead_letters(rabbitmq_url) == []


async def test_webhook_exhausted_goes_to_dlq(
    api: httpx.AsyncClient,
    start_consumer: StartConsumer,
    receiver: WebhookReceiver,
    rabbitmq_url: str,
) -> None:
    await start_consumer()
    payment_id = await create_payment(api, payment_body(receiver.url("down", fail=100)))

    messages = await eventually(lambda: dead_letters(rabbitmq_url), lambda m: len(m) == 1)

    assert len(await receiver.deliveries("down")) == 3
    headers = messages[0].headers
    assert headers["x-dead-letter-stage"] == "webhook"
    assert headers["x-dead-letter-attempts"] == 3
    assert "500" in str(headers["x-dead-letter-reason"])
    # Сам платёж проведён — недоставленный webhook статус не меняет.
    assert await get_status(api, payment_id) == "succeeded"


async def test_gateway_outage_leaves_payment_pending_and_goes_to_dlq(
    api: httpx.AsyncClient,
    start_consumer: StartConsumer,
    receiver: WebhookReceiver,
    rabbitmq_url: str,
) -> None:
    await start_consumer(gateway=UNAVAILABLE_GATEWAY)
    payment_id = await create_payment(api, payment_body(receiver.url("outage")))

    messages = await eventually(lambda: dead_letters(rabbitmq_url), lambda m: len(m) == 1)

    assert messages[0].headers["x-dead-letter-stage"] == "gateway"
    assert messages[0].headers["x-dead-letter-attempts"] == 3
    assert await get_status(api, payment_id) == "pending"
    assert await receiver.deliveries("outage") == []


async def test_events_published_while_consumer_is_down_are_processed_later(
    api: httpx.AsyncClient, start_consumer: StartConsumer, receiver: WebhookReceiver
) -> None:
    payment_id = await create_payment(api, payment_body(receiver.url("later")))
    await asyncio.sleep(0.5)  # relay успевает опубликовать событие, consumer ещё не запущен

    await start_consumer()

    await eventually(lambda: receiver.deliveries("later"), lambda d: len(d) == 1)
    assert await get_status(api, payment_id) == "succeeded"


async def test_same_idempotency_key_and_body_returns_same_payment(
    api: httpx.AsyncClient, database_url: str
) -> None:
    body = payment_body("https://example.com/hook")
    headers = {"Idempotency-Key": "order-42"}

    first = await api.post("/api/v1/payments", json=body, headers=headers)
    second = await api.post("/api/v1/payments", json=body, headers=headers)

    assert first.status_code == second.status_code == 202
    assert first.json() == second.json()
    assert "idempotent-replayed" not in first.headers
    assert second.headers["idempotent-replayed"] == "true"

    engine = create_async_engine(database_url)
    async with engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM outbox")) == 1
    await engine.dispose()


async def test_same_idempotency_key_with_different_body_conflicts(
    api: httpx.AsyncClient,
) -> None:
    headers = {"Idempotency-Key": "order-43"}
    body = payment_body("https://example.com/hook")
    await api.post("/api/v1/payments", json=body, headers=headers)

    response = await api.post("/api/v1/payments", json={**body, "amount": "1.00"}, headers=headers)

    assert response.status_code == 409


async def test_concurrent_requests_with_same_key_create_one_payment(
    api: httpx.AsyncClient,
) -> None:
    body = payment_body("https://example.com/hook")

    responses = await asyncio.gather(
        *(
            api.post("/api/v1/payments", json=body, headers={"Idempotency-Key": "race"})
            for _ in range(10)
        )
    )

    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1


async def test_api_key_is_required(api: httpx.AsyncClient) -> None:
    body = payment_body("https://example.com/hook")
    no_key = {"X-API-Key": ""}

    post = await api.post("/api/v1/payments", json=body, headers={**no_key, "Idempotency-Key": "k"})
    get = await api.get(f"/api/v1/payments/{uuid4()}", headers={"X-API-Key": "wrong"})

    assert post.status_code == get.status_code == 401


async def test_unknown_payment_is_404(api: httpx.AsyncClient) -> None:
    assert (await api.get(f"/api/v1/payments/{uuid4()}")).status_code == 404


async def test_idempotency_key_header_is_required(api: httpx.AsyncClient) -> None:
    response = await api.post("/api/v1/payments", json=payment_body("https://example.com/h"))

    assert response.status_code == 422


def test_migrations_match_models(database_url: str) -> None:
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "migrations"))
    config.set_main_option("sqlalchemy.url", database_url)

    command.check(config)  # бросает исключение, если модели разошлись с миграциями
