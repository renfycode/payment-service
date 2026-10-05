"""Надёжность outbox, очистка outbox и healthcheck consumer."""

import secrets
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from faststream.rabbit import RabbitBroker
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from typer.testing import CliRunner

from payments.cli import app as cli
from payments.config import Settings
from payments.consumer.app import HEALTH_PATH
from payments.containers import PUBLISHING_CHANNEL
from payments.db.session import create_session_factory
from payments.messaging import outbox_cleanup, topology
from payments.messaging.outbox_cleanup import uuid7_lower_bound
from payments.messaging.outbox_relay import OutboxRelay
from payments.messaging.topology import PAYMENTS_EXCHANGE, declare_topology
from tests.integration.conftest import StartConsumer
from tests.integration.helpers import eventually

pytestmark = pytest.mark.integration


@pytest.fixture
async def engine(database_url: str) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(database_url)
    yield engine
    await engine.dispose()


async def insert_outbox(
    engine: AsyncEngine, *, created_days_ago: int, published_days_ago: int | None
) -> str:
    now = datetime.now(UTC)
    created = now - timedelta(days=created_days_ago)
    message_id = str(uuid7_at(created))
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO outbox (id, event_type, payload, created_at, published_at) "
                "VALUES (:id, 'payment.created', '{}', :created, :published)"
            ),
            {
                "id": message_id,
                "created": created,
                "published": None
                if published_days_ago is None
                else now - timedelta(days=published_days_ago),
            },
        )
    return message_id


def uuid7_at(moment: datetime) -> UUID:
    """UUIDv7 со временем moment и случайными младшими битами — как у сервиса."""
    random_bits = (secrets.randbits(12) << 64) | secrets.randbits(62)
    return UUID(int=uuid7_lower_bound(moment).int | random_bits)


async def outbox_ids(engine: AsyncEngine) -> set[str]:
    async with engine.connect() as conn:
        return {str(row[0]) for row in await conn.execute(text("SELECT id FROM outbox"))}


async def test_unroutable_event_stays_in_outbox(
    settings: Settings, engine: AsyncEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Событие с маршрутом, у которого нет очереди: брокер вернёт его (mandatory),
    # и relay не должен считать его опубликованным.
    monkeypatch.setitem(topology.EVENT_ROUTES, "test.unroutable", (PAYMENTS_EXCHANGE, "nowhere"))
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO outbox (id, event_type, payload) VALUES (:id, 'test.unroutable', '{}')"
            ),
            {"id": str(uuid4())},
        )
    broker = RabbitBroker(settings.rabbitmq.url, logger=None, default_channel=PUBLISHING_CHANNEL)
    await broker.connect()
    try:
        await declare_topology(broker, (200, 400))
        relay = OutboxRelay(
            create_session_factory(engine),
            broker,
            batch_size=10,
            poll_interval=0.1,
            max_backoff=1,
        )
        result = await relay.publish_batch()
    finally:
        await broker.stop()

    assert result.published == 0
    assert result.error is not None
    async with engine.connect() as conn:
        row = (
            await conn.execute(text("SELECT published_at, attempts, last_error FROM outbox"))
        ).one()
    assert row.published_at is None
    assert row.attempts == 1
    assert row.last_error


async def test_cleanup_deletes_only_old_published_events(engine: AsyncEngine) -> None:
    old_published = await insert_outbox(engine, created_days_ago=200, published_days_ago=200)
    recent_published = await insert_outbox(engine, created_days_ago=10, published_days_ago=10)
    old_unpublished = await insert_outbox(engine, created_days_ago=300, published_days_ago=None)
    session_factory = create_session_factory(engine)
    cutoff = outbox_cleanup.cutoff_for(timedelta(days=180))

    assert await outbox_cleanup.count_published_before(session_factory, cutoff) == 1
    deleted = await outbox_cleanup.delete_published_before(session_factory, cutoff, batch_size=1)

    assert deleted == 1
    assert await outbox_ids(engine) == {recent_published, old_unpublished}
    assert old_published not in await outbox_ids(engine)


async def test_cleanup_works_in_batches(engine: AsyncEngine) -> None:
    for _ in range(5):
        await insert_outbox(engine, created_days_ago=365, published_days_ago=365)
    session_factory = create_session_factory(engine)

    deleted = await outbox_cleanup.delete_published_before(
        session_factory, outbox_cleanup.cutoff_for(timedelta(days=180)), batch_size=2
    )

    assert deleted == 5
    assert await outbox_ids(engine) == set()


def test_cleanup_cli(cli_env: dict[str, str], database_url: str) -> None:
    import asyncio  # noqa: PLC0415 — CLI сам вызывает asyncio.run, тест синхронный

    async def prepare() -> None:
        engine = create_async_engine(database_url)
        await insert_outbox(engine, created_days_ago=400, published_days_ago=400)
        await insert_outbox(engine, created_days_ago=1, published_days_ago=1)
        await engine.dispose()

    asyncio.run(prepare())
    runner = CliRunner()

    declined = runner.invoke(cli, ["outbox", "cleanup"], env=cli_env, input="n\n")
    accepted = runner.invoke(cli, ["outbox", "cleanup", "--yes"], env=cli_env)
    again = runner.invoke(cli, ["outbox", "cleanup", "--older-than-days", "180"], env=cli_env)

    assert declined.exit_code == 1
    assert accepted.exit_code == 0, accepted.output
    assert "Deleted 1 outbox event(s)" in accepted.output
    assert "Nothing to delete" in again.output


async def test_consumer_health_endpoint(start_consumer: StartConsumer) -> None:
    app = await start_consumer()

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://consumer"
    ) as client:
        response = await client.get(HEALTH_PATH)

    assert response.status_code == 204


async def test_consumer_limits_prefetch(
    settings: Settings, start_consumer: StartConsumer, rabbitmq_management_url: str
) -> None:
    # Проверяем то, что видит RabbitMQ, а не настройки: FastStream однажды молча
    # подписывался через канал без лимита.
    await start_consumer()
    rabbit = settings.rabbitmq
    async with httpx.AsyncClient(
        base_url=rabbitmq_management_url,
        auth=(rabbit.user, rabbit.password.get_secret_value()),
    ) as client:

        async def prefetch_by_queue() -> dict[str, int]:
            consumers = (await client.get("/api/consumers")).json()
            return {c["queue"]["name"]: c["prefetch_count"] for c in consumers}

        # Статистика management API обновляется раз в несколько секунд.
        prefetch = await eventually(prefetch_by_queue, lambda p: "payments.new" in p)

    assert prefetch["payments.new"] == settings.consumer.prefetch
