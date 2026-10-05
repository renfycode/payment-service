"""Интеграционное окружение: настоящие Postgres, RabbitMQ и получатель webhook в Docker.

API и consumer запускаются в процессе тестов: так видны их логи и легко
подменять настройки (поведение шлюза, задержки повторов) под конкретный сценарий.
"""

from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from pathlib import Path
from typing import Any

import aio_pika
import httpx
import pytest
from alembic import command
from alembic.config import Config
from asgi_lifespan import LifespanManager
from faststream import FastStream
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.rabbitmq import RabbitMqContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.image import DockerImage
from testcontainers.core.wait_strategies import HttpWaitStrategy

from payments.api.app import create_app as create_api
from payments.config import Settings
from payments.consumer.app import create_app as create_consumer
from payments.messaging.retry import RetryPolicy
from payments.messaging.topology import (
    DEAD_LETTER_QUEUE_NAME,
    NEW_PAYMENTS_QUEUE_NAME,
    retry_queue_name,
)
from tests.integration.helpers import WebhookReceiver

ROOT = Path(__file__).parents[2]
API_KEY = "test-api-key"
WEBHOOK_SECRET = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
RETRY_BASE_DELAY = 0.2


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if "integration" in item.nodeid:
            item.add_marker(pytest.mark.integration)


@pytest.fixture(scope="session")
def postgres() -> Iterator[PostgresContainer]:
    with PostgresContainer("postgres:17-alpine", driver="asyncpg") as container:
        yield container


@pytest.fixture(scope="session")
def rabbitmq() -> Iterator[RabbitMqContainer]:
    with RabbitMqContainer("rabbitmq:4.1-management-alpine") as container:
        yield container


@pytest.fixture(scope="session")
def webhook_receiver() -> Iterator[str]:
    """Базовый URL получателя webhook, доступный с хоста."""
    path = ROOT / "tests" / "integration" / "webhook_receiver"
    with (
        DockerImage(path=str(path), tag="payments-webhook-receiver:test") as image,
        DockerContainer(str(image))
        .with_env("WEBHOOK_SECRET", WEBHOOK_SECRET)
        .with_exposed_ports(8080)
        .waiting_for(HttpWaitStrategy(8080, "/health")) as container,
    ):
        yield f"http://{container.get_container_host_ip()}:{container.get_exposed_port(8080)}"


@pytest.fixture(scope="session")
def database_url(postgres: PostgresContainer) -> str:
    url: str = postgres.get_connection_url()
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "migrations"))
    config.set_main_option("sqlalchemy.url", url)
    command.upgrade(config, "head")
    return url


@pytest.fixture(scope="session")
def rabbitmq_url(rabbitmq: RabbitMqContainer) -> str:
    host = rabbitmq.get_container_host_ip()
    port = rabbitmq.get_exposed_port(rabbitmq.port)
    return f"amqp://{rabbitmq.username}:{rabbitmq.password}@{host}:{port}/"


@pytest.fixture
def settings(database_url: str, rabbitmq_url: str) -> Settings:
    return Settings(
        database_url=database_url,
        rabbitmq_url=rabbitmq_url,
        api_key=API_KEY,
        webhook_secret=WEBHOOK_SECRET,
        outbox_poll_interval=0.1,
        retry_base_delay=RETRY_BASE_DELAY,
        gateway_min_delay=0,
        gateway_max_delay=0,
        gateway_success_rate=1.0,
        gateway_decline_rate=0.0,
        webhook_timeout=5,
    )


@pytest.fixture(autouse=True)
async def clean_state(database_url: str, rabbitmq_url: str) -> AsyncIterator[None]:
    """Каждый тест начинает с пустых таблиц и очередей."""
    yield
    engine = create_async_engine(database_url)
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE payments, outbox"))
    await engine.dispose()

    delays = RetryPolicy(max_attempts=3, base_delay=RETRY_BASE_DELAY).delays_ms
    queues = [NEW_PAYMENTS_QUEUE_NAME, DEAD_LETTER_QUEUE_NAME, *map(retry_queue_name, delays)]
    connection = await aio_pika.connect(rabbitmq_url)
    async with connection:
        for name in queues:
            channel = await connection.channel()
            try:
                queue = await channel.get_queue(name, ensure=True)
                await queue.purge()
            except aio_pika.exceptions.ChannelClosed:
                pass  # очередь ещё не объявлена


@pytest.fixture
async def api(settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    app = create_api(settings)
    async with (
        LifespanManager(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://api",
            headers={"X-API-Key": API_KEY},
        ) as client,
    ):
        yield client


type StartConsumer = Callable[..., Awaitable[FastStream]]


@pytest.fixture
async def start_consumer(settings: Settings) -> AsyncIterator[StartConsumer]:
    """Запускает consumer; именованные аргументы переопределяют настройки."""
    started: list[FastStream] = []

    async def start(**overrides: Any) -> FastStream:
        app = create_consumer(settings.model_copy(update=overrides))
        await app.start()
        started.append(app)
        return app

    yield start
    for app in started:
        await app.stop()


@pytest.fixture
async def receiver(webhook_receiver: str) -> AsyncIterator[WebhookReceiver]:
    async with httpx.AsyncClient(base_url=webhook_receiver) as client:
        yield WebhookReceiver(webhook_receiver, client)
