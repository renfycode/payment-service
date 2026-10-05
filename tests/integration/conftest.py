"""Интеграционное окружение: настоящие Postgres, RabbitMQ и получатель webhook в Docker.

API и consumer запускаются в процессе тестов: так видны их логи и легко
подменять настройки (поведение шлюза, задержки повторов) под конкретный сценарий.
"""

from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from pathlib import Path

import aio_pika
import httpx
import pytest
from alembic import command
from asgi_lifespan import LifespanManager
from dependency_injector import providers
from faststream.asgi import AsgiFastStream
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.rabbitmq import RabbitMqContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.image import DockerImage
from testcontainers.core.wait_strategies import HttpWaitStrategy

from payments.api.app import create_app as create_api
from payments.config import (
    ApiSettings,
    DatabaseSettings,
    OutboxSettings,
    RabbitMQSettings,
    RetrySettings,
    Settings,
    WebhookSettings,
)
from payments.consumer.app import create_app as create_consumer
from payments.consumer.gateway import PaymentGateway
from payments.containers import ApiContainer, ConsumerContainer
from payments.db.migrate import alembic_config
from payments.domain import PaymentStatus
from payments.messaging.retry import RetryPolicy
from payments.messaging.topology import (
    DEAD_LETTER_QUEUE_NAME,
    NEW_PAYMENTS_QUEUE_NAME,
    retry_queue_name,
)
from tests.integration.helpers import StaticGateway, WebhookReceiver

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
    # 15672 — management API: тесты проверяют, что реально видит брокер (prefetch и т. п.).
    with RabbitMqContainer("rabbitmq:4.1-management-alpine").with_exposed_ports(15672) as container:
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
def database_settings(postgres: PostgresContainer) -> DatabaseSettings:
    return DatabaseSettings(
        host=postgres.get_container_host_ip(),
        port=int(postgres.get_exposed_port(postgres.port)),
        name=postgres.dbname,
        user=postgres.username,
        password=SecretStr(postgres.password),
    )


@pytest.fixture(scope="session")
def database_url(database_settings: DatabaseSettings) -> str:
    url = database_settings.url
    command.upgrade(alembic_config(url), "head")
    return url


@pytest.fixture(scope="session")
def rabbitmq_settings(rabbitmq: RabbitMqContainer) -> RabbitMQSettings:
    return RabbitMQSettings(
        host=rabbitmq.get_container_host_ip(),
        port=int(rabbitmq.get_exposed_port(rabbitmq.port)),
        vhost=rabbitmq.vhost,
        user=rabbitmq.username,
        password=SecretStr(rabbitmq.password),
    )


@pytest.fixture(scope="session")
def rabbitmq_management_url(rabbitmq: RabbitMqContainer) -> str:
    return f"http://{rabbitmq.get_container_host_ip()}:{rabbitmq.get_exposed_port(15672)}"


@pytest.fixture(scope="session")
def rabbitmq_url(rabbitmq_settings: RabbitMQSettings) -> str:
    return rabbitmq_settings.url


@pytest.fixture
def settings(
    database_url: str,  # гарантирует применённые миграции
    database_settings: DatabaseSettings,
    rabbitmq_settings: RabbitMQSettings,
) -> Settings:
    return Settings(
        api=ApiSettings(key=SecretStr(API_KEY)),
        database=database_settings,
        rabbitmq=rabbitmq_settings,
        webhook=WebhookSettings(secret=SecretStr(WEBHOOK_SECRET), timeout=5),
        outbox=OutboxSettings(poll_interval=0.1),
        retry=RetrySettings(base_delay=RETRY_BASE_DELAY),
    )


@pytest.fixture
def cli_env(settings: Settings) -> dict[str, str]:
    """Переменные окружения, задающие ту же конфигурацию для CLI (без TOML-файла)."""
    db, mq = settings.database, settings.rabbitmq
    return {
        "PAYMENTS_CONFIG": "",
        "PAYMENTS__API__KEY": settings.api.key.get_secret_value(),
        "PAYMENTS__WEBHOOK__SECRET": settings.webhook.secret.get_secret_value(),
        "PAYMENTS__DATABASE__HOST": db.host,
        "PAYMENTS__DATABASE__PORT": str(db.port),
        "PAYMENTS__DATABASE__NAME": db.name,
        "PAYMENTS__DATABASE__USER": db.user,
        "PAYMENTS__DATABASE__PASSWORD": db.password.get_secret_value(),
        "PAYMENTS__RABBITMQ__HOST": mq.host,
        "PAYMENTS__RABBITMQ__PORT": str(mq.port),
        "PAYMENTS__RABBITMQ__USER": mq.user,
        "PAYMENTS__RABBITMQ__PASSWORD": mq.password.get_secret_value(),
    }


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
    app = create_api(ApiContainer(settings=settings))
    async with (
        LifespanManager(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://api",
            headers={"X-API-Key": API_KEY},
        ) as client,
    ):
        yield client


type StartConsumer = Callable[..., Awaitable[AsgiFastStream]]

SUCCESSFUL_GATEWAY = StaticGateway(PaymentStatus.SUCCEEDED)


@pytest.fixture
async def start_consumer(settings: Settings) -> AsyncIterator[StartConsumer]:
    """Запускает consumer. Шлюз подменяется детерминированной заглушкой через override
    провайдера; по умолчанию все платежи проходят успешно и без задержки."""
    started: list[AsgiFastStream] = []

    async def start(*, gateway: PaymentGateway = SUCCESSFUL_GATEWAY) -> AsgiFastStream:
        container = ConsumerContainer(settings=settings)
        container.gateway.override(providers.Object(gateway))
        app = create_consumer(container)
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
