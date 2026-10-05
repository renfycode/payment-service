"""DI-контейнеры (dependency-injector) — единственное место, где собираются зависимости.

CoreContainer  — общее для обоих процессов: настройки, БД, политика повторов.
ApiContainer   — процесс api: брокер для публикации, outbox relay, сервисы.
ConsumerContainer — процесс consumer: брокер с подписчиком, шлюз, webhook, processor.

Ресурсы (подключения, клиенты, фоновые задачи) — providers.Resource: их открывает
init_resources() и закрывает shutdown_resources() в lifespan соответствующего процесса.
"""

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Sequence
from typing import Any

import httpx
from dependency_injector import containers, providers
from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncEngine

from payments.config import Settings
from payments.consumer.gateway import EmulatedPaymentGateway
from payments.consumer.processor import PaymentProcessor, SqlPaymentStore
from payments.consumer.webhooks import WebhookNotifier, WebhookSigner
from payments.db.session import create_engine, create_session_factory
from payments.messaging.outbox_relay import OutboxRelay
from payments.messaging.retry import RetryPolicy
from payments.messaging.topology import declare_topology
from payments.services import PaymentService


async def database_engine(database_url: str) -> AsyncIterator[AsyncEngine]:
    engine = create_engine(database_url)
    yield engine
    await engine.dispose()


async def connected_broker(
    rabbitmq_url: str, retry_delays_ms: tuple[int, ...]
) -> AsyncIterator[RabbitBroker]:
    """Брокер api: только публикует, поэтому логгер FastStream не нужен."""
    broker = RabbitBroker(rabbitmq_url, logger=None)
    await broker.connect()
    await declare_topology(broker, retry_delays_ms)
    yield broker
    await broker.stop()


async def running_outbox_relay(relay: OutboxRelay) -> AsyncIterator[asyncio.Task[None]]:
    task = asyncio.create_task(relay.run(), name="outbox-relay")
    yield task
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def webhook_http_client(request_timeout: float) -> AsyncIterator[httpx.AsyncClient]:
    # Редиректы не выполняем: по Standard Webhooks 3xx — неуспешная доставка.
    async with httpx.AsyncClient(timeout=request_timeout, follow_redirects=False) as client:
        yield client


async def init_resources(container: containers.Container) -> None:
    # Для контейнера с async-ресурсами init_resources() возвращает awaitable.
    if (pending := container.init_resources()) is not None:
        await pending


async def shutdown_resources(
    container: containers.Container, *, first: Sequence[providers.Resource[Any]] = ()
) -> None:
    """Закрывает ресурсы контейнера.

    dependency-injector не гарантирует обратный порядок зависимостей при закрытии,
    поэтому ресурсы, которые пользуются остальными (например, фоновые задачи),
    передаются в first и закрываются раньше.
    """
    for resource in first:
        if (pending := resource.shutdown()) is not None:
            await pending
    if (pending := container.shutdown_resources()) is not None:
        await pending


class CoreContainer(containers.DeclarativeContainer):
    settings = providers.Dependency(instance_of=Settings)

    engine = providers.Resource(database_engine, database_url=settings.provided.database.url)
    session_factory = providers.Singleton(create_session_factory, engine)
    retry_policy = providers.Singleton(
        RetryPolicy,
        max_attempts=settings.provided.retry.max_attempts,
        base_delay=settings.provided.retry.base_delay,
    )


class ApiContainer(containers.DeclarativeContainer):
    wiring_config = containers.WiringConfiguration(
        modules=["payments.api.routes", "payments.api.dependencies"]
    )

    settings = providers.Dependency(instance_of=Settings)
    core = providers.Container(CoreContainer, settings=settings)

    broker = providers.Resource(
        connected_broker,
        rabbitmq_url=settings.provided.rabbitmq.url,
        retry_delays_ms=core.retry_policy.provided.delays_ms,
    )
    outbox_relay = providers.Singleton(
        OutboxRelay,
        session_factory=core.session_factory,
        broker=broker,
        batch_size=settings.provided.outbox.batch_size,
        poll_interval=settings.provided.outbox.poll_interval,
    )
    outbox_relay_task = providers.Resource(running_outbox_relay, relay=outbox_relay)

    payment_service = providers.Factory(PaymentService, session_factory=core.session_factory)


class ConsumerContainer(containers.DeclarativeContainer):
    settings = providers.Dependency(instance_of=Settings)
    core = providers.Container(CoreContainer, settings=settings)

    # Жизненным циклом брокера consumer управляет FastStream (start/stop приложения).
    # Записи FastStream идут в стандартный logging и оформляются loguru.
    broker = providers.Singleton(
        RabbitBroker,
        settings.provided.rabbitmq.url,
        logger=providers.Object(logging.getLogger("faststream.rabbit")),
    )

    gateway = providers.Singleton(
        EmulatedPaymentGateway,
        min_delay=settings.provided.gateway.min_delay,
        max_delay=settings.provided.gateway.max_delay,
        success_rate=settings.provided.gateway.success_rate,
        decline_rate=settings.provided.gateway.decline_rate,
    )
    http_client = providers.Resource(
        webhook_http_client, request_timeout=settings.provided.webhook.timeout
    )
    webhook_signer = providers.Singleton(
        WebhookSigner, secret=settings.provided.webhook.secret.get_secret_value.call()
    )
    notifier = providers.Singleton(WebhookNotifier, client=http_client, signer=webhook_signer)
    store = providers.Singleton(SqlPaymentStore, session_factory=core.session_factory)
    processor = providers.Singleton(
        PaymentProcessor,
        store=store,
        gateway=gateway,
        notifier=notifier,
        retry_policy=core.retry_policy,
    )
