import asyncio
from collections.abc import AsyncIterator

from asgi_lifespan import LifespanManager
from dependency_injector import providers
from pydantic import SecretStr

from payments.api.app import create_app
from payments.config import (
    ApiSettings,
    DatabaseSettings,
    RabbitMQSettings,
    Settings,
    WebhookSettings,
)
from payments.containers import ApiContainer

SETTINGS = Settings(
    api=ApiSettings(key=SecretStr("key")),
    database=DatabaseSettings(password=SecretStr("pw")),
    rabbitmq=RabbitMQSettings(password=SecretStr("pw")),
    webhook=WebhookSettings(secret=SecretStr("whsec_c2VjcmV0")),
)


async def test_outbox_relay_stops_before_broker_and_database() -> None:
    events: list[str] = []

    async def fake_resource(name: str) -> AsyncIterator[str]:
        events.append(f"open {name}")
        yield name
        events.append(f"close {name}")

    class FakeRelay:
        async def run(self) -> None:
            try:
                await asyncio.Event().wait()
            finally:
                events.append("relay stopped")

    container = ApiContainer(settings=SETTINGS)
    container.core.engine.override(providers.Resource(fake_resource, "engine"))
    container.broker.override(providers.Resource(fake_resource, "broker"))
    container.outbox_relay.override(providers.Singleton(FakeRelay))

    async with LifespanManager(create_app(container)):
        await asyncio.sleep(0)  # даём задаче relay стартовать

    assert events.index("relay stopped") < events.index("close broker")
    assert events.index("relay stopped") < events.index("close engine")
