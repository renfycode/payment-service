import asyncio
import contextlib
from collections.abc import AsyncIterator

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from faststream.rabbit import RabbitBroker

from payments.api.routes import router
from payments.config import Settings
from payments.db.session import create_engine, create_session_factory
from payments.domain import IdempotencyConflictError, PaymentNotFoundError
from payments.logging_config import configure_logging
from payments.messaging.outbox_relay import OutboxRelay
from payments.messaging.retry import RetryPolicy
from payments.messaging.topology import declare_topology


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    configure_logging(settings.log_level, json=settings.log_json)

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_engine(settings.database_url)
        session_factory = create_session_factory(engine)
        broker = RabbitBroker(settings.rabbitmq_url, logger=None)
        await broker.connect()
        retry_policy = RetryPolicy(settings.max_attempts, settings.retry_base_delay)
        await declare_topology(broker, retry_policy.delays_ms)

        relay = OutboxRelay(
            session_factory,
            broker,
            batch_size=settings.outbox_batch_size,
            poll_interval=settings.outbox_poll_interval,
        )
        relay_task = asyncio.create_task(relay.run(), name="outbox-relay")

        app.state.settings = settings
        app.state.session_factory = session_factory
        try:
            yield
        finally:
            relay_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await relay_task
            await broker.stop()
            await engine.dispose()

    app = FastAPI(title="Payments Service", version="0.1.0", lifespan=lifespan)
    app.include_router(router)

    @app.get("/health", include_in_schema=False)
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.exception_handler(PaymentNotFoundError)
    async def not_found_handler(_: Request, exc: PaymentNotFoundError) -> JSONResponse:
        return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"detail": str(exc)})

    @app.exception_handler(IdempotencyConflictError)
    async def conflict_handler(_: Request, exc: IdempotencyConflictError) -> JSONResponse:
        return JSONResponse(status_code=status.HTTP_409_CONFLICT, content={"detail": str(exc)})

    return app
