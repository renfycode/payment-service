import contextlib
from collections.abc import AsyncIterator

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from payments.api.routes import router
from payments.config import Settings
from payments.containers import ApiContainer, init_resources, shutdown_resources
from payments.domain import IdempotencyConflictError, PaymentNotFoundError
from payments.logging_config import configure_logging


def create_app(container: ApiContainer | None = None) -> FastAPI:
    if container is None:
        # Обязательные секреты приходят из окружения — pyright этого не видит.
        container = ApiContainer(settings=Settings())  # pyright: ignore[reportCallIssue]
    settings = container.settings()
    configure_logging(settings.logging.level, json=settings.logging.format == "json")
    container.wire()

    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        # Подключения к БД и брокеру, outbox relay — ресурсы контейнера.
        await init_resources(container)
        try:
            yield
        finally:
            # Relay публикует через брокер — останавливаем его до закрытия брокера и БД.
            await shutdown_resources(container, first=[container.outbox_relay_task])

    app = FastAPI(title="Payments Service", version="0.1.0", lifespan=lifespan)
    app.state.container = container
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
