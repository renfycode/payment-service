import asyncio

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from payments.config import MigrationSettings
from payments.db.models import Base
from payments.logging_config import configure_logging

config = context.config


target_metadata = Base.metadata


def get_database_url() -> str:
    # Явно переданный URL (из тестов) важнее конфигурации сервиса.
    if url := config.get_main_option("sqlalchemy.url"):
        return url
    settings = MigrationSettings()  # pyright: ignore[reportCallIssue]
    configure_logging(settings.logging.level, json=settings.logging.format == "json")
    return settings.database.url


def run_migrations_offline() -> None:
    context.configure(
        url=get_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    engine = create_async_engine(get_database_url(), poolclass=pool.NullPool)

    async with engine.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await engine.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
