"""Очистка таблицы outbox от давно опубликованных событий."""

from datetime import UTC, datetime, timedelta

from payments.db.session import SessionFactory
from payments.db.uow import UnitOfWork

# Удаляем пачками в отдельных транзакциях: не держим долгих блокировок
# и не раздуваем WAL одной огромной транзакцией.
DEFAULT_BATCH_SIZE = 10_000


def cutoff_for(older_than: timedelta, *, now: datetime | None = None) -> datetime:
    return (now or datetime.now(UTC)) - older_than


async def count_published_before(session_factory: SessionFactory, cutoff: datetime) -> int:
    async with UnitOfWork(session_factory) as uow:
        return await uow.outbox.count_published_before(cutoff)


async def delete_published_before(
    session_factory: SessionFactory, cutoff: datetime, *, batch_size: int = DEFAULT_BATCH_SIZE
) -> int:
    """Удаляет опубликованные события старше cutoff и возвращает их число."""
    deleted = 0
    while True:
        async with UnitOfWork(session_factory) as uow:
            batch = await uow.outbox.delete_published_before(cutoff, batch_size)
            await uow.commit()
        deleted += batch
        if batch < batch_size:
            return deleted
