"""Очистка таблицы outbox от давно опубликованных событий.

Возраст события определяется по его id: это UUIDv7, в старших 48 битах которого
записано время создания в миллисекундах. Поэтому «создано раньше cutoff» — это
id < наименьшего UUIDv7 с временем cutoff, то есть диапазон по первичному ключу.
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

from payments.db.session import SessionFactory
from payments.db.uow import UnitOfWork

# Удаляем пачками в отдельных транзакциях: не держим долгих блокировок
# и не раздуваем WAL одной огромной транзакцией.
DEFAULT_BATCH_SIZE = 10_000

_UUID7_VERSION = 0x7 << 76
_RFC4122_VARIANT = 0b10 << 62


def cutoff_for(older_than: timedelta, *, now: datetime | None = None) -> datetime:
    return (now or datetime.now(UTC)) - older_than


def uuid7_lower_bound(moment: datetime) -> UUID:
    """Наименьший UUIDv7 с временем moment: все UUIDv7, созданные раньше, меньше него."""
    timestamp_ms = int(moment.timestamp() * 1000)
    return UUID(int=(timestamp_ms << 80) | _UUID7_VERSION | _RFC4122_VARIANT)


async def count_published_before(session_factory: SessionFactory, cutoff: datetime) -> int:
    async with UnitOfWork(session_factory) as uow:
        return await uow.outbox.count_published_before(uuid7_lower_bound(cutoff))


async def delete_published_before(
    session_factory: SessionFactory, cutoff: datetime, *, batch_size: int = DEFAULT_BATCH_SIZE
) -> int:
    """Удаляет опубликованные события, созданные раньше cutoff, и возвращает их число."""
    boundary = uuid7_lower_bound(cutoff)
    deleted = 0
    while True:
        async with UnitOfWork(session_factory) as uow:
            batch = await uow.outbox.delete_published_before(boundary, batch_size)
            await uow.commit()
        deleted += batch
        if batch < batch_size:
            return deleted
