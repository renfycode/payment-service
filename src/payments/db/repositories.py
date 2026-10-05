from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from payments.db.models import OutboxMessage, Payment
from payments.domain import PaymentStatus


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, payment_id: UUID) -> Payment | None:
        payment: Payment | None = await self._session.get(
            Payment, payment_id, populate_existing=True
        )
        return payment

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        stmt = select(Payment).where(Payment.idempotency_key == idempotency_key)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    def add(self, payment: Payment) -> None:
        self._session.add(payment)

    async def finalize(
        self, payment_id: UUID, status: PaymentStatus, processed_at: datetime
    ) -> bool:
        """Переводит платёж из pending в финальный статус.

        Возвращает False, если платёж уже был финализирован конкурентным обработчиком.
        """
        stmt = (
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(status=status, processed_at=processed_at)
            .returning(Payment.id)
        )
        return await self._session.scalar(stmt) is not None


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, message_id: UUID, event_type: str, payload: dict[str, Any]) -> None:
        self._session.add(OutboxMessage(id=message_id, event_type=event_type, payload=payload))

    async def lock_unpublished(self, limit: int) -> Sequence[OutboxMessage]:
        """Блокирует пачку неопубликованных сообщений.

        SKIP LOCKED позволяет нескольким экземплярам relay работать параллельно
        без двойной публикации одной и той же записи.
        """
        stmt = (
            select(OutboxMessage)
            .where(OutboxMessage.published_at.is_(None))
            .order_by(OutboxMessage.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return (await self._session.scalars(stmt)).all()

    async def count_published_before(self, cutoff: datetime) -> int:
        stmt = select(func.count()).where(OutboxMessage.published_at < cutoff)
        return (await self._session.execute(stmt)).scalar_one()

    async def delete_published_before(self, cutoff: datetime, limit: int) -> int:
        """Удаляет до limit опубликованных событий старше cutoff. Неопубликованные
        не трогаются никогда: это события, ещё не доставленные в брокер."""
        batch = (
            select(OutboxMessage.id)
            .where(OutboxMessage.published_at < cutoff)
            .order_by(OutboxMessage.published_at)
            .limit(limit)
        )
        stmt = delete(OutboxMessage).where(OutboxMessage.id.in_(batch))
        result = await self._session.execute(stmt)
        return int(result.rowcount)  # type: ignore[attr-defined]
