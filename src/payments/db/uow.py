from types import TracebackType
from typing import Self

from sqlalchemy.ext.asyncio import AsyncSession

from payments.db.repositories import OutboxRepository, PaymentRepository
from payments.db.session import SessionFactory


class UnitOfWork:
    """Одна транзакция БД и репозитории поверх неё.

    Без явного commit() изменения откатываются при выходе из контекста:
    close() завершает транзакцию откатом, но, в отличие от rollback(), не помечает
    загруженные объекты устаревшими — их можно читать после выхода.
    """

    session: AsyncSession
    payments: PaymentRepository
    outbox: OutboxRepository

    def __init__(self, session_factory: SessionFactory) -> None:
        self._session_factory = session_factory

    async def __aenter__(self) -> Self:
        self.session = self._session_factory()
        self.payments = PaymentRepository(self.session)
        self.outbox = OutboxRepository(self.session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.session.close()

    async def commit(self) -> None:
        await self.session.commit()

    async def rollback(self) -> None:
        await self.session.rollback()
