import hashlib
import json
from uuid import UUID

import uuid_utils.compat as uuid
from sqlalchemy.exc import IntegrityError

from payments.db.models import Payment
from payments.db.session import SessionFactory
from payments.db.uow import UnitOfWork
from payments.domain import IdempotencyConflictError, PaymentNotFoundError, PaymentStatus
from payments.messaging.topology import PAYMENT_CREATED_EVENT
from payments.schemas import PaymentCreate, PaymentCreatedEvent


def request_fingerprint(data: PaymentCreate) -> str:
    """SHA-256 канонического JSON тела запроса."""
    canonical = json.dumps(
        data.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


class PaymentService:
    def __init__(self, session_factory: SessionFactory) -> None:
        self._session_factory = session_factory

    async def create(self, data: PaymentCreate, idempotency_key: str) -> tuple[Payment, bool]:
        """Создаёт платёж и событие в outbox в одной транзакции.

        Возвращает (платёж, создан_ли_сейчас). Повтор запроса с тем же ключом и телом
        возвращает исходный платёж; тот же ключ с другим телом — IdempotencyConflictError.
        """
        fingerprint = request_fingerprint(data)

        async with UnitOfWork(self._session_factory) as uow:
            existing = await uow.payments.get_by_idempotency_key(idempotency_key)
            if existing is not None:
                return self._replay(existing, idempotency_key, fingerprint), False

            payment = Payment(
                id=uuid.uuid7(),
                amount=data.amount,
                currency=data.currency,
                description=data.description,
                metadata_=data.metadata,
                status=PaymentStatus.PENDING,
                idempotency_key=idempotency_key,
                request_hash=fingerprint,
                webhook_url=str(data.webhook_url),
            )
            uow.payments.add(payment)
            uow.outbox.add(
                message_id=uuid.uuid7(),
                event_type=PAYMENT_CREATED_EVENT,
                payload=PaymentCreatedEvent(payment_id=payment.id).model_dump(mode="json"),
            )
            try:
                await uow.commit()
            except IntegrityError:
                # Конкурентный запрос с тем же ключом успел закоммитить первым.
                await uow.rollback()
                existing = await uow.payments.get_by_idempotency_key(idempotency_key)
                if existing is None:
                    raise
                return self._replay(existing, idempotency_key, fingerprint), False

            await uow.session.refresh(payment)
            return payment, True

    async def get(self, payment_id: UUID) -> Payment:
        async with UnitOfWork(self._session_factory) as uow:
            payment = await uow.payments.get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return payment

    @staticmethod
    def _replay(existing: Payment, idempotency_key: str, fingerprint: str) -> Payment:
        if existing.request_hash != fingerprint:
            raise IdempotencyConflictError(idempotency_key)
        return existing
