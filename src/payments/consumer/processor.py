from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol
from uuid import UUID

from loguru import logger

from payments.consumer.gateway import PaymentGateway
from payments.db.models import Payment
from payments.db.session import SessionFactory
from payments.db.uow import UnitOfWork
from payments.domain import PaymentStatus
from payments.messaging.retry import RetryPolicy


class Stage(StrEnum):
    GATEWAY = "gateway"
    WEBHOOK = "webhook"


@dataclass(frozen=True, slots=True)
class Ack:
    """Сообщение обработано полностью."""


@dataclass(frozen=True, slots=True)
class Retry:
    stage: Stage
    attempt: int  # номер следующей попытки
    delay_ms: int


@dataclass(frozen=True, slots=True)
class DeadLetter:
    stage: Stage
    attempt: int
    reason: str


type Outcome = Ack | Retry | DeadLetter


class Notifier(Protocol):
    async def notify(self, payment: Payment) -> None: ...


class PaymentStore(Protocol):
    async def get(self, payment_id: UUID) -> Payment | None: ...

    async def finalize(self, payment_id: UUID, status: PaymentStatus) -> Payment:
        """Переводит pending-платёж в финальный статус и возвращает актуальное состояние."""
        ...


class SqlPaymentStore:
    def __init__(self, session_factory: SessionFactory) -> None:
        self._session_factory = session_factory

    async def get(self, payment_id: UUID) -> Payment | None:
        async with UnitOfWork(self._session_factory) as uow:
            return await uow.payments.get(payment_id)

    async def finalize(self, payment_id: UUID, status: PaymentStatus) -> Payment:
        async with UnitOfWork(self._session_factory) as uow:
            updated = await uow.payments.finalize(payment_id, status, datetime.now(UTC))
            await uow.commit()
            if not updated:
                logger.info("Payment was already finalized concurrently")
            payment = await uow.payments.get(payment_id)
        if payment is None:
            raise RuntimeError(f"Payment {payment_id} disappeared")
        return payment


class PaymentProcessor:
    """Обработка события payment.created: шлюз → финальный статус в БД → webhook.

    Обработчик идемпотентен: текущий этап определяется по статусу платежа в БД,
    поэтому повторная доставка сообщения не проводит платёж повторно.
    У каждого этапа свой счётчик попыток.
    """

    def __init__(
        self,
        store: PaymentStore,
        gateway: PaymentGateway,
        notifier: Notifier,
        retry_policy: RetryPolicy,
    ) -> None:
        self._store = store
        self._gateway = gateway
        self._notifier = notifier
        self._retry_policy = retry_policy

    async def process(self, payment_id: UUID, stage: Stage | None, attempt: int) -> Outcome:
        """stage и attempt — этап и номер попытки из заголовков сообщения (None для первой)."""
        with logger.contextualize(payment_id=str(payment_id)):
            return await self._process(payment_id, stage, attempt)

    async def _process(self, payment_id: UUID, stage: Stage | None, attempt: int) -> Outcome:
        current = stage or Stage.GATEWAY
        try:
            payment = await self._store.get(payment_id)
            if payment is None:
                logger.error("Payment not found")
                return DeadLetter(current, attempt, f"Payment {payment_id} not found")

            if payment.status is PaymentStatus.PENDING:
                current = Stage.GATEWAY
                payment = await self._charge(payment)

            current = Stage.WEBHOOK
            await self._notifier.notify(payment)
        except Exception as exc:
            # Счётчик попыток сбрасывается, если упал не тот этап, что в заголовках.
            stage_attempt = attempt if current == stage else 1
            return self._on_failure(current, stage_attempt, exc)

        logger.success("Payment processed, webhook delivered")
        return Ack()

    async def _charge(self, payment: Payment) -> Payment:
        result = await self._gateway.charge(payment.id)
        finalized = await self._store.finalize(payment.id, result)
        logger.info("Payment finalized as {}", finalized.status)
        return finalized

    def _on_failure(self, stage: Stage, attempt: int, exc: Exception) -> Retry | DeadLetter:
        delay_ms = self._retry_policy.delay_after(attempt)
        attempts = f"{attempt}/{self._retry_policy.max_attempts}"
        with logger.contextualize(stage=stage.value, attempt=attempts):
            if delay_ms is None:
                logger.error("Attempts exhausted, sending to DLQ: {!r}", exc)
                return DeadLetter(stage, attempt, repr(exc))
            logger.warning("Attempt failed, retry in {} ms: {!r}", delay_ms, exc)
            return Retry(stage, attempt + 1, delay_ms)
