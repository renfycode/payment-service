from enum import StrEnum
from uuid import UUID


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class DomainError(Exception):
    """Базовая ошибка предметной области."""


class PaymentNotFoundError(DomainError):
    def __init__(self, payment_id: UUID) -> None:
        super().__init__(f"Payment {payment_id} not found")
        self.payment_id: UUID = payment_id


class IdempotencyConflictError(DomainError):
    def __init__(self, idempotency_key: str) -> None:
        super().__init__(
            f"Idempotency-Key {idempotency_key!r} was already used with a different request body"
        )
        self.idempotency_key: str = idempotency_key
