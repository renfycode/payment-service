"""Публичные контракты сервиса: тела HTTP-запросов/ответов, сообщения брокера, webhook."""

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, UrlConstraints, field_validator

from payments.domain import Currency, PaymentStatus

AMOUNT_QUANTUM = Decimal("0.01")

WebhookUrl = Annotated[
    AnyHttpUrl, UrlConstraints(max_length=2048, allowed_schemes=["http", "https"])
]


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]
    currency: Currency
    description: Annotated[str, Field(max_length=500)]
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: WebhookUrl

    @field_validator("amount")
    @classmethod
    def _normalize_amount(cls, value: Decimal) -> Decimal:
        # 100, 100.0 и 100.00 — одна и та же сумма: приводим к единому виду,
        # чтобы хэш тела запроса для идемпотентности не зависел от записи числа.
        return value.quantize(AMOUNT_QUANTUM)


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(validation_alias="metadata_")
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None


class PaymentCreatedEvent(BaseModel):
    """Сообщение в очереди payments.new. Источник правды о платеже — БД."""

    payment_id: UUID


class WebhookEvent(BaseModel):
    type: Literal["payment.succeeded", "payment.failed"]
    timestamp: datetime
    data: PaymentRead
