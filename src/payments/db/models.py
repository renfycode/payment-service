from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    Index,
    MetaData,
    Numeric,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from payments.domain import Currency, PaymentStatus

NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


def _enum_values(enum_cls: type[Currency | PaymentStatus]) -> list[str]:
    return [member.value for member in enum_cls]


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (CheckConstraint("amount > 0", name="amount_positive"),)

    id: Mapped[UUID] = mapped_column(primary_key=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[Currency] = mapped_column(
        Enum(Currency, name="currency", values_callable=_enum_values)
    )
    description: Mapped[str] = mapped_column(String(500))
    # Атрибут "metadata" зарезервирован в Declarative API, поэтому имя атрибута отличается.
    metadata_: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB)
    status: Mapped[PaymentStatus] = mapped_column(
        Enum(PaymentStatus, name="payment_status", values_callable=_enum_values),
        default=PaymentStatus.PENDING,
    )
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True)
    # SHA-256 нормализованного тела запроса: отличает повтор от конфликта ключа.
    request_hash: Mapped[str] = mapped_column(String(64))
    webhook_url: Mapped[str] = mapped_column(String(2048))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OutboxMessage(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        Index(
            "ix_outbox_unpublished",
            "created_at",
            postgresql_where=text("published_at IS NULL"),
        ),
        # Для очистки опубликованных событий (payments outbox cleanup).
        Index(
            "ix_outbox_published_at",
            "published_at",
            postgresql_where=text("published_at IS NOT NULL"),
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    event_type: Mapped[str] = mapped_column(String(100))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempts: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    last_error: Mapped[str | None] = mapped_column(Text)
