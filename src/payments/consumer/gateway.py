import asyncio
import hashlib
import random
from typing import Literal, Protocol
from uuid import UUID

from payments.domain import PaymentStatus

type ChargeResult = Literal[PaymentStatus.SUCCEEDED, PaymentStatus.FAILED]


class GatewayUnavailableError(Exception):
    """Техническая ошибка шлюза: результат неизвестен, операцию нужно повторить."""


class PaymentGateway(Protocol):
    async def charge(self, payment_id: UUID) -> ChargeResult:
        """Проводит платёж. payment_id служит ключом идемпотентности на стороне шлюза.

        Возвращает succeeded или failed (бизнес-отказ),
        при технической ошибке бросает GatewayUnavailableError.
        """
        ...


def stable_fraction(payment_id: UUID) -> float:
    """Число в [0, 1), постоянное для payment_id: одинаковое между вызовами, рестартами
    и репликами consumer."""
    digest = hashlib.sha256(payment_id.bytes).digest()
    return int.from_bytes(digest[:8]) / 2**64


class EmulatedPaymentGateway:
    """Эмулятор шлюза.

    Как настоящий шлюз с ключом идемпотентности, на повторный запрос по тому же payment_id
    он отвечает тем же итогом (succeeded/failed): итог выводится из payment_id.
    Технический сбой, наоборот, случаен на каждый вызов — иначе повтор никогда бы
    не помог. Доли исходов при первом вызове: success_rate / decline_rate / остаток.
    """

    def __init__(
        self,
        *,
        min_delay: float,
        max_delay: float,
        success_rate: float,
        decline_rate: float,
        rng: random.Random | None = None,
    ) -> None:
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._success_rate = success_rate
        self._decline_rate = decline_rate
        self._rng = rng or random.Random()

    async def charge(self, payment_id: UUID) -> ChargeResult:
        await asyncio.sleep(self._rng.uniform(self._min_delay, self._max_delay))
        answered = self._success_rate + self._decline_rate
        if self._rng.random() >= answered:
            raise GatewayUnavailableError(f"Gateway is temporarily unavailable ({payment_id})")
        if stable_fraction(payment_id) < self._success_rate / answered:
            return PaymentStatus.SUCCEEDED
        return PaymentStatus.FAILED
