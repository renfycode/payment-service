import asyncio
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


class EmulatedPaymentGateway:
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
        roll = self._rng.random()
        if roll < self._success_rate:
            return PaymentStatus.SUCCEEDED
        if roll < self._success_rate + self._decline_rate:
            return PaymentStatus.FAILED
        raise GatewayUnavailableError(f"Gateway is temporarily unavailable ({payment_id})")
