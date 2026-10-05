import random
from collections import Counter
from uuid import uuid4

import pytest

from payments.consumer.gateway import EmulatedPaymentGateway, GatewayUnavailableError
from payments.domain import PaymentStatus


async def test_outcome_distribution_follows_configured_rates() -> None:
    gateway = EmulatedPaymentGateway(
        min_delay=0,
        max_delay=0,
        success_rate=0.9,
        decline_rate=0.07,
        rng=random.Random(42),
    )
    outcomes: Counter[str] = Counter()
    for _ in range(10_000):
        try:
            outcomes[await gateway.charge(uuid4())] += 1
        except GatewayUnavailableError:
            outcomes["unavailable"] += 1

    assert outcomes[PaymentStatus.SUCCEEDED] / 10_000 == pytest.approx(0.90, abs=0.01)
    assert outcomes[PaymentStatus.FAILED] / 10_000 == pytest.approx(0.07, abs=0.01)
    assert outcomes["unavailable"] / 10_000 == pytest.approx(0.03, abs=0.01)


def make_gateway(success_rate: float, decline_rate: float) -> EmulatedPaymentGateway:
    return EmulatedPaymentGateway(
        min_delay=0,
        max_delay=0,
        success_rate=success_rate,
        decline_rate=decline_rate,
        rng=random.Random(7),
    )


async def test_outcome_is_stable_for_the_same_payment() -> None:
    gateway = make_gateway(success_rate=0.5, decline_rate=0.5)

    for _ in range(50):
        payment_id = uuid4()
        results = {await gateway.charge(payment_id) for _ in range(5)}
        assert len(results) == 1


async def test_outcome_survives_gateway_restart() -> None:
    payment_ids = [uuid4() for _ in range(50)]

    first = [await make_gateway(0.5, 0.5).charge(p) for p in payment_ids]
    second = [await make_gateway(0.5, 0.5).charge(p) for p in payment_ids]

    assert first == second


async def test_technical_failure_is_transient() -> None:
    gateway = make_gateway(success_rate=0.45, decline_rate=0.05)  # 50% сбоев на вызов
    payment_id = uuid4()

    outcomes: list[str] = []
    for _ in range(40):
        try:
            outcomes.append(await gateway.charge(payment_id))
        except GatewayUnavailableError:
            outcomes.append("unavailable")

    assert "unavailable" in outcomes
    assert len(set(outcomes) - {"unavailable"}) == 1  # успешные ответы — один и тот же итог
