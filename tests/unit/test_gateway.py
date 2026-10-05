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
