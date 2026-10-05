from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from payments.consumer.app import parse_retry_headers
from payments.consumer.gateway import ChargeResult, GatewayUnavailableError
from payments.consumer.processor import Ack, DeadLetter, PaymentProcessor, Retry, Stage
from payments.consumer.webhooks import WebhookDeliveryError
from payments.db.models import Payment
from payments.domain import PaymentStatus
from payments.messaging.retry import RetryPolicy

POLICY = RetryPolicy(max_attempts=3, base_delay=2.0)


@dataclass
class FakeStore:
    payments: dict[UUID, Payment] = field(default_factory=dict)

    async def get(self, payment_id: UUID) -> Payment | None:
        return self.payments.get(payment_id)

    async def finalize(self, payment_id: UUID, status: PaymentStatus) -> Payment:
        payment = self.payments[payment_id]
        if payment.status is PaymentStatus.PENDING:
            payment.status = status
            payment.processed_at = datetime.now(UTC)
        return payment


@dataclass
class FakeGateway:
    results: list[ChargeResult | Exception]
    calls: int = 0

    async def charge(self, payment_id: UUID) -> ChargeResult:
        self.calls += 1
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


@dataclass
class FakeNotifier:
    failures: int = 0
    sent: list[UUID] = field(default_factory=list)

    async def notify(self, payment: Payment) -> None:
        if self.failures > 0:
            self.failures -= 1
            raise WebhookDeliveryError("endpoint responded 500")
        self.sent.append(payment.id)


def make_store(status: PaymentStatus = PaymentStatus.PENDING) -> tuple[FakeStore, UUID]:
    payment_id = uuid4()
    payment = Payment(id=payment_id, status=status, webhook_url="https://example.com")
    return FakeStore({payment_id: payment}), payment_id


async def test_happy_path_charges_and_notifies() -> None:
    store, payment_id = make_store()
    gateway = FakeGateway([PaymentStatus.SUCCEEDED])
    notifier = FakeNotifier()

    outcome = await PaymentProcessor(store, gateway, notifier, POLICY).process(payment_id, None, 1)

    assert outcome == Ack()
    assert store.payments[payment_id].status is PaymentStatus.SUCCEEDED
    assert notifier.sent == [payment_id]


async def test_business_decline_is_final_and_notified() -> None:
    store, payment_id = make_store()
    notifier = FakeNotifier()
    processor = PaymentProcessor(store, FakeGateway([PaymentStatus.FAILED]), notifier, POLICY)

    assert await processor.process(payment_id, None, 1) == Ack()
    assert store.payments[payment_id].status is PaymentStatus.FAILED
    assert notifier.sent == [payment_id]


async def test_gateway_outage_is_retried_with_exponential_delay() -> None:
    store, payment_id = make_store()
    gateway = FakeGateway([GatewayUnavailableError(), GatewayUnavailableError()])
    processor = PaymentProcessor(store, gateway, FakeNotifier(), POLICY)

    assert await processor.process(payment_id, None, 1) == Retry(Stage.GATEWAY, 2, 2000)
    assert await processor.process(payment_id, Stage.GATEWAY, 2) == Retry(Stage.GATEWAY, 3, 4000)


async def test_gateway_outage_after_last_attempt_leaves_payment_pending() -> None:
    store, payment_id = make_store()
    notifier = FakeNotifier()
    processor = PaymentProcessor(store, FakeGateway([GatewayUnavailableError()]), notifier, POLICY)

    outcome = await processor.process(payment_id, Stage.GATEWAY, 3)

    assert isinstance(outcome, DeadLetter)
    assert (outcome.stage, outcome.attempt) == (Stage.GATEWAY, 3)
    assert store.payments[payment_id].status is PaymentStatus.PENDING
    assert notifier.sent == []


async def test_webhook_stage_has_its_own_attempt_counter() -> None:
    store, payment_id = make_store()
    processor = PaymentProcessor(
        store, FakeGateway([PaymentStatus.SUCCEEDED]), FakeNotifier(failures=1), POLICY
    )

    # Шлюз ответил на последней попытке, а webhook упал: счётчик начинается заново.
    outcome = await processor.process(payment_id, Stage.GATEWAY, 3)

    assert outcome == Retry(Stage.WEBHOOK, 2, 2000)


async def test_redelivery_of_finalized_payment_skips_gateway() -> None:
    store, payment_id = make_store(PaymentStatus.SUCCEEDED)
    store.payments[payment_id].processed_at = datetime.now(UTC)
    gateway = FakeGateway([])
    notifier = FakeNotifier()

    outcome = await PaymentProcessor(store, gateway, notifier, POLICY).process(
        payment_id, Stage.WEBHOOK, 2
    )

    assert outcome == Ack()
    assert gateway.calls == 0
    assert notifier.sent == [payment_id]


async def test_webhook_failure_after_last_attempt_is_dead_lettered() -> None:
    store, payment_id = make_store(PaymentStatus.SUCCEEDED)
    processor = PaymentProcessor(store, FakeGateway([]), FakeNotifier(failures=1), POLICY)

    outcome = await processor.process(payment_id, Stage.WEBHOOK, 3)

    assert isinstance(outcome, DeadLetter)
    assert (outcome.stage, outcome.attempt) == (Stage.WEBHOOK, 3)
    assert "WebhookDeliveryError" in outcome.reason


async def test_unknown_payment_is_dead_lettered_immediately() -> None:
    processor = PaymentProcessor(FakeStore(), FakeGateway([]), FakeNotifier(), POLICY)

    outcome = await processor.process(uuid4(), None, 1)

    assert isinstance(outcome, DeadLetter)
    assert "not found" in outcome.reason


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ({}, (None, 1)),
        ({"x-stage": "gateway", "x-attempt": 2}, (Stage.GATEWAY, 2)),
        ({"x-stage": "webhook", "x-attempt": "3"}, (Stage.WEBHOOK, 3)),
        ({"x-stage": "bogus", "x-attempt": "nan"}, (None, 1)),
        ({"x-attempt": 0}, (None, 1)),
    ],
)
def test_parse_retry_headers(
    headers: dict[str, object], expected: tuple[Stage | None, int]
) -> None:
    assert parse_retry_headers(headers) == expected
