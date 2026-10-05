from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import httpx
import pytest

from payments.consumer.webhooks import (
    WebhookDeliveryError,
    WebhookNotifier,
    WebhookSigner,
    build_event,
    webhook_message_id,
)
from payments.db.models import Payment
from payments.domain import Currency, PaymentStatus

SECRET = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"


def make_payment(status: PaymentStatus = PaymentStatus.SUCCEEDED) -> Payment:
    return Payment(
        id=UUID("0199b3a0-0000-7000-8000-000000000001"),
        amount=Decimal("10.00"),
        currency=Currency.USD,
        description="test",
        metadata_={"k": "v"},
        status=status,
        idempotency_key="key-1",
        request_hash="0" * 64,
        webhook_url="https://example.com/hook",
        created_at=datetime(2026, 10, 5, tzinfo=UTC),
        processed_at=None if status is PaymentStatus.PENDING else datetime(2026, 10, 5, tzinfo=UTC),
    )


def test_signature_matches_standard_webhooks_reference_vector() -> None:
    # Тестовый вектор из эталонных библиотек standard-webhooks.
    signature = WebhookSigner(SECRET).sign(
        "msg_p5jXN8AQM9LWM0D4loKWxJek", 1614265330, b'{"test": 2432232314}'
    )

    assert signature == "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="


@pytest.mark.parametrize(
    ("status", "event_type"),
    [(PaymentStatus.SUCCEEDED, "payment.succeeded"), (PaymentStatus.FAILED, "payment.failed")],
)
def test_event_type_follows_payment_status(status: PaymentStatus, event_type: str) -> None:
    event = build_event(make_payment(status))

    assert event.type == event_type
    assert event.data.metadata == {"k": "v"}


def test_pending_payment_has_no_webhook_event() -> None:
    with pytest.raises(ValueError, match="not finalized"):
        build_event(make_payment(PaymentStatus.PENDING))


async def test_notifier_sends_signed_request() -> None:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(204)

    payment = make_payment()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await WebhookNotifier(client, WebhookSigner(SECRET)).notify(payment)

    (request,) = captured
    message_id = request.headers["webhook-id"]
    timestamp = int(request.headers["webhook-timestamp"])
    assert message_id == webhook_message_id(payment)
    assert request.headers["webhook-signature"] == WebhookSigner(SECRET).sign(
        message_id, timestamp, request.content
    )


@pytest.mark.parametrize("status_code", [301, 400, 500, 503])
async def test_non_2xx_response_is_a_delivery_error(status_code: int) -> None:
    transport = httpx.MockTransport(lambda _: httpx.Response(status_code))
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(WebhookDeliveryError):
            await WebhookNotifier(client, WebhookSigner(SECRET)).notify(make_payment())


async def test_network_error_is_a_delivery_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(WebhookDeliveryError):
            await WebhookNotifier(client, WebhookSigner(SECRET)).notify(make_payment())
