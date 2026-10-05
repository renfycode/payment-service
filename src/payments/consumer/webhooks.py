"""Отправка webhook по спецификации Standard Webhooks (https://www.standardwebhooks.com)."""

import base64
import hashlib
import hmac
import time

import httpx

from payments.db.models import Payment
from payments.domain import PaymentStatus
from payments.schemas import PaymentRead, WebhookEvent

SECRET_PREFIX = "whsec_"


class WebhookDeliveryError(Exception):
    """Получатель недоступен или ответил не 2xx."""


class WebhookSigner:
    def __init__(self, secret: str) -> None:
        self._key = base64.b64decode(secret.removeprefix(SECRET_PREFIX))

    def sign(self, message_id: str, timestamp: int, body: bytes) -> str:
        signed_content = f"{message_id}.{timestamp}.".encode() + body
        digest = hmac.new(self._key, signed_content, hashlib.sha256).digest()
        return f"v1,{base64.b64encode(digest).decode()}"


def build_event(payment: Payment) -> WebhookEvent:
    if payment.status is PaymentStatus.SUCCEEDED:
        event_type = "payment.succeeded"
    elif payment.status is PaymentStatus.FAILED:
        event_type = "payment.failed"
    else:
        raise ValueError(f"Payment {payment.id} is not finalized")
    if payment.processed_at is None:
        raise ValueError(f"Payment {payment.id} has no processed_at")
    return WebhookEvent(
        type=event_type,
        timestamp=payment.processed_at,
        data=PaymentRead.model_validate(payment),
    )


def webhook_message_id(payment: Payment) -> str:
    # Стабилен между повторами: получатель дедуплицирует доставки по webhook-id.
    return f"msg_{payment.id.hex}"


class WebhookNotifier:
    def __init__(self, client: httpx.AsyncClient, signer: WebhookSigner) -> None:
        self._client = client
        self._signer = signer

    async def notify(self, payment: Payment) -> None:
        body = build_event(payment).model_dump_json().encode()
        message_id = webhook_message_id(payment)
        timestamp = int(time.time())
        headers = {
            "content-type": "application/json",
            "webhook-id": message_id,
            "webhook-timestamp": str(timestamp),
            "webhook-signature": self._signer.sign(message_id, timestamp, body),
        }
        try:
            response = await self._client.post(payment.webhook_url, content=body, headers=headers)
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(f"Webhook request failed: {exc!r}") from exc
        if not response.is_success:
            raise WebhookDeliveryError(f"Webhook endpoint responded {response.status_code}")
