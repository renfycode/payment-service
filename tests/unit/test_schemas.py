from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from payments.schemas import PaymentCreate
from payments.services import request_fingerprint

VALID: dict[str, Any] = {
    "amount": "100.50",
    "currency": "RUB",
    "description": "Order #1",
    "metadata": {"order_id": 1},
    "webhook_url": "https://example.com/hook",
}


def test_amount_is_normalized_to_two_decimals() -> None:
    assert PaymentCreate.model_validate({**VALID, "amount": 100}).amount == Decimal("100.00")


def test_fingerprint_ignores_amount_notation_and_key_order() -> None:
    a = PaymentCreate.model_validate({**VALID, "amount": "100", "metadata": {"a": 1, "b": 2}})
    b = PaymentCreate.model_validate({**VALID, "amount": 100.0, "metadata": {"b": 2, "a": 1}})

    assert request_fingerprint(a) == request_fingerprint(b)


def test_fingerprint_differs_for_different_body() -> None:
    a = PaymentCreate.model_validate(VALID)
    b = PaymentCreate.model_validate({**VALID, "amount": "100.51"})

    assert request_fingerprint(a) != request_fingerprint(b)


@pytest.mark.parametrize(
    "override",
    [
        {"amount": 0},
        {"amount": "-1"},
        {"amount": "1.001"},
        {"currency": "GBP"},
        {"webhook_url": "ftp://example.com"},
        {"webhook_url": "not a url"},
        {"description": "x" * 501},
        {"unexpected": "field"},
    ],
)
def test_invalid_payment_is_rejected(override: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate({**VALID, **override})
