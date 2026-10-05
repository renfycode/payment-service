"""Smoke-проверка запущенного стека (make local-smoke).

Проходит путь платежа целиком через публичные интерфейсы:
API → outbox → RabbitMQ → consumer → шлюз → БД → webhook с подписью.

Переменные окружения:
  SMOKE_API_URL           адрес API с хоста            (по умолчанию http://localhost:8000)
  SMOKE_RECEIVER_URL      адрес получателя с хоста     (по умолчанию http://localhost:8080)
  SMOKE_RECEIVER_INTERNAL адрес получателя из сети compose (http://webhook-receiver:8080)
  PAYMENTS_API_KEY        ключ API                     (по умолчанию dev-api-key)
"""

import os
import sys
import time
from collections.abc import Callable
from typing import Any
from uuid import uuid4

import httpx

API_URL = os.environ.get("SMOKE_API_URL", "http://localhost:8000")
RECEIVER_URL = os.environ.get("SMOKE_RECEIVER_URL", "http://localhost:8080")
RECEIVER_INTERNAL_URL = os.environ.get("SMOKE_RECEIVER_INTERNAL", "http://webhook-receiver:8080")
API_KEY = os.environ.get("PAYMENTS_API_KEY", "dev-api-key")
# Шлюз отвечает за 2–5 с; при технических сбоях добавляются повторы через 2 и 4 с.
TIMEOUT_SECONDS = 40.0

GREEN, RED, DIM, RESET = "\033[32m", "\033[31m", "\033[2m", "\033[0m"


class SmokeError(Exception):
    pass


def ok(message: str) -> None:
    # flush: шаги и ошибки (stderr) должны идти в порядке выполнения.
    print(f"{GREEN}✓{RESET} {message}", flush=True)


def wait_for[T](what: str, probe: Callable[[], T], done: Callable[[T], bool]) -> T:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    while True:
        value = probe()
        if done(value):
            return value
        if time.monotonic() > deadline:
            raise SmokeError(f"timed out after {TIMEOUT_SECONDS:.0f}s waiting for {what}")
        time.sleep(0.5)


def run() -> None:
    hook = f"smoke-{uuid4().hex[:8]}"
    body = {
        "amount": "100.00",
        "currency": "RUB",
        "description": "Smoke test",
        "metadata": {"source": "make local-smoke"},
        "webhook_url": f"{RECEIVER_INTERNAL_URL}/hooks/{hook}",
    }
    headers = {"X-API-Key": API_KEY, "Idempotency-Key": hook}

    with (
        httpx.Client(base_url=API_URL, timeout=10) as api,
        httpx.Client(base_url=RECEIVER_URL, timeout=10) as receiver,
    ):
        api.get("/health").raise_for_status()
        ok(f"API is up at {API_URL}")
        receiver.get("/health").raise_for_status()
        ok(f"Webhook receiver is up at {RECEIVER_URL}")

        if api.get(f"/api/v1/payments/{uuid4()}").status_code != 401:
            raise SmokeError("request without X-API-Key was not rejected with 401")
        ok("Requests without API key are rejected (401)")

        created = api.post("/api/v1/payments", json=body, headers=headers)
        if created.status_code != 202:
            raise SmokeError(f"create payment: {created.status_code} {created.text}")
        payment_id = created.json()["payment_id"]
        ok(f"Payment accepted (202): {payment_id}")

        replay = api.post("/api/v1/payments", json=body, headers=headers)
        if replay.status_code != 202 or replay.json()["payment_id"] != payment_id:
            raise SmokeError(f"idempotent replay returned {replay.status_code} {replay.text}")
        ok("Repeated request with the same Idempotency-Key returns the same payment")

        def payment() -> dict[str, Any]:
            response = api.get(f"/api/v1/payments/{payment_id}", headers={"X-API-Key": API_KEY})
            response.raise_for_status()
            result: dict[str, Any] = response.json()
            return result

        final = wait_for("final payment status", payment, lambda p: p["status"] != "pending")
        ok(f"Payment processed: status={final['status']} {DIM}(7% are declined by design){RESET}")

        def deliveries() -> list[dict[str, Any]]:
            response = receiver.get(f"/hooks/{hook}")
            response.raise_for_status()
            result: list[dict[str, Any]] = response.json()
            return result

        delivered = wait_for(
            "webhook delivery", deliveries, lambda d: any(x["status_code"] == 204 for x in d)
        )
        webhook = next(x for x in delivered if x["status_code"] == 204)
        if not webhook["signature_valid"]:
            raise SmokeError("webhook signature is invalid")
        expected_type = f"payment.{final['status']}"
        if webhook["body"]["type"] != expected_type:
            raise SmokeError(f"webhook type {webhook['body']['type']!r} != {expected_type!r}")
        ok(f"Webhook {expected_type} delivered with a valid Standard Webhooks signature")


def main() -> None:
    print(f"{DIM}Smoke test against {API_URL}{RESET}", flush=True)
    try:
        run()
    except httpx.ConnectError as exc:
        url = exc.request.url
        target = "webhook receiver" if str(url).startswith(RECEIVER_URL) else "API"
        print(
            f"{RED}✗ Smoke test failed:{RESET} cannot connect to {target} at {url}", file=sys.stderr
        )
        # make local-start поднимает стек без получателя: он только в профиле smoke.
        print(
            f"{DIM}  Run make local-smoke: it starts the stack with the webhook receiver{RESET}",
            file=sys.stderr,
        )
        sys.exit(1)
    except (SmokeError, httpx.HTTPError) as exc:
        print(f"{RED}✗ Smoke test failed:{RESET} {exc}", file=sys.stderr)
        print(f"{DIM}  Logs: make local-logs{RESET}", file=sys.stderr)
        sys.exit(1)
    print(f"{GREEN}Smoke test passed{RESET}")


if __name__ == "__main__":
    main()
