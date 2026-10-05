"""Тестовый получатель webhook.

Независимо от кода сервиса проверяет подпись по спецификации Standard Webhooks
и записывает каждую доставку.

POST /hooks/<name>[?fail=N]  — первые N запросов на <name> получают 500;
                               далее 204 при валидной подписи, 401 при невалидной.
GET  /hooks/<name>           — JSON-список доставок на <name>.
GET  /health                 — 200.
"""

import base64
import hashlib
import hmac
import json
import os
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

SECRET = base64.b64decode(os.environ["WEBHOOK_SECRET"].removeprefix("whsec_"))
TOLERANCE_SECONDS = 5 * 60

_lock = threading.Lock()
_deliveries: dict[str, list[dict[str, Any]]] = {}


def verify(headers: Any, body: bytes) -> bool:
    msg_id = headers.get("webhook-id")
    timestamp = headers.get("webhook-timestamp")
    signatures = headers.get("webhook-signature")
    if not (msg_id and timestamp and signatures):
        return False
    if abs(time.time() - int(timestamp)) > TOLERANCE_SECONDS:
        return False
    expected = base64.b64encode(
        hmac.new(SECRET, f"{msg_id}.{timestamp}.".encode() + body, hashlib.sha256).digest()
    ).decode()
    # Заголовок может содержать несколько подписей через пробел: "v1,<sig> v1,<sig2>".
    return any(
        version == "v1" and hmac.compare_digest(signature, expected)
        for version, _, signature in (item.partition(",") for item in signatures.split())
    )


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        path = urlsplit(self.path).path
        if path == "/health":
            self._respond(HTTPStatus.OK, {"status": "ok"})
        elif path.startswith("/hooks/"):
            with _lock:
                deliveries = list(_deliveries.get(path.removeprefix("/hooks/"), []))
            self._respond(HTTPStatus.OK, deliveries)
        else:
            self._respond(HTTPStatus.NOT_FOUND, {"detail": "not found"})

    def do_POST(self) -> None:
        url = urlsplit(self.path)
        if not url.path.startswith("/hooks/"):
            self._respond(HTTPStatus.NOT_FOUND, {"detail": "not found"})
            return
        name = url.path.removeprefix("/hooks/")
        fail_first = int(parse_qs(url.query).get("fail", ["0"])[0])
        body = self.rfile.read(int(self.headers.get("content-length", 0)))
        signature_valid = verify(self.headers, body)

        with _lock:
            deliveries = _deliveries.setdefault(name, [])
            if len(deliveries) < fail_first:
                status = HTTPStatus.INTERNAL_SERVER_ERROR
            elif signature_valid:
                status = HTTPStatus.NO_CONTENT
            else:
                status = HTTPStatus.UNAUTHORIZED
            deliveries.append(
                {
                    "status_code": status.value,
                    "signature_valid": signature_valid,
                    "received_at": time.time(),
                    "headers": {k.lower(): v for k, v in self.headers.items()},
                    "body": json.loads(body or b"null"),
                }
            )
        self._respond(status, None)

    def _respond(self, status: HTTPStatus, payload: Any) -> None:
        self.send_response(status)
        if payload is None:
            self.end_headers()
            return
        data = json.dumps(payload).encode()
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format: str, *args: Any) -> None:
        print(f"{self.command} {self.path} -> {args[1] if len(args) > 1 else ''}", flush=True)


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", 8080), Handler)
    print("Webhook receiver listening on :8080", flush=True)
    server.serve_forever()
