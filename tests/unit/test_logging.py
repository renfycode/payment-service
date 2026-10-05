import json
import logging
from collections.abc import Iterator

import pytest
from loguru import logger

from payments.logging_config import configure_logging


@pytest.fixture(autouse=True)
def reset_logging() -> Iterator[None]:
    yield
    configure_logging("INFO")


def test_stdlib_records_are_routed_to_loguru_with_context(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging("INFO", json=True)

    logging.getLogger("faststream.rabbit").info(
        "Received", extra={"queue": "payments.new", "message_id": "m-1"}
    )

    entry = json.loads(capsys.readouterr().err)
    assert entry["logger"] == "faststream.rabbit"
    assert entry["message"] == "Received"
    assert entry["queue"] == "payments.new"
    assert entry["message_id"] == "m-1"
    assert "_stdlib" not in entry


def test_json_entry_contains_bound_context_and_exception(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging("INFO", json=True)

    try:
        raise RuntimeError("boom")
    except RuntimeError:
        logger.bind(payment_id="p-1").exception("Failed")

    entry = json.loads(capsys.readouterr().err)
    assert entry["level"] == "ERROR"
    assert entry["payment_id"] == "p-1"
    assert "RuntimeError: boom" in entry["exception"]


def test_pretty_output_tolerates_braces_in_context(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO")

    logger.bind(payload="{not a placeholder}").info("Hello {}", "world")

    output = capsys.readouterr().err
    assert "Hello world" in output
    assert "payload={not a placeholder}" in output


def test_healthcheck_requests_are_not_access_logged(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO", json=True)
    access = logging.getLogger("uvicorn.access")

    access.info('127.0.0.1:1 - "GET /health HTTP/1.1" 200')
    access.info('127.0.0.1:1 - "GET /api/v1/payments/x HTTP/1.1" 200')

    lines = capsys.readouterr().err.splitlines()
    assert len(lines) == 1
    assert "/api/v1/payments" in lines[0]
