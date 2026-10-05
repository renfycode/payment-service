import tomllib
from typing import Any

import pytest
from typer.testing import CliRunner

from payments.cli import app
from payments.cli.config import to_toml
from payments.config import CONFIG_PATH_ENV

runner = CliRunner()

SECRETS = {
    "PAYMENTS__API__KEY": "super-secret-key",
    "PAYMENTS__WEBHOOK__SECRET": "whsec_c2VjcmV0",
    "PAYMENTS__DATABASE__PASSWORD": "db-password",
    "PAYMENTS__RABBITMQ__PASSWORD": "mq-password",
}


@pytest.fixture(autouse=True)
def isolated_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(CONFIG_PATH_ENV, raising=False)
    for name in SECRETS:
        monkeypatch.delenv(name, raising=False)


def test_help_lists_commands() -> None:
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    for command in ("api", "consumer", "db", "config", "dlq"):
        assert command in result.output


def test_config_show_masks_secrets() -> None:
    result = runner.invoke(
        app, ["config", "show"], env={**SECRETS, "PAYMENTS__RETRY__MAX_ATTEMPTS": "5"}
    )

    assert result.exit_code == 0, result.output
    assert "max_attempts = 5" in result.output
    for secret in SECRETS.values():
        assert secret not in result.output
    assert "**********" in result.output


def test_config_check_reports_missing_secrets_with_env_hint() -> None:
    result = runner.invoke(app, ["config", "check"], env={"PAYMENTS__API__KEY": "k"})

    assert result.exit_code == 1
    assert "database.password" in result.output
    assert "PAYMENTS__DATABASE__PASSWORD" in result.output


def test_config_check_passes_for_valid_configuration() -> None:
    result = runner.invoke(
        app, ["config", "check"], env={**SECRETS, CONFIG_PATH_ENV: "config/docker.toml"}
    )

    assert result.exit_code == 0, result.output
    assert "valid" in result.output


def test_db_downgrade_requires_confirmation() -> None:
    result = runner.invoke(
        app, ["db", "downgrade", "base"], env={"PAYMENTS__DATABASE__PASSWORD": "pw"}, input="n\n"
    )

    assert result.exit_code == 1
    assert "Aborted" in result.output


def test_rendered_config_is_valid_toml() -> None:
    data: dict[str, dict[str, Any]] = {
        "database": {"host": "db", "port": 5432},
        "logging": {"format": 'say "hi"', "flag": True},
        "gateway": {"success_rate": 0.9},
    }

    assert tomllib.loads(to_toml(data)) == data
