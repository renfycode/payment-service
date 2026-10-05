from pathlib import Path

import pytest
from pydantic import ValidationError

from payments.config import CONFIG_PATH_ENV, MigrationSettings, Settings

ROOT = Path(__file__).parents[2]

SECRETS = {
    "PAYMENTS__API__KEY": "api-key",
    "PAYMENTS__WEBHOOK__SECRET": "whsec_c2VjcmV0",
    "PAYMENTS__DATABASE__PASSWORD": "p@ss/word",
    "PAYMENTS__RABBITMQ__PASSWORD": "rabbit:pw",
}


@pytest.fixture
def secrets_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in SECRETS.items():
        monkeypatch.setenv(name, value)


def use_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, content: str) -> None:
    path = tmp_path / "config.toml"
    path.write_text(content)
    monkeypatch.setenv(CONFIG_PATH_ENV, str(path))


@pytest.mark.usefixtures("secrets_env")
@pytest.mark.parametrize("name", ["local.toml", "docker.toml", "production.example.toml"])
def test_shipped_config_files_are_valid(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    monkeypatch.setenv(CONFIG_PATH_ENV, str(ROOT / "config" / name))

    Settings()  # pyright: ignore[reportCallIssue]


@pytest.mark.usefixtures("secrets_env")
def test_values_are_read_from_toml(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    use_config(
        monkeypatch,
        tmp_path,
        '[retry]\nmax_attempts = 5\n[database]\nhost = "db.internal"\n',
    )

    settings = Settings()  # pyright: ignore[reportCallIssue]

    assert settings.retry.max_attempts == 5
    assert settings.retry.base_delay == 2.0  # не указан в файле — значение по умолчанию
    assert settings.database.host == "db.internal"
    assert settings.api.key.get_secret_value() == "api-key"


@pytest.mark.usefixtures("secrets_env")
def test_environment_overrides_toml(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    use_config(monkeypatch, tmp_path, "[retry]\nmax_attempts = 5\nbase_delay = 1.0\n")
    monkeypatch.setenv("PAYMENTS__RETRY__MAX_ATTEMPTS", "7")

    settings = Settings()  # pyright: ignore[reportCallIssue]

    assert settings.retry.max_attempts == 7
    assert settings.retry.base_delay == 1.0


@pytest.mark.usefixtures("secrets_env")
def test_secrets_in_toml_are_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    use_config(monkeypatch, tmp_path, '[api]\nkey = "oops"\n')

    with pytest.raises(ValueError, match=r"api\.key -> PAYMENTS__API__KEY"):
        Settings()  # pyright: ignore[reportCallIssue]


@pytest.mark.usefixtures("secrets_env")
def test_unknown_keys_are_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    use_config(monkeypatch, tmp_path, "[retry]\nmax_atempts = 5\n")

    with pytest.raises(ValidationError, match="max_atempts"):
        Settings()  # pyright: ignore[reportCallIssue]


def test_missing_config_file_is_an_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv(CONFIG_PATH_ENV, str(tmp_path / "missing.toml"))

    with pytest.raises(FileNotFoundError):
        Settings()  # pyright: ignore[reportCallIssue]


def test_missing_secret_is_an_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(CONFIG_PATH_ENV, raising=False)

    with pytest.raises(ValidationError, match="api"):
        Settings()  # pyright: ignore[reportCallIssue]


@pytest.mark.usefixtures("secrets_env")
def test_connection_urls_escape_credentials() -> None:
    settings = Settings()  # pyright: ignore[reportCallIssue]

    assert settings.database.url == (
        "postgresql+asyncpg://payments:p%40ss%2Fword@localhost:5432/payments"
    )
    assert settings.rabbitmq.url == "amqp://guest:rabbit%3Apw@localhost:5672/%2F"


def test_migrations_need_only_database_secret(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    use_config(monkeypatch, tmp_path, '[database]\nhost = "db"\n[retry]\nmax_attempts = 5\n')
    monkeypatch.setenv("PAYMENTS__DATABASE__PASSWORD", "pw")

    settings = MigrationSettings()  # pyright: ignore[reportCallIssue]

    assert settings.database.host == "db"
