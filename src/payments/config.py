"""Конфигурация сервиса.

Источники по убыванию приоритета:
1. аргументы конструктора (тесты);
2. переменные окружения PAYMENTS__<СЕКЦИЯ>__<КЛЮЧ>, например PAYMENTS__RETRY__MAX_ATTEMPTS=5;
3. TOML-файл из переменной PAYMENTS_CONFIG (например, config/docker.toml);
4. значения по умолчанию ниже.

Секреты (поля SecretStr) задаются только через окружение: в TOML они запрещены,
чтобы файл конфигурации можно было хранить в git и спокойно ревьюить.
"""

import os
from pathlib import Path
from typing import Any, Literal, Self
from urllib.parse import quote

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)
from sqlalchemy import URL

CONFIG_PATH_ENV = "PAYMENTS_CONFIG"
ENV_PREFIX = "PAYMENTS__"
ENV_NESTED_DELIMITER = "__"


class Section(BaseModel):
    # forbid: опечатка в ключе TOML — ошибка старта, а не молча проигнорированный параметр.
    model_config = ConfigDict(extra="forbid", frozen=True)


class ApiSettings(Section):
    key: SecretStr


class DatabaseSettings(Section):
    host: str = "localhost"
    port: int = 5432
    name: str = "payments"
    user: str = "payments"
    password: SecretStr

    @property
    def url(self) -> str:
        return URL.create(
            "postgresql+asyncpg",
            username=self.user,
            password=self.password.get_secret_value(),
            host=self.host,
            port=self.port,
            database=self.name,
        ).render_as_string(hide_password=False)


class RabbitMQSettings(Section):
    host: str = "localhost"
    port: int = 5672
    vhost: str = "/"
    user: str = "guest"
    password: SecretStr

    @property
    def url(self) -> str:
        user = quote(self.user, safe="")
        password = quote(self.password.get_secret_value(), safe="")
        return f"amqp://{user}:{password}@{self.host}:{self.port}/{quote(self.vhost, safe='')}"


class OutboxSettings(Section):
    poll_interval: float = Field(default=1.0, gt=0)
    batch_size: int = Field(default=100, gt=0)
    # Пауза при недоступном брокере растёт как poll_interval * 2^n, но не выше max_backoff.
    max_backoff: float = Field(default=30.0, gt=0)


class ConsumerSettings(Section):
    # Сколько сообщений один экземпляр обрабатывает одновременно (prefetch RabbitMQ).
    # Ограничивает нагрузку на БД и HTTP-клиент и равномерно делит очередь между репликами.
    prefetch: int = Field(default=10, ge=1)


class RetrySettings(Section):
    # Общее число попыток на каждый этап (шлюз, webhook), включая первую.
    max_attempts: int = Field(default=3, ge=1)
    # Задержка перед повтором n (n = 1, 2, …) равна base_delay * 2 ** (n - 1), секунды.
    base_delay: float = Field(default=2.0, gt=0)


class GatewaySettings(Section):
    min_delay: float = Field(default=2.0, ge=0)
    max_delay: float = Field(default=5.0, ge=0)
    success_rate: float = Field(default=0.90, ge=0, le=1)
    # Доля бизнес-отказов; остаток (1 - success - decline) — технические сбои шлюза.
    decline_rate: float = Field(default=0.07, ge=0, le=1)

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.min_delay > self.max_delay:
            raise ValueError("min_delay must not exceed max_delay")
        if self.success_rate + self.decline_rate > 1:
            raise ValueError("success_rate + decline_rate must not exceed 1")
        return self


class WebhookSettings(Section):
    # Секрет подписи в формате Standard Webhooks: "whsec_<base64>".
    secret: SecretStr
    timeout: float = Field(default=10.0, gt=0)


class LoggingSettings(Section):
    level: str = "INFO"
    # pretty — цветной вывод для людей, json — одна запись на строку для сборщиков логов.
    format: Literal["pretty", "json"] = "pretty"


class _ConfigBase(BaseSettings):
    """Общий механизм загрузки: env важнее TOML, TOML важнее значений по умолчанию."""

    model_config = SettingsConfigDict(
        env_prefix=ENV_PREFIX,
        env_nested_delimiter=ENV_NESTED_DELIMITER,
        extra="forbid",
        frozen=True,
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        sources: list[PydanticBaseSettingsSource] = [init_settings, env_settings]
        if config_path := os.environ.get(CONFIG_PATH_ENV):
            sources.append(SecretFreeTomlSource(settings_cls, Path(config_path)))
        return tuple(sources)


class Settings(_ConfigBase):
    """Полная конфигурация api и consumer."""

    api: ApiSettings
    database: DatabaseSettings
    rabbitmq: RabbitMQSettings
    webhook: WebhookSettings
    outbox: OutboxSettings = OutboxSettings()
    consumer: ConsumerSettings = ConsumerSettings()
    retry: RetrySettings = RetrySettings()
    gateway: GatewaySettings = GatewaySettings()
    logging: LoggingSettings = LoggingSettings()


class MaintenanceSettings(_ConfigBase):
    """Только БД и логирование — для служебных команд (миграции, очистка outbox).

    Им не нужны секреты API, webhook и RabbitMQ.
    """

    # Остальные секции общего файла конфигурации здесь не нужны.
    model_config = SettingsConfigDict(extra="ignore")

    database: DatabaseSettings
    logging: LoggingSettings = LoggingSettings()


class SecretFreeTomlSource(TomlConfigSettingsSource):
    """TOML-источник, который требует существования файла и запрещает в нём секреты."""

    def __init__(self, settings_cls: type[BaseSettings], path: Path) -> None:
        if not path.is_file():
            raise FileNotFoundError(f"Config file {path} (from ${CONFIG_PATH_ENV}) not found")
        super().__init__(settings_cls, toml_file=path)

        leaked = [
            secret for secret in secret_paths(settings_cls) if _lookup(self.toml_data, secret)
        ]
        if leaked:
            hints = ", ".join(
                f"{'.'.join(p)} -> {ENV_PREFIX}{ENV_NESTED_DELIMITER.join(p).upper()}"
                for p in leaked
            )
            raise ValueError(f"Secrets must not be stored in {path}, use environment: {hints}")


def secret_paths(model: type[BaseModel], prefix: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
    """Пути ко всем полям SecretStr модели, например [("api", "key"), ...]."""
    paths: list[tuple[str, ...]] = []
    for name, field in model.model_fields.items():
        annotation = field.annotation
        if annotation is SecretStr:
            paths.append((*prefix, name))
        elif isinstance(annotation, type) and issubclass(annotation, BaseModel):
            paths.extend(secret_paths(annotation, (*prefix, name)))
    return paths


def _lookup(data: dict[str, Any], path: tuple[str, ...]) -> bool:
    node: Any = data
    for key in path:
        if not isinstance(node, dict) or key not in node:
            return False
        node = node[key]
    return True
