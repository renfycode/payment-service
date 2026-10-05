from typing import Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Конфигурация сервиса. Все параметры читаются из переменных окружения."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"

    api_key: SecretStr
    # Секрет подписи webhook в формате Standard Webhooks: "whsec_<base64>".
    webhook_secret: SecretStr

    outbox_poll_interval: float = Field(default=1.0, gt=0)
    outbox_batch_size: int = Field(default=100, gt=0)

    # Общее число попыток на каждый этап (шлюз, webhook), включая первую.
    max_attempts: int = Field(default=3, ge=1)
    # Задержка перед повтором n (n = 1, 2, …) равна retry_base_delay * 2 ** (n - 1).
    retry_base_delay: float = Field(default=2.0, gt=0)

    gateway_min_delay: float = Field(default=2.0, ge=0)
    gateway_max_delay: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.90, ge=0, le=1)
    # Доля бизнес-отказов; остаток (1 - success - decline) — технические сбои шлюза.
    gateway_decline_rate: float = Field(default=0.07, ge=0, le=1)

    webhook_timeout: float = Field(default=10.0, gt=0)

    log_level: str = "INFO"

    @model_validator(mode="after")
    def _check_gateway(self) -> Self:
        if self.gateway_min_delay > self.gateway_max_delay:
            raise ValueError("gateway_min_delay must not exceed gateway_max_delay")
        if self.gateway_success_rate + self.gateway_decline_rate > 1:
            raise ValueError("gateway_success_rate + gateway_decline_rate must not exceed 1")
        return self
