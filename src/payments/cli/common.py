import os
from typing import NoReturn

import typer
from pydantic import BaseModel, ValidationError
from rich.console import Console
from rich.markup import escape

from payments.config import (
    CONFIG_PATH_ENV,
    ENV_NESTED_DELIMITER,
    ENV_PREFIX,
    MaintenanceSettings,
    Settings,
)
from payments.logging_config import configure_logging

console = Console()
err_console = Console(stderr=True)


def new_typer(help_text: str) -> typer.Typer:
    # show_locals=False: в локальных переменных трейсбека могут оказаться секреты.
    return typer.Typer(
        help=help_text,
        no_args_is_help=True,
        pretty_exceptions_show_locals=False,
        rich_markup_mode="rich",
    )


def fail(message: str) -> NoReturn:
    err_console.print(f"[bold red]Error:[/] {message}")
    raise typer.Exit(code=1)


def describe_config_error(exc: Exception) -> str:
    """Компактное описание ошибки конфигурации с подсказкой переменной окружения."""
    if not isinstance(exc, ValidationError):
        return escape(str(exc))
    lines = []
    for error in exc.errors(include_url=False):
        path = tuple(str(part) for part in error["loc"])
        # Секция, заданная только секретами из env, при их отсутствии целиком «пропущена»:
        # разворачиваем до обязательных полей, чтобы подсказать конкретные переменные.
        missing = _required_fields(path) if error["type"] == "missing" else []
        for field_path in missing or [path]:
            env_var = ENV_PREFIX + ENV_NESTED_DELIMITER.join(field_path).upper()
            field, message = escape(".".join(field_path)), escape(error["msg"])
            lines.append(f"  • {field}: {message}  [dim](env {env_var})[/]")
    return "\n".join(lines)


def _required_fields(path: tuple[str, ...]) -> list[tuple[str, ...]]:
    """Обязательные поля вложенной секции настроек по её пути, например ("database",)."""
    model: type[BaseModel] = Settings
    for name in path:
        field = model.model_fields.get(name)
        annotation = field.annotation if field else None
        if not (isinstance(annotation, type) and issubclass(annotation, BaseModel)):
            return []
        model = annotation
    return [(*path, name) for name, field in model.model_fields.items() if field.is_required()]


def _config_failure(exc: Exception) -> NoReturn:
    fail(f"invalid configuration ({config_source()}):\n{describe_config_error(exc)}")


def config_source() -> str:
    path = os.environ.get(CONFIG_PATH_ENV)
    return f"{CONFIG_PATH_ENV}={path}" if path else f"{CONFIG_PATH_ENV} is not set"


def load_settings() -> Settings:
    """Загружает конфигурацию; при ошибке печатает её и завершает команду с кодом 1."""
    try:
        return Settings()  # pyright: ignore[reportCallIssue]
    except (ValidationError, ValueError, FileNotFoundError) as exc:
        _config_failure(exc)


def load_maintenance_settings() -> MaintenanceSettings:
    try:
        return MaintenanceSettings()  # pyright: ignore[reportCallIssue]
    except (ValidationError, ValueError, FileNotFoundError) as exc:
        _config_failure(exc)


def setup_logging(settings: Settings | MaintenanceSettings) -> None:
    configure_logging(settings.logging.level, json=settings.logging.format == "json")
