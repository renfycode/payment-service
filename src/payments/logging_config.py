"""Логирование через loguru.

Весь вывод идёт через loguru: и наш код, и библиотеки на стандартном logging
(uvicorn, FastStream, SQLAlchemy, aio-pika) — их записи перехватывает InterceptHandler.

Два формата:
- pretty (по умолчанию) — цветной человекочитаемый вывод для разработки и docker compose logs;
- json (format = "json" в секции [logging]) — одна JSON-запись на строку для сборщиков логов.
"""

import json as jsonlib
import logging
import sys
import traceback
from typing import TYPE_CHECKING, Any

from loguru import logger

if TYPE_CHECKING:
    from loguru import Message, Record

# Поля, которые FastStream кладёт в LogRecord: показываем их как контекст записи.
_STDLIB_CONTEXT_FIELDS = ("exchange", "queue", "message_id")

# Логгеры библиотек, которые ставят собственные обработчики: снимаем их,
# чтобы записи всплывали в корневой логгер и попадали в loguru.
_LIBRARY_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access", "faststream")

# Шумные библиотеки: на INFO они пишут каждое соединение и каждый запрос.
_QUIET_LOGGERS = ("httpx", "httpcore", "aiormq", "aio_pika", "asyncio")

_HEALTHCHECK_PATH = "/health"

# Служебный ключ extra: запись пришла из стандартного logging. В вывод не попадает.
_STDLIB_MARKER = "_stdlib"


class InterceptHandler(logging.Handler):
    """Перенаправляет записи стандартного logging в loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        level: str | int
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        context = {
            field: value
            for field in _STDLIB_CONTEXT_FIELDS
            if (value := getattr(record, field, None))
        }

        def use_stdlib_origin(loguru_record: "Record") -> None:
            # Источник записи — исходный логгер библиотеки, а не этот обработчик.
            loguru_record["name"] = record.name
            loguru_record["function"] = record.funcName
            loguru_record["line"] = record.lineno

        logger.patch(use_stdlib_origin).bind(**context, **{_STDLIB_MARKER: True}).opt(
            exception=record.exc_info
        ).log(level, record.getMessage())


class _HealthcheckFilter(logging.Filter):
    """Не пишет в access-лог запросы healthcheck: docker дёргает их каждые 5 секунд."""

    def filter(self, record: logging.LogRecord) -> bool:
        return _HEALTHCHECK_PATH not in record.getMessage()


def _pretty_format(record: "Record") -> str:
    # Значения контекста подставляются плейсхолдерами, а не напрямую:
    # фигурные скобки в данных не сломают разметку loguru.
    context = "".join(
        f" <dim>{key}=</dim><magenta>{{extra[{key}]}}</magenta>"
        for key in record["extra"]
        if not key.startswith("_")
    )
    # Для библиотек функция и строка указывают на их внутренние обёртки над logging —
    # полезно только имя логгера. Для нашего кода показываем точное место вызова.
    if record["extra"].get(_STDLIB_MARKER):
        source = "<cyan>{name}</cyan>"
    else:
        source = "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan>"
    return (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> <dim>│</dim> "
        "<level>{level: <8}</level> <dim>│</dim> "
        f"{source} <dim>│</dim> "
        "<level>{message}</level>" + context + "\n{exception}"
    )


def _json_sink(message: "Message") -> None:
    record = message.record
    entry: dict[str, Any] = {
        "timestamp": record["time"].isoformat(),
        "level": record["level"].name,
        "logger": record["name"],
        "message": record["message"],
        **{key: value for key, value in record["extra"].items() if not key.startswith("_")},
    }
    if not record["extra"].get(_STDLIB_MARKER):
        entry["function"] = record["function"]
        entry["line"] = record["line"]
    if (exception := record["exception"]) is not None:
        entry["exception"] = "".join(
            traceback.format_exception(exception.type, exception.value, exception.traceback)
        )
    sys.stderr.write(jsonlib.dumps(entry, ensure_ascii=False, default=str) + "\n")


def configure_logging(level: str = "INFO", *, json: bool = False) -> None:
    level = level.upper()
    logger.remove()
    if json:
        logger.add(_json_sink, level=level, backtrace=False, diagnose=False)
    else:
        # diagnose=False: значения переменных в трейсбеках могут содержать секреты и ПДн.
        logger.add(sys.stderr, level=level, format=_pretty_format, backtrace=True, diagnose=False)

    logging.basicConfig(handlers=[InterceptHandler()], level=level, force=True)
    for name in _LIBRARY_LOGGERS:
        library_logger = logging.getLogger(name)
        library_logger.handlers.clear()
        library_logger.propagate = True
    for name in _QUIET_LOGGERS:
        logging.getLogger(name).setLevel(max(logging.WARNING, logging.getLevelName(level)))

    access_logger = logging.getLogger("uvicorn.access")
    if not any(isinstance(f, _HealthcheckFilter) for f in access_logger.filters):
        access_logger.addFilter(_HealthcheckFilter())
