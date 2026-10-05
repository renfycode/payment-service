"""Конфигурация Alembic без alembic.ini.

Миграции лежат внутри пакета (payments/migrations) и находятся по имени пакета,
поэтому команды работают из любого каталога — и в репозитории, и в Docker-образе.
URL БД берётся из конфигурации сервиса (MaintenanceSettings) в migrations/env.py,
если не передан явно.
"""

import sys

from alembic.config import Config

SCRIPT_LOCATION = "payments:migrations"


def alembic_config(database_url: str | None = None) -> Config:
    # stdout явно: значение по умолчанию в Alembic захватывается при импорте и не учитывает
    # перенаправление вывода (например, в тестах CLI).
    config = Config(stdout=sys.stdout)
    config.set_main_option("script_location", SCRIPT_LOCATION)
    # Имена файлов миграций: 0002_add_something.py — порядок виден в листинге каталога.
    config.set_main_option("file_template", "%%(rev)s_%%(slug)s")
    if database_url is not None:
        config.set_main_option("sqlalchemy.url", database_url)
    return config
