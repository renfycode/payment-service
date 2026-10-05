# Payments Service

Микросервис асинхронной обработки платежей. Принимает запрос на оплату, проводит его через эмулятор платёжного шлюза и уведомляет клиента о результате через webhook.

**Стек:** Python 3.13, FastAPI + Pydantic v2, SQLAlchemy 2.0 (async) + asyncpg, PostgreSQL 17, RabbitMQ 4 (FastStream), Alembic, dependency-injector, Typer, loguru, Docker Compose, uv.

## Быстрый старт

```bash
make local-start    # собрать и поднять весь стек в Docker
make local-smoke    # проверить его целиком: платёж → обработка → подписанный webhook
```

| Команда | Что делает |
|---|---|
| `make local-start` | собирает образ и поднимает postgres, rabbitmq, api, consumer; ждёт, пока все станут healthy |
| `make local-smoke` | поднимает стек вместе с тестовым получателем webhook и прогоняет smoke-тест |
| `make local-logs` | логи api и consumer в реальном времени |
| `make local-stop` | останавливает стек, данные (БД, очереди) сохраняются |
| `make local-clean` | останавливает стек и удаляет тома с данными |

Smoke-тест (`scripts/smoke.py`) проходит путь платежа через публичные интерфейсы: проверяет доступность API и получателя, отказ без `X-API-Key`, создание платежа и идемпотентный повтор, финальный статус, доставку webhook нужного типа с валидной подписью. При ошибке он сообщает, на каком шаге она произошла, и завершается с кодом 1. Получатель webhook — тот же контейнер, что в интеграционных тестах; в compose он подключён под профилем `smoke` и при обычном запуске не поднимается.

Без `make` то же самое делается напрямую: `docker compose up -d --build --wait`.

Конфигурация берётся из `config/docker.toml`. Секреты для локального стенда заданы в `docker-compose.yml` значениями по умолчанию: API-ключ `dev-api-key`, секрет подписи webhook `whsec_MRVnyabDOXIn1GLmVyP2VeXwy/Ts+AwO`, пароли Postgres и RabbitMQ `payments`. Переопределить их можно переменными окружения при запуске: `PAYMENTS_API_KEY`, `PAYMENTS_WEBHOOK_SECRET`, `POSTGRES_PASSWORD`, `RABBITMQ_PASSWORD`.

| Сервис | Адрес |
|---|---|
| API | http://localhost:8000 |
| Swagger UI | http://localhost:8000/docs |
| RabbitMQ Management | http://localhost:15672 (`payments` / `payments`) |
| PostgreSQL | `localhost:5432` (для запуска приложения на хосте) |
| RabbitMQ AMQP | `localhost:5672` (для запуска приложения на хосте) |

Миграции применяются автоматически при старте контейнера `api` (`payments api --migrate`).

## Командная строка

Все процессы и служебные операции запускаются одной командой `payments`: в образе она доступна напрямую, локально — через `uv run payments`. `payments --help` и `payments <команда> --help` показывают все параметры.

| Команда | Назначение |
|---|---|
| `payments api [--host] [--port] [--workers] [--reload] [--migrate]` | HTTP API вместе с outbox relay; `--migrate` применяет миграции перед стартом |
| `payments consumer [--host] [--port]` | обработчик очереди `payments.new`; `GET /health` на порту 8001 проверяет подключение к RabbitMQ |
| `payments db upgrade [REVISION]` | применить миграции, по умолчанию до `head` |
| `payments db downgrade REVISION [-y]` | откатить миграции до `0001`, `base` или на шаг назад: `payments db downgrade -- -1`; спрашивает подтверждение |
| `payments db current` / `payments db history` | текущая ревизия БД / список миграций |
| `payments db revision -m "..."` | создать миграцию с автогенерацией по моделям и следующим номером (`0003`, …) |
| `payments config show` | итоговая конфигурация (TOML + env + умолчания), секреты замаскированы |
| `payments config check` | проверить конфигурацию; код выхода 1 и список ошибок с подсказкой нужной переменной |
| `payments dlq list [--limit N]` | сообщения в DLQ с этапом, числом попыток и причиной; из очереди не удаляются |
| `payments dlq requeue [--payment-id ID] [--limit N] [-y]` | вернуть сообщения из DLQ в `payments.new`; счётчик попыток начинается заново |
| `payments outbox cleanup [--older-than-days 180] [-y]` | удалить опубликованные события outbox старше N дней; неопубликованные не удаляются никогда |

В docker compose:

```bash
docker compose exec api payments config show
docker compose exec api payments db current
docker compose exec api payments dlq list
docker compose exec api payments dlq requeue --payment-id 01a10b80-bcc0-7460-886d-3681688f9d84
```

В продакшене флаг `--migrate` не используется: миграции — отдельный шаг развёртывания (`payments db upgrade` в Job или init-контейнере), чтобы несколько реплик api не накатывали их одновременно.

## Примеры использования

Все эндпоинты требуют заголовок `X-API-Key`. В качестве `webhook_url` удобно взять адрес с https://webhook.site.

### Создание платежа

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: dev-api-key" \
  -H "Idempotency-Key: order-42" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": {"order_id": 42},
        "webhook_url": "https://webhook.site/<your-id>"
      }'
```

```http
HTTP/1.1 202 Accepted

{"payment_id": "01a10b80-bcc0-7460-886d-3681688f9d84", "status": "pending", "created_at": "2026-10-05T09:59:08.728889Z"}
```

### Получение платежа

```bash
curl http://localhost:8000/api/v1/payments/01a10b80-bcc0-7460-886d-3681688f9d84 \
  -H "X-API-Key: dev-api-key"
```

```json
{
  "id": "01a10b80-bcc0-7460-886d-3681688f9d84",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Order #42",
  "metadata": {"order_id": 42},
  "status": "succeeded",
  "idempotency_key": "order-42",
  "webhook_url": "https://webhook.site/<your-id>",
  "created_at": "2026-10-05T09:59:08.728889Z",
  "processed_at": "2026-10-05T09:59:12.125699Z"
}
```

### Коды ответов

| Код | Когда |
|---|---|
| `202` | Платёж принят. Повтор с тем же `Idempotency-Key` и тем же телом возвращает исходный ответ и заголовок `Idempotent-Replayed: true` |
| `401` | Нет или неверный `X-API-Key` |
| `404` | Платёж не найден |
| `409` | `Idempotency-Key` уже использован с другим телом запроса |
| `422` | Ошибка валидации (в том числе отсутствует `Idempotency-Key`) |

Сумма передаётся строкой или числом, должна быть больше 0, не больше 2 знаков после запятой. В ответах сумма — строка, чтобы не терять точность.

### Webhook

После обработки сервис отправляет `POST` на `webhook_url`:

```json
{
  "type": "payment.succeeded",
  "timestamp": "2026-10-05T09:59:12.125699Z",
  "data": { "...": "то же, что в GET /api/v1/payments/{id}" }
}
```

`type` — `payment.succeeded` или `payment.failed`. Успешной доставкой считается любой ответ `2xx`, редиректы не выполняются.

Запрос подписан по спецификации [Standard Webhooks](https://www.standardwebhooks.com) заголовками `webhook-id`, `webhook-timestamp`, `webhook-signature`. `webhook-id` одинаков во всех повторах — по нему получатель отбрасывает дубли. Проверка на стороне получателя:

```python
import base64, hashlib, hmac


def verify(secret: str, headers: dict[str, str], body: bytes) -> bool:
    key = base64.b64decode(secret.removeprefix("whsec_"))
    signed = f"{headers['webhook-id']}.{headers['webhook-timestamp']}.".encode() + body
    expected = base64.b64encode(hmac.new(key, signed, hashlib.sha256).digest()).decode()
    return any(
        hmac.compare_digest(sig.partition(",")[2], expected)
        for sig in headers["webhook-signature"].split()
    )
```

Также можно использовать официальные библиотеки `standardwebhooks` для Python, JS, Go и других языков.

## Архитектура

```
 Client ──POST──▶ api ──┬─ INSERT payments ─┐  одна транзакция
                        └─ INSERT outbox  ──┘
                         outbox relay (фоновая задача в api)
                                │ publish + publisher confirms
                                ▼
            exchange "payments" ──payments.new──▶ [payments.new] ──▶ consumer
                                                                     │ 1. шлюз (2–5 с)
                                                                     │ 2. UPDATE status
                                                                     │ 3. webhook
             ┌────── техническая ошибка, попытка < 3 ─────────────────┤
             ▼                                                       │ попытки исчерпаны
  exchange "payments.retry"                                          ▼
   ├─▶ [payments.new.retry.2000ms] ─TTL─┐           exchange "payments.dlx"
   └─▶ [payments.new.retry.4000ms] ─TTL─┴─▶ payments.new    └─▶ [payments.new.dlq]
```

### Ключевые решения

**Outbox pattern.** Платёж и событие `payment.created` пишутся в одной транзакции. Фоновая задача в процессе `api` раз в секунду забирает неопубликованные записи (`SELECT … FOR UPDATE SKIP LOCKED`, поэтому безопасно при нескольких репликах). Она публикует их с publisher confirms и только после подтверждения брокера проставляет `published_at`. Гарантия — at-least-once. Публикация с `mandatory` и `on_return_raises`: если брокер не смог доставить событие ни в одну очередь, это ошибка, и событие остаётся в outbox, а не теряется с подтверждением.

Пока брокер недоступен, relay повторяет попытки с растущей паузой `poll_interval · 2ⁿ` до `max_backoff` (по умолчанию 2, 4, 8, 16, 30, 30… с) и пишет одну строку на попытку; после восстановления — одну запись `Outbox publishing recovered`. API в это время продолжает принимать платежи. Опубликованные события удаляются командой `payments outbox cleanup` (запускайте по расписанию, например раз в сутки).

**Идемпотентность API.** `Idempotency-Key` уникален в таблице `payments`, рядом хранится SHA-256 нормализованного тела запроса. Тот же ключ с тем же телом возвращает исходный платёж, с другим телом — `409`. Гонку одновременных запросов с одним ключом закрывает уникальный индекс: проигравший запрос ловит `IntegrityError` и возвращает победивший платёж.

**Идемпотентность consumer.** Сообщение содержит только `payment_id`, источник правды — БД. Этап определяется по статусу: если платёж уже финализирован, шлюз повторно не вызывается. Статус меняется условным `UPDATE … WHERE status = 'pending'`, поэтому конкурентные обработчики не перезапишут результат друг друга.

**Эмуляция шлюза.** Задержка 2–5 с. Исходы:
- 90% — успех (`succeeded`);
- 7% — бизнес-отказ (`failed`, webhook отправляется, повторов нет);
- 3% — технический сбой (повтор).

Задержка и доли настраиваются в секции `[gateway]`. Как настоящий шлюз с ключом идемпотентности, эмулятор на повторный запрос по тому же `payment_id` возвращает тот же итог: он выводится из `payment_id`, поэтому не меняется ни между попытками, ни после рестартов, ни на разных репликах. Технический сбой, наоборот, случаен на каждый вызов — иначе повтор никогда бы не помог.

**Retry.** 3 попытки на этап, то есть первая и 2 повтора с экспоненциальной задержкой `base_delay · 2ⁿ⁻¹` (2 с и 4 с по умолчанию). Число попыток и базовая задержка настраиваются в секции `[retry]`. Повторы реализованы очередями-задержками с TTL и dead-letter обратно в `payments.new`: consumer не блокируется на ожидании, повторы переживают его рестарт. Под каждую задержку своя очередь — в одной очереди с разными TTL сообщения истекают только из головы.

У этапов «шлюз» и «webhook» независимые счётчики (заголовки `x-stage`, `x-attempt`): сбои шлюза не съедают попытки доставки webhook.

**Dead Letter Queue.** Если попытки этапа исчерпаны, consumer публикует сообщение в `payments.new.dlq` с заголовками `x-dead-letter-stage`, `x-dead-letter-attempts`, `x-dead-letter-reason`.
- Если исчерпаны попытки шлюза, платёж остаётся `pending`: результат на стороне шлюза неизвестен, честнее разобраться вручную, чем объявить его `failed`.
- Если не удалось доставить webhook, статус платежа не меняется.

Сообщения, которые не удалось даже разобрать, отклоняются (`reject`) и попадают в DLQ через DLX очереди. Все очереди — quorum с dead-lettering в режиме at-least-once.

**Аутентификация.** Статический ключ в `X-API-Key`, сравнение за постоянное время (`secrets.compare_digest`).

**Внедрение зависимостей.** Все зависимости собираются в `src/payments/containers.py` на [dependency-injector](https://python-dependency-injector.ets-labs.org):
- `CoreContainer` — общее для обоих процессов: типизированный `Settings`, engine БД, фабрика сессий, политика повторов;
- `ApiContainer` — процесс api: брокер для публикации, outbox relay и его фоновая задача, `PaymentService`;
- `ConsumerContainer` — процесс consumer: брокер с подписчиком, шлюз, HTTP-клиент и подпись webhook, processor.

Подключения, HTTP-клиент и фоновая задача relay описаны как `providers.Resource`: их открывает `init_resources()` и закрывает `shutdown_resources()` в lifespan FastAPI и в хуках FastStream. dependency-injector не закрывает ресурсы в обратном порядке зависимостей, поэтому задача relay останавливается явно, до брокера и БД.

В FastAPI зависимости внедряются штатно, через `@inject` и `Depends(Provide[...])`. FastStream разбирает сигнатуру обработчика сам и маркеры `Provide` не понимает, поэтому обработчик берёт processor из контейнера явно. В интеграционных тестах шлюз подменяется через `container.gateway.override(...)` детерминированными заглушками: «всегда успех», «всегда отказ», «всегда недоступен».

### Логирование

Весь вывод идёт через [loguru] — и код сервиса, и библиотеки на стандартном `logging` (uvicorn, FastStream, Alembic, SQLAlchemy): их записи перехватываются и оформляются так же. К записям consumer привязан контекст: `payment_id`, этап и номер попытки, а у сообщений FastStream — `exchange`, `queue`, `message_id`. По `payment_id` удобно собрать всю историю платежа:

```
2026-10-05 10:07:22.110 │ INFO     │ faststream.rabbit │ Received exchange=payments queue=payments.new message_id=01a10b88-…
2026-10-05 10:07:26.500 │ INFO     │ payments.consumer.processor:_charge:126 │ Payment finalized as succeeded payment_id=01a10b88-…
2026-10-05 10:07:26.506 │ WARNING  │ payments.consumer.processor:_on_failure:136 │ Attempt failed, retry in 2000 ms: WebhookDeliveryError('Webhook endpoint responded 500') payment_id=01a10b88-… stage=webhook attempt=1/3
2026-10-05 10:07:28.523 │ SUCCESS  │ payments.consumer.processor:_process:120 │ Payment processed, webhook delivered payment_id=01a10b88-…
```

`format = "json"` в секции `[logging]` включает компактный JSON для сборщиков логов (Loki, ELK): `timestamp`, `level`, `logger`, `message`, контекст и `exception` с трейсбеком. Значения переменных в трейсбеках не выводятся (`diagnose=False`), чтобы в логи не утекали секреты и данные платежей. Запросы healthcheck в access-лог не пишутся.

[loguru]: https://github.com/Delgan/loguru

### Известные компромиссы

- Состояние доставки webhook в БД не хранится (строго по ТЗ). Поэтому при повторной доставке сообщения с уже финализированным платежом webhook отправится ещё раз; получатель дедуплицирует по `webhook-id`.
- Один экземпляр consumer обрабатывает одновременно до `[consumer] prefetch` сообщений (по умолчанию 10). Лимит задаётся prefetch RabbitMQ: ограничивает нагрузку на БД и HTTP-клиент и равномерно делит очередь между репликами. Для роста пропускной способности увеличьте prefetch или масштабируйте consumer репликами: `docker compose up --scale consumer=3`.
- Сообщения из DLQ после устранения причины возвращаются командой `payments dlq requeue`. Сначала сообщение публикуется в `payments.new` с подтверждением брокера, затем удаляется из DLQ: при сбое между шагами возможен дубль (consumer идемпотентен), но не потеря. Счётчик попыток начинается заново: диагностические заголовки DLQ не совпадают с `x-stage` / `x-attempt`.

## Конфигурация

Несекретные параметры хранятся в TOML, по одному файлу на окружение. Нужный файл выбирается переменной `PAYMENTS_CONFIG`:

| Файл | Назначение |
|---|---|
| `config/local.toml` | приложение на хосте, Postgres и RabbitMQ из `docker compose` |
| `config/docker.toml` | всё в `docker compose` (монтируется в контейнеры) |
| `config/production.example.toml` | шаблон для продакшена |

Образ от окружения не зависит: файл монтируется при развёртывании (в Kubernetes — ConfigMap), путь к нему передаётся в `PAYMENTS_CONFIG`.

Порядок приоритета: переменные окружения → TOML → значения по умолчанию из `src/payments/config.py`. В файле достаточно указать то, что отличается от умолчаний. Любой параметр можно точечно переопределить переменной `PAYMENTS__<СЕКЦИЯ>__<КЛЮЧ>`, например `PAYMENTS__RETRY__MAX_ATTEMPTS=5`.

**Секреты только в окружении** (Vault, k8s Secrets, CI). Если секрет окажется в TOML, сервис не запустится — так файлы конфигурации можно спокойно хранить в git и ревьюить:

| Переменная | Описание |
|---|---|
| `PAYMENTS__API__KEY` | Ключ для заголовка `X-API-Key` |
| `PAYMENTS__WEBHOOK__SECRET` | Секрет подписи webhook, `whsec_<base64>` |
| `PAYMENTS__DATABASE__PASSWORD` | Пароль PostgreSQL |
| `PAYMENTS__RABBITMQ__PASSWORD` | Пароль RabbitMQ |

Неизвестный ключ в TOML (например, опечатка `max_atempts`) тоже останавливает старт, а не игнорируется молча. Миграциям нужны только `[database]` и пароль БД, секреты API и webhook для них не требуются.

| Секция | Ключи (по умолчанию) |
|---|---|
| `[database]` | `host` (`localhost`), `port` (`5432`), `name` (`payments`), `user` (`payments`) |
| `[rabbitmq]` | `host` (`localhost`), `port` (`5672`), `vhost` (`/`), `user` (`guest`) |
| `[outbox]` | `poll_interval` (`1.0` с), `batch_size` (`100`), `max_backoff` (`30.0` с) |
| `[consumer]` | `prefetch` (`10`) — сообщений, обрабатываемых одним экземпляром одновременно |
| `[retry]` | `max_attempts` (`3`, включая первую), `base_delay` (`2.0` с; задержки `base · 2ⁿ⁻¹`) |
| `[gateway]` | `min_delay` / `max_delay` (`2.0` / `5.0` с), `success_rate` / `decline_rate` (`0.90` / `0.07`, остаток — технические сбои) |
| `[webhook]` | `timeout` (`10.0` с) |
| `[logging]` | `level` (`INFO`), `format` (`pretty` или `json`) |

## Разработка

Основные действия собраны в `Makefile` (`make` без аргументов покажет список):

| Команда | Что делает |
|---|---|
| `make install` | зависимости из `uv.lock`, включая dev |
| `make format` | форматирование (ruff format) и безопасные автоисправления (ruff check --fix) |
| `make lint` | проверка стиля и форматирования без изменения файлов |
| `make typecheck` | mypy (strict) и pyright — тот же анализ, что видит IDE |
| `make check` | `lint` + `typecheck` |
| `make test-unit` | быстрые unit-тесты, без Docker |
| `make test-integration` | интеграционные тесты (нужен Docker) |
| `make test` | все тесты |
| `make ci` | всё, что должно проходить в CI: `check` + `test` |
| `make local-*` | локальный стек в Docker, см. «Быстрый старт» |

Аргументы pytest передаются через `PYTEST_ARGS`, например `make test PYTEST_ARGS="-k idempotency -x"`.

Интеграционные тесты (`tests/integration`) поднимают через testcontainers настоящие PostgreSQL и RabbitMQ, а также контейнер-получатель webhook (`tests/integration/webhook_receiver`). Получатель независимо проверяет подпись и умеет отвечать `500` на первые N запросов. Проверяются:
- полный цикл платежа;
- бизнес-отказ;
- повторы webhook с экспоненциальной задержкой;
- попадание в DLQ при недоступном получателе и при сбоях шлюза;
- обработка событий, опубликованных, пока consumer был остановлен;
- идемпотентность, в том числе конкурентные запросы с одним ключом;
- аутентификация;
- соответствие миграций моделям;
- команды CLI: `db current`, `dlq list`, `dlq requeue` (в том числе фильтр по платежу и подтверждение), `outbox cleanup`;
- событие, которое брокер не смог никуда доставить, остаётся в outbox;
- очистка outbox удаляет только старые опубликованные события, пачками;
- `/health` consumer и prefetch, который реально видит RabbitMQ (через management API).

Локальный запуск приложения на хосте (Postgres и RabbitMQ — из compose):

```bash
docker compose up -d --wait postgres rabbitmq
export PAYMENTS_CONFIG=config/local.toml \
       PAYMENTS__API__KEY=dev-api-key \
       PAYMENTS__WEBHOOK__SECRET=whsec_MRVnyabDOXIn1GLmVyP2VeXwy/Ts+AwO \
       PAYMENTS__DATABASE__PASSWORD=payments \
       PAYMENTS__RABBITMQ__PASSWORD=payments
uv run payments config check
uv run payments db upgrade
uv run payments api --reload      # терминал 1
uv run payments consumer          # терминал 2, с теми же переменными
```

## Структура

```
src/payments/
├── api/            # FastAPI: приложение, роуты, зависимости (auth, сервисы)
├── cli/            # Typer: команда payments (api, consumer, db, config, dlq)
├── consumer/       # FastStream consumer: processor (этапы и retry), эмулятор шлюза, webhook
├── db/             # SQLAlchemy-модели, репозитории, Unit of Work, конфигурация Alembic
├── messaging/      # топология RabbitMQ, политика повторов, outbox relay, работа с DLQ
├── migrations/     # миграции Alembic (поставляются вместе с пакетом)
├── services.py     # создание/получение платежа, идемпотентность
├── schemas.py      # контракты: HTTP, сообщения брокера, webhook
├── domain.py       # перечисления и доменные ошибки
├── logging_config.py  # loguru: цветной вывод / JSON, перехват стандартного logging
├── config.py       # настройки: TOML + секреты из окружения
└── containers.py   # DI-контейнеры: сборка зависимостей и ресурсов
config/             # TOML-конфигурация по окружениям
scripts/smoke.py    # smoke-тест запущенного стека (make local-smoke)
tests/{unit,integration}
```
