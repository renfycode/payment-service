# Payments Service

Микросервис асинхронной обработки платежей. Принимает запрос на оплату, проводит его через эмулятор платёжного шлюза и уведомляет клиента о результате через webhook.

**Стек:** Python 3.13, FastAPI + Pydantic v2, SQLAlchemy 2.0 (async) + asyncpg, PostgreSQL 17, RabbitMQ 4 (FastStream), Alembic, Docker Compose, uv.

## Быстрый старт

```bash
cp .env.example .env          # замените API_KEY и WEBHOOK_SECRET
docker compose up -d --build --wait
```

| Сервис | Адрес |
|---|---|
| API | http://localhost:8000 |
| Swagger UI | http://localhost:8000/docs |
| RabbitMQ Management | http://localhost:15672 (логин/пароль из `.env`, по умолчанию `payments` / `payments`) |

Миграции применяются автоматически при старте контейнера `api`.

## Примеры использования

Все эндпоинты требуют заголовок `X-API-Key`. В качестве `webhook_url` удобно взять адрес с https://webhook.site.

### Создание платежа

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: change-me-api-key" \
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
  -H "X-API-Key: change-me-api-key"
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

**Outbox pattern.** Платёж и событие `payment.created` пишутся в одной транзакции. Фоновая задача в процессе `api` раз в секунду забирает неопубликованные записи (`SELECT … FOR UPDATE SKIP LOCKED`, поэтому безопасно при нескольких репликах). Она публикует их с publisher confirms и только после подтверждения брокера проставляет `published_at`. Гарантия — at-least-once.

**Идемпотентность API.** `Idempotency-Key` уникален в таблице `payments`, рядом хранится SHA-256 нормализованного тела запроса. Тот же ключ с тем же телом возвращает исходный платёж, с другим телом — `409`. Гонку одновременных запросов с одним ключом закрывает уникальный индекс: проигравший запрос ловит `IntegrityError` и возвращает победивший платёж.

**Идемпотентность consumer.** Сообщение содержит только `payment_id`, источник правды — БД. Этап определяется по статусу: если платёж уже финализирован, шлюз повторно не вызывается. Статус меняется условным `UPDATE … WHERE status = 'pending'`, поэтому конкурентные обработчики не перезапишут результат друг друга.

**Эмуляция шлюза.** Задержка 2–5 с. Исходы:
- 90% — успех (`succeeded`);
- 7% — бизнес-отказ (`failed`, webhook отправляется, повторов нет);
- 3% — технический сбой (повтор).

Доли настраиваются.

**Retry.** 3 попытки на этап, то есть первая и 2 повтора с экспоненциальной задержкой `RETRY_BASE_DELAY · 2ⁿ⁻¹` (2 с и 4 с по умолчанию). Повторы реализованы очередями-задержками с TTL и dead-letter обратно в `payments.new`: consumer не блокируется на ожидании, повторы переживают его рестарт. Под каждую задержку своя очередь — в одной очереди с разными TTL сообщения истекают только из головы.

У этапов «шлюз» и «webhook» независимые счётчики (заголовки `x-stage`, `x-attempt`): сбои шлюза не съедают попытки доставки webhook.

**Dead Letter Queue.** Если попытки этапа исчерпаны, consumer публикует сообщение в `payments.new.dlq` с заголовками `x-dead-letter-stage`, `x-dead-letter-attempts`, `x-dead-letter-reason`.
- Если исчерпаны попытки шлюза, платёж остаётся `pending`: результат на стороне шлюза неизвестен, честнее разобраться вручную, чем объявить его `failed`.
- Если не удалось доставить webhook, статус платежа не меняется.

Сообщения, которые не удалось даже разобрать, отклоняются (`reject`) и попадают в DLQ через DLX очереди. Все очереди — quorum с dead-lettering в режиме at-least-once.

**Аутентификация.** Статический ключ в `X-API-Key`, сравнение за постоянное время (`secrets.compare_digest`).

### Известные компромиссы

- Состояние доставки webhook в БД не хранится (строго по ТЗ). Поэтому при повторной доставке сообщения с уже финализированным платежом webhook отправится ещё раз; получатель дедуплицирует по `webhook-id`.
- Consumer обрабатывает сообщения последовательно. Для роста пропускной способности масштабируйте его репликами: `docker compose up --scale consumer=3`.
- Чтобы переотправить сообщения из DLQ после устранения причины, включите shovel (`docker compose exec rabbitmq rabbitmq-plugins enable rabbitmq_shovel rabbitmq_shovel_management`) и в RabbitMQ Management → Queues → `payments.new.dlq` выполните *Move messages* в `payments.new`. Счётчик попыток у таких сообщений начинается заново: диагностические заголовки DLQ не совпадают с `x-stage` / `x-attempt`.

## Конфигурация

Переменные окружения (полный список — `src/payments/config.py`):

| Переменная | По умолчанию | Описание |
|---|---|---|
| `API_KEY` | — (обязательна) | Ключ для `X-API-Key` |
| `WEBHOOK_SECRET` | — (обязательна) | Секрет подписи, `whsec_<base64>` |
| `DATABASE_URL` | `postgresql+asyncpg://payments:payments@localhost:5432/payments` | |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | |
| `MAX_ATTEMPTS` | `3` | Попыток на этап, включая первую |
| `RETRY_BASE_DELAY` | `2.0` | Секунды; задержки `base · 2ⁿ⁻¹` |
| `GATEWAY_MIN_DELAY` / `GATEWAY_MAX_DELAY` | `2.0` / `5.0` | Задержка эмулятора, секунды |
| `GATEWAY_SUCCESS_RATE` / `GATEWAY_DECLINE_RATE` | `0.90` / `0.07` | Остаток — технические сбои |
| `WEBHOOK_TIMEOUT` | `10.0` | Таймаут запроса webhook, секунды |
| `OUTBOX_POLL_INTERVAL` / `OUTBOX_BATCH_SIZE` | `1.0` / `100` | |
| `LOG_LEVEL` | `INFO` | |

## Разработка

```bash
uv sync                               # зависимости, включая dev
uv run ruff check . && uv run ruff format --check .
uv run mypy src tests                 # strict
uv run pytest tests/unit              # быстрые unit-тесты
uv run pytest                         # всё, включая интеграционные (нужен Docker)
```

Интеграционные тесты (`tests/integration`) поднимают через testcontainers настоящие PostgreSQL и RabbitMQ, а также контейнер-получатель webhook (`tests/integration/webhook_receiver`). Получатель независимо проверяет подпись и умеет отвечать `500` на первые N запросов. Проверяются:
- полный цикл платежа;
- бизнес-отказ;
- повторы webhook с экспоненциальной задержкой;
- попадание в DLQ при недоступном получателе и при сбоях шлюза;
- обработка событий, опубликованных, пока consumer был остановлен;
- идемпотентность, в том числе конкурентные запросы с одним ключом;
- аутентификация;
- соответствие миграций моделям.

Локальный запуск без Docker для приложения (Postgres и RabbitMQ должны быть доступны):

```bash
export API_KEY=dev WEBHOOK_SECRET=whsec_MRVnyabDOXIn1GLmVyP2VeXwy/Ts+AwO DATABASE_URL=...
uv run alembic upgrade head
uv run uvicorn payments.api.app:create_app --factory --reload
uv run python -m payments.consumer
```

## Структура

```
src/payments/
├── api/            # FastAPI: приложение, роуты, зависимости (auth, сервисы)
├── consumer/       # FastStream consumer: processor (этапы и retry), эмулятор шлюза, webhook
├── db/             # SQLAlchemy-модели, репозитории, Unit of Work
├── messaging/      # топология RabbitMQ, политика повторов, outbox relay
├── services.py     # создание/получение платежа, идемпотентность
├── schemas.py      # контракты: HTTP, сообщения брокера, webhook
├── domain.py       # перечисления и доменные ошибки
└── config.py       # настройки из окружения
migrations/         # Alembic
tests/{unit,integration}
```
