# order-service

HTTP-сервис заказов с асинхронной выгрузкой во внешнюю **ERP**.

Принимает заказы, хранит их в PostgreSQL и в той же транзакции кладёт задание в очередь `integration_jobs`.
Задания забирает отдельный процесс-воркер по gRPC, отправляет заказ в ERP и записывает результат обратно.
События `order.created` публикуются в Kafka через outbox, зависшие задания возвращает reaper, сверху — метрики Prometheus и трейсы OpenTelemetry.

## Возможности

- Заказы: создание с позициями, чтение, список по клиенту и статусу, `confirm` / `retry` / `cancel`.
- Типы заказов `regular` и `preorder` (у предзаказа обязательна `delivery_date`).
- Цены берутся из каталога по gRPC в момент создания, в заказ копируется `price_at_order`.
- Очередь `integration_jobs` на `FOR UPDATE SKIP LOCKED`: один заказ — одно задание (`idempotency_key`).
- Внешний воркер через gRPC `ClaimJob` / `CompleteJob` с `lease_token` и `locked_at`.
- Идемпотентность в ERP по `Idempotency-Key`: `409` считается успехом, номер заказа сохраняется.
- Классификация ошибок ERP: временная ошибка возвращает задание в очередь, перманентная — в `failed`.
- Reaper возвращает в очередь задания, зависшие в `in_progress`.
- Лимитер запросов в ERP на Redis (фиксированное окно) и кэш цен в Redis.
- Outbox в Kafka: `published_at` + publish lease, поэтому реплики API не публикуют одну строку дважды.
- Метрики Prometheus и трейсы OpenTelemetry, один trace на заказ.
- Helm-чарт: `Deployment` api и worker, probes, по 2 реплики.

## Архитектура

![Архитектура order-service](docs/architecture.png)

Исходник схемы — `docs/architecture.svg`, PNG — для GitHub и просмотра без SVG-рендера.

Слои:

```
cmd/api                 — HTTP-сервер, gRPC JobService, publisher, reaper
cmd/worker              — внешний воркер: ClaimJob → ERP → CompleteJob
cmd/catalogfake         — заглушка каталога (gRPC + HTTP, меняющиеся цены)
cmd/erpfake             — заглушка ERP (HTTP, режимы сбоев и задержек)
internal/handler        — chi-роуты, JSON, HTTP-коды
internal/service        — валидация, транзакции, классификация ответов ERP
internal/repository     — SQL через sqlx/pgx, очередь и outbox
internal/jobs           — gRPC-реализация ClaimJob/CompleteJob
internal/worker         — цикл воркера, бюджет времени на shutdown
internal/catalog        — gRPC-клиент каталога и кэш цен
internal/erp            — HTTP-клиент ERP
internal/publisher      — outbox → Kafka
internal/limiter        — лимитер на Redis
internal/metrics        — метрики Prometheus и gRPC-интерсептор
internal/telemetry      — OpenTelemetry, интерсепторы, проброс trace context
internal/model          — доменные структуры
migrations              — схема PostgreSQL
deploy/helm/orders      — чарт: Deployment api + worker, probes
```

## Быстрый старт

### Весь стек в Docker

```bash
make up
# сборка четырёх образов из одного Dockerfile (ARG BINARY) + миграции
curl -sS http://localhost:8080/healthz
# ok
```

Поднимаются:

| Сервис | Порт | Назначение |
|---|---|---|
| `api` | 8080, 9090 | HTTP API и gRPC JobService |
| `worker` | 8082 | `/healthz`, `/metrics`, pprof |
| `catalogfake` | 9091, 8084 | gRPC-каталог и HTTP-управление ценами |
| `erpfake` | 8081 | заглушка ERP |
| `jaeger` | 16686, 4318 | UI трейсов и OTLP/HTTP |
| `postgres` | 5432 | заказы, позиции, очередь |
| `redis` | 6379 | кэш цен, окно лимитера |
| `redpanda` | 19092 | Kafka для событий |

`make up` собирает образы **по одному** (`COMPOSE_PARALLEL_LIMIT=1`): параллельная сборка четырёх Go-бинарников на ноутбуке съедает всю память.

### Локально

```bash
docker compose up -d postgres redis redpanda jaeger
make migrate-up

go run ./cmd/catalogfake &
go run ./cmd/erpfake &
go run ./cmd/api &
go run ./cmd/worker &
```

`api` и `worker` берут настройки из окружения (значения по умолчанию рассчитаны на этот сценарий):

```bash
export DATABASE_URL='postgres://orders:orders@localhost:5432/orders?sslmode=disable'
export CATALOG_GRPC_ADDR='localhost:9091'
export REDIS_ADDR='localhost:6379'
export ERP_BASE_URL='http://localhost:8081'
export KAFKA_ADDR='localhost:19092'
export OTEL_EXPORTER_OTLP_ENDPOINT='http://localhost:4318'
export JOBS_ADDR='localhost:9090'
```

## Переменные окружения

API:

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `DATABASE_URL` | `postgres://orders:orders@localhost:5432/orders?sslmode=disable` | DSN PostgreSQL |
| `HTTP_ADDR` | `:8080` | адрес HTTP-сервера |
| `GRPC_ADDR` | `:9090` | адрес gRPC JobService |
| `CATALOG_GRPC_ADDR` | `localhost:9091` | каталог цен |
| `REDIS_ADDR` | `localhost:6379` | кэш цен и лимитер |
| `PRICE_CACHE_TTL` | `60s` | время жизни цены в кэше |
| `ERP_BASE_URL` | `http://localhost:8081` | база внешней ERP |
| `ERP_TIMEOUT` | `15s` | таймаут одного вызова ERP |
| `ERP_LIMIT` | `0` | вызовов в окне, `0` — лимитер выключен |
| `ERP_WINDOW` | `1s` | длина окна лимитера |
| `REAPER_AFTER` | `30s` | порог «зависшего» задания, должен быть больше `ERP_TIMEOUT` |
| `REAPER_EVERY` | `10s` | как часто запускается reaper |
| `POLL_INTERVAL` | `1s` | период публикации outbox |
| `POLL_IDLE` | `1s` | пауза ClaimJob, когда очередь пуста и нет `retry_after` |
| `KAFKA_ADDR` | `localhost:19092` | брокеры через запятую |
| `KAFKA_TOPIC` | `order.created` | топик событий |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` | OTLP/HTTP коллектор, пусто — трейсы выключены |

Воркер:

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `JOBS_ADDR` | `localhost:9090` | адрес gRPC JobService |
| `WORKER_ID` | `hostname-pid` | идентификатор воркера в очереди |
| `WORKER_PPROF_ADDR` | `:8082` | служебный HTTP: `/healthz`, `/metrics`, pprof |
| `RPC_DEADLINE` | `5s` | таймаут вызова `ClaimJob` |
| `SHUTDOWN_TIMEOUT` | `20s` | бюджет на завершение после SIGTERM |
| `POLL_IDLE` | `1s` | пауза между пустыми захватами |
| `ERP_BASE_URL` | `http://localhost:8081` | база внешней ERP |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` | OTLP/HTTP коллектор |

## API

| Метод | Путь | Описание |
|---|---|---|
| `GET` | `/healthz` | проверка живости |
| `GET` | `/metrics` | метрики Prometheus |
| `POST` | `/api/v1/orders` | создать заказ |
| `GET` | `/api/v1/orders` | список по `customer_id` и `status` (оба обязательны) |
| `GET` | `/api/v1/orders/{id}` | получить заказ, `404` — если нет |
| `POST` | `/api/v1/orders/{id}/confirm` | подтвердить и поставить в очередь (`202`) |
| `POST` | `/api/v1/orders/{id}/retry` | вернуть `failed` в очередь |
| `POST` | `/api/v1/orders/{id}/cancel` | отменить из `created` или `confirmed` |

```bash
curl -sS -X POST localhost:8080/api/v1/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer_id":1,"items":[{"product_id":1,"qty":2}]}'
# {"id":12,"customer_id":1,"status":"created","order_type":"regular","delivery_date":null,
#  "created_at":"...","items":[{"product_id":"1","qty":2,"price_at_order":"100.00"}]}

curl -sS -X POST localhost:8080/api/v1/orders/12/confirm
# {"status":"confirmed"}
```

Статусы заказа:

| Статус | Значение |
|---|---|
| `created` | создан, задание ещё не поставлено |
| `confirmed` | подтверждён, задание в очереди |
| `submitted_to_erp` | ERP принял заказ, номер в `erp_order_id` |
| `failed` | перманентная ошибка ERP, нужен `retry` после исправления данных |
| `cancelled` | отменён |

## Как работает интеграция

1. `POST /api/v1/orders` в одной транзакции вставляет заказ, позиции и (после `confirm`) задание в `integration_jobs`.
2. Цена каждого товара спрашивается в каталоге при создании; в заказ пишется `price_at_order`.
3. `confirm` переводит заказ в `confirmed` с уникальным `idempotency_key` и `correlation_id` и сразу отправляет заявку в ERP.
   Если ERP ответила временной ошибкой, задание остаётся в очереди.
4. Воркер захватывает задание:

   ```sql
   UPDATE integration_jobs
      SET status = 'in_progress', locked_at = now(), lease_token = gen_random_uuid()
    WHERE id = (SELECT id FROM integration_jobs
                 WHERE status = 'queued' AND run_at <= now()
                 ORDER BY run_at
                 FOR UPDATE SKIP LOCKED
                 LIMIT 1)
   RETURNING id, order_id, idempotency_key, locked_at, lease_token, correlation_id;
   ```

   Лок коммитится до вызова ERP: второй воркер это задание уже не возьмёт.
5. `POST /orders` в ERP с `Idempotency-Key` и `X-Correlation-ID`. Результат возвращается через `CompleteJob`, который проверяет `lease_token` и `locked_at`.

Классификация ответов ERP:

| Ответ / ошибка | Что делает сервис |
|---|---|
| `2xx`, `409` (дубль по ключу) | заказ `submitted_to_erp`, задание `done` |
| `400`, `422` | заказ и задание `failed`, повтора не будет |
| `408`, `429`, таймаут, сеть, `5xx` | задание снова `queued`, `run_at = now() + 30s` |

Публикация событий:

- Publisher в каждом экземпляре API берёт строки с `published_at IS NULL` и `publish_locked_at` старше 60 секунд, ставит `publish_locked_at = now()` и только после ack брокера пишет `published_at`.
- Доставка at-least-once: повтор возможен, если процесс упал между записью в Kafka и коммитом `published_at`; потребитель дедуплицирует по `job_id`.
- Отменённые задания не публикуются.

Reaper раз в `REAPER_EVERY` возвращает в `queued` задания, у которых `status = 'in_progress'` и `locked_at` старше `REAPER_AFTER`; `run_at` ставится в `now()`, без backoff.

## Наблюдаемость

Метрики:

| Метрика | Где | Смысл |
|---|---|---|
| `grpc_server_handled_total{method,code}` | api | сколько gRPC-вызовов обработано, по методу и коду статуса |
| `grpc_client_roundtrip_latency{method}` | worker | гистограмма полного клиентского вызова |
| `erp_submit_errors_total{order_type,error_type}` | api | ошибки отправки в ERP: `permanent` / `temporary` |
| `erp_payload_prepare_errors_total{order_type,error_type}` | api | тело не прошло валидацию, до ERP не дошло |

Трейсы: один trace на заказ, три span'а с общим `correlation_id`.

| Span | Сервис | Что показывает |
|---|---|---|
| `ClaimJob` | worker | задание захвачено, теги `job.id`, `order.id` |
| `erp.Submit` | worker | ответ ERP, тег `http.status_code` |
| `CompleteJob` | worker + api | результат записан, проверка lease |

UI Jaeger — `http://localhost:16686`, поиск по сервису `order-service-worker` или по тегу `correlation_id`.

## Тесты и линт

```bash
docker compose up -d postgres redis   # тестам нужны PostgreSQL и Redis
go test ./...
make lint                             # golangci-lint v1.64.8, как в CI
```

Линтер настроен в `.golangci.yml`: `govet`, `errcheck`, `staticcheck`. CI — две стадии, `test` и `lint`.

Тесты работают с той же базой, что и локальный стек, поэтому перед `go test ./...` не держите запущенными `api` и `worker`: их фоновые горутины забирают задания и публикуют строки outbox, а тесты ждут их в `queued`.

## Ограничения

- Нет аутентификации и проверки, что заказ принадлежит вызывающему клиенту.
- У заданий нет счётчика попыток: при постоянной временной ошибке повтор будет каждые 30 секунд.
- Лимитер — фиксированное окно: на границе окон возможен всплеск до `2 × ERP_LIMIT`.
- `/metrics` и `/debug/pprof` не защищены и должны быть доступны только изнутри сети.
- У воркера нет своей БД: единственный источник правды — очередь, поэтому вся логика восстановления живёт в `CompleteJob`, `lease_token` и reaper.
- Событие публикуется только на `order.created`; отмена и смена статуса отдельными событиями не публикуются.
