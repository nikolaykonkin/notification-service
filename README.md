# notification-service

[![CI](https://github.com/nikolaykonkin/notification-service/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/nikolaykonkin/notification-service/actions/workflows/ci.yml)

Сервис уведомлений на Go (в разработке). Целевая архитектура: принимает события по gRPC, публикует их в Kafka, обрабатывает асинхронно, кеширует статусы в Redis и хранит историю в PostgreSQL. Ограничивает число уведомлений на пользователя (rate limiting). Пет-проект: делаю для себя, чтобы на практике разобраться с gRPC, Kafka и Redis.

## Статус

На текущий момент готово:

- gRPC-контракт `NotificationService` с унарными методами `SendNotification` и `GetNotification`, кодогенерация через `make proto`.
- Конфигурация из переменных окружения. Все ошибки собираются через `errors.Join`, сервис не стартует, если обязательная переменная не задана или некорректна.
- PostgreSQL: миграция `migrations/001_init.sql` (UUID, JSONB, CHECK на `type` и `status`, индексы) и репозиторий `internal/storage` с методами `Create`, `GetByID`, `UpdateStatus`.
- Redis: кеш уведомлений по id с TTL (`internal/cache`) и rate limiting по `user_id` (`internal/ratelimit`, fixed window на `INCR` + `EXPIRE NX` в одной транзакции).
- Kafka producer (`internal/kafka`): публикует событие в топик `notifications.email` или `notifications.push` по типу уведомления. Ключ сообщения `user_id`, поэтому события одного пользователя идут в одну партицию и сохраняют порядок. Запись синхронная, подтверждается всеми репликами.
- Kafka consumer (`internal/kafka`): читает оба топика в составе consumer group. Гарантия доставки at-least-once: offset фиксируется после обработки, поэтому при сбое событие может прийти повторно. Некорректные сообщения логируются и пропускаются. Если обработчик вернул ошибку, consumer останавливается без фиксации offset.
- Проверка доступности Kafka на старте (`kafka.Ping`): TCP-подключение хотя бы к одному брокеру.
- Обработка события (`internal/processor`): эмуляция отправки (пауза), запись статуса `sent` или `failed` в PostgreSQL и удаление записи из кеша. Ошибка отправки сохраняется как статус `failed`, сбой кеша только логируется. Результат передается в метрики через интерфейс `Recorder`.
- gRPC-сервер (`internal/api`): `SendNotification` проверяет запрос и лимит (при сбое Redis пропускает запрос), сохраняет уведомление со статусом `pending` в PostgreSQL и публикует событие в Kafka; если публикация не удалась, запись помечается `failed`. `GetNotification` читает из кеша, а при промахе или сбое кеша из PostgreSQL. Внутренние ошибки клиенту не раскрываются. Покрыт тестами через bufconn.
- Метрики Prometheus (`internal/metrics`): отдельный реестр, счетчики `notifications_sent_total` и `notifications_failed_total`, гистограмма `notification_duration_seconds` (метка `type`: `email`, `push` или `unknown`), стандартные метрики Go и процесса.
- HTTP-сервер (`internal/httpserver`): `GET /metrics` для Prometheus и `GET /health`, который проверяет PostgreSQL и Redis (общий таймаут 2 секунды) и отвечает 200 или 503 без раскрытия текста ошибок. Заданы таймауты чтения и записи.
- Сборка и запуск (`cmd/notification-service`): зависимости собираются через конструкторы, при старте проверяются PostgreSQL, Redis и Kafka (таймаут 15 секунд). Graceful shutdown по SIGINT и SIGTERM: сначала останавливаются HTTP-сервер (до 2 секунд) и gRPC-сервер (до 5 секунд), затем consumer, producer, Redis и PostgreSQL. Если consumer или один из серверов падают сами, процесс завершается с кодом 1.
- Docker Compose с PostgreSQL 17, Redis 7 и Kafka 3.9 в режиме KRaft. Одноразовый контейнер `kafka-init` создает топики `notifications.email` и `notifications.push`.
- CI на GitHub Actions: `go vet`, `go test -race`, `go build`.

Что это значит на практике: сервис запускается командой `make run` вместе с локальной инфраструктурой, принимает gRPC-вызовы на порту `GRPC_PORT` и отдает `/metrics` и `/health` на порту `HTTP_PORT`. Prometheus и Grafana в Docker Compose пока не подняты, метрики можно посмотреть через `curl`. Эмуляция отправки не завершается ошибкой, поэтому `notifications_failed_total` в ручной проверке остается нулем. Docker-образа сервиса нет. Включен gRPC reflection для удобства `grpcurl`. Все остальное перечислено в разделе Roadmap.

## Архитектура

Сервис это один процесс: gRPC-сервер, HTTP-сервер (`/metrics`, `/health`), producer и consumer живут в нем вместе (пакеты `internal/api`, `internal/httpserver`, `internal/kafka`). Consumer только читает сообщения и передает их обработчику, а сама обработка вынесена в `internal/processor`. Kafka, Redis и PostgreSQL внешние системы, а Kafka здесь только транспорт между producer и consumer одного и того же сервиса.

```
Отправка:

клиент
  |
  | gRPC SendNotification
  v
+------- notification-service (один процесс) --------+
|                                                    |
|  gRPC-сервер ---- проверка лимита -----------------|--> Redis
|       |                                            |
|       v                                            |
|  producer -----------------------------------------|--> Kafka
|                                                    |    notifications.email
|                                                    |    notifications.push
|  consumer <----------------------------------------|--- Kafka
|       |                                            |
|       +---- статус sent/failed --------------------|--> PostgreSQL
|       +---- обновление кеша -----------------------|--> Redis
|                                                    |
+----------------------------------------------------+

Чтение:

  клиент --gRPC GetNotification--> notification-service --> Redis (кеш, TTL 5 минут)
                                                                | промах
                                                                v
                                                           PostgreSQL
```

## Стек

Используется сейчас:

- Go 1.26
- gRPC, Protocol Buffers (`protoc`, `protoc-gen-go`, `protoc-gen-go-grpc`)
- PostgreSQL 17, драйвер pgx
- Redis 7, клиент go-redis
- Kafka 3.9 (KRaft, без ZooKeeper), клиент kafka-go
- Prometheus client_golang (метрики)
- Docker, Docker Compose
- GitHub Actions

Планируется: Prometheus (сервер), Grafana, Dockerfile сервиса, публикация образа в ghcr.io.

## Структура проекта

```
.
├── cmd/notification-service/   точка входа: сборка зависимостей, запуск, graceful shutdown (готово)
├── internal/
│   ├── api/                    gRPC-сервер (готово)
│   ├── cache/                  кеш уведомлений в Redis (готово)
│   ├── config/                 конфигурация из env (готово)
│   ├── httpserver/             HTTP: /metrics и /health (готово)
│   ├── kafka/                  producer, consumer, проверка доступности (готово)
│   ├── metrics/                метрики Prometheus (готово)
│   ├── processor/              обработка события: отправка, статус, сброс кеша (готово)
│   ├── ratelimit/              rate limiting на Redis (готово)
│   └── storage/                репозиторий PostgreSQL (готово)
├── migrations/                 001_init.sql
├── proto/notification/v1/      контракт и сгенерированный код
├── .github/workflows/ci.yml    CI
├── docker-compose.yml          PostgreSQL, Redis, Kafka
├── Makefile
└── .env.example
```

Появится позже: `Dockerfile`, конфигурация Prometheus и дашборд Grafana в Docker Compose, workflow публикации образа.

## Быстрый старт

Нужны Go 1.26, Docker и make. Сгенерированный protobuf-код лежит в репозитории, поэтому `protoc` нужен только для `make proto`.

Поднять инфраструктуру:

```bash
cp .env.example .env
make docker-up
docker compose ps -a
```

Через 30-60 секунд `postgres`, `redis` и `kafka` должны быть в статусе `healthy`, а `kafka-init` в статусе `Exited (0)`: он создал топики и завершился.

Миграция из `migrations/` выполняется автоматически, но только при первом запуске на пустой базе. Чтобы применить ее заново, пересоздайте том: `docker compose down -v`, затем `make docker-up`.

Запустить сервис (останавливается по Ctrl+C):

```bash
make run
```

Вызвать сервис через [grpcurl](https://github.com/fullstorydev/grpcurl) (в другом терминале):

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -d '{"user_id": 42, "type": "NOTIFICATION_TYPE_EMAIL", "payload": "{\"to\":\"user@example.com\"}"}' \
  localhost:50051 notification.v1.NotificationService/SendNotification
grpcurl -plaintext -d '{"id": "<id из ответа>"}' \
  localhost:50051 notification.v1.NotificationService/GetNotification
```

Сразу после отправки статус `NOTIFICATION_STATUS_PENDING`, примерно через секунду `NOTIFICATION_STATUS_SENT`. Лимит по умолчанию 10 уведомлений в час на пользователя (`RATE_LIMIT_PER_HOUR`), сверх лимита сервис отвечает `ResourceExhausted`.

Проверить состояние и метрики:

```bash
curl -s localhost:8080/health
curl -s localhost:8080/metrics | grep notification
```

`/health` отвечает 200 и `{"status":"ok",...}`, если PostgreSQL и Redis отвечают, иначе 503.

Запустить тесты:

```bash
make vet
make test
```

Тесты PostgreSQL, Redis и Kafka без соответствующих переменных окружения пропускаются (`SKIP`). Чтобы выполнить их с настоящими сервисами:

```bash
TEST_DATABASE_URL="postgres://notification:notification@localhost:5432/notification?sslmode=disable" \
TEST_REDIS_URL="redis://localhost:6379/0" \
TEST_KAFKA_BROKERS="localhost:9092" \
make test-race
```

Тесты Kafka идут заметно дольше остальных: consumer ждет вступления в группу. Топики общие, поэтому каждый тест ищет в них сообщения со своим уникальным id.

Остановить инфраструктуру:

```bash
make docker-down
```

## CI

Workflow `.github/workflows/ci.yml` запускается на push в `main` и на pull request: `go vet`, `go test -race`, `go build`. Тесты PostgreSQL, Redis и Kafka в CI пока пропускаются, потому что сервисы в workflow не подняты. Интеграционные тесты с реальными PostgreSQL, Redis и Kafka появятся в CI позже (см. Roadmap).

## Roadmap

- Dockerfile сервиса, Prometheus и Grafana в Docker Compose
- Интеграционные тесты
- CD: сборка Docker-образа в CI и публикация в ghcr.io
- Полная версия README
- Финальная проверка: `make vet`, `make test-race`, `docker compose up`, smoke-тест через grpcurl