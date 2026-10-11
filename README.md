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
- Обработка события (`internal/processor`): эмуляция отправки (пауза), запись статуса `sent` или `failed` в PostgreSQL и удаление записи из кеша. Ошибка отправки сохраняется как статус `failed`, сбой кеша только логируется.
- Docker Compose с PostgreSQL 17, Redis 7 и Kafka 3.9 в режиме KRaft. Одноразовый контейнер `kafka-init` создает топики `notifications.email` и `notifications.push`.
- CI на GitHub Actions: `go vet`, `go test -race`, `go build`.

Что это значит на практике: сервис пока не запускается как приложение. `main.go` только загружает конфигурацию, пишет лог и завершается. Producer, consumer и процессор написаны и покрыты тестами, но не соединены с gRPC-сервером и не запускаются из `main.go`. Работают и проверяются тестами отдельные пакеты и локальная инфраструктура. Все остальное перечислено в разделе Roadmap.

## Архитектура

Целевая схема (в коде сейчас есть только отдельные пакеты `config`, `storage`, `cache`, `ratelimit`, `kafka` и `processor`; между собой они не соединены, gRPC-сервера и сборки в `main.go` еще нет).

Сервис это один процесс: gRPC-сервер, producer и consumer живут в нем вместе (пакеты `internal/api` и `internal/kafka`). Consumer только читает сообщения и передает их обработчику, а сама обработка вынесена в `internal/processor`. Kafka, Redis и PostgreSQL внешние системы, а Kafka здесь только транспорт между producer и consumer одного и того же сервиса.

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
- Docker, Docker Compose
- GitHub Actions

Планируется: Prometheus, Grafana, Dockerfile сервиса, публикация образа в ghcr.io.

## Структура проекта

```
.
├── cmd/notification-service/   точка входа (пока только загрузка конфигурации)
├── internal/
│   ├── api/                    gRPC-сервер (пока только doc.go)
│   ├── cache/                  кеш уведомлений в Redis (готово)
│   ├── config/                 конфигурация из env (готово)
│   ├── kafka/                  producer и consumer (готово)
│   ├── metrics/                метрики Prometheus (пока только doc.go)
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

Проверить загрузку конфигурации (сервис загрузит настройки из `.env`, запишет лог и завершится):

```bash
make run
```

Остановить инфраструктуру:

```bash
make docker-down
```

## CI

Workflow `.github/workflows/ci.yml` запускается на push в `main` и на pull request: `go vet`, `go test -race`, `go build`. Тесты PostgreSQL, Redis и Kafka в CI пока пропускаются, потому что сервисы в workflow не подняты. Интеграционные тесты с реальными PostgreSQL, Redis и Kafka появятся в CI позже (см. Roadmap).

## Roadmap

- gRPC-сервер (`internal/api`), тесты через bufconn
- `main.go`: сборка зависимостей и graceful shutdown
- Метрики Prometheus, `/metrics` и `/health`
- Dockerfile сервиса, Prometheus и Grafana в Docker Compose
- Интеграционные тесты
- CD: сборка Docker-образа в CI и публикация в ghcr.io
- Полная версия README
- Финальная проверка: `make vet`, `make test-race`, `docker compose up`, smoke-тест через grpcurl