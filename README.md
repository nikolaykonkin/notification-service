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
- Docker-образ сервиса (`Dockerfile`): многоступенчатая сборка, статический бинарник (`CGO_ENABLED=0`), запуск от непривилегированного пользователя, `.dockerignore` не пускает `.env` в образ, кеш сборки Go между сборками.
- Docker Compose: PostgreSQL 17, Redis 7, Kafka 3.9 в режиме KRaft, одноразовый контейнер `kafka-init` (создает топики `notifications.email` и `notifications.push`), сам сервис (healthcheck через `/health`, перезапуск при падении), Prometheus и Grafana. Источник данных и дашборд Grafana подключаются из файлов (provisioning).
- Интеграционные тесты (`internal/integration`): в одном процессе собираются настоящие компоненты (gRPC-сервер через bufconn, PostgreSQL, Redis, Kafka producer и consumer, процессор, метрики) и проверяются сквозные сценарии: уведомление принято как `pending`, обработано асинхронно, устаревший статус из кеша сброшен; сбой отправки (`failed` с причиной); превышение лимита.
- CI на GitHub Actions: job `test` (`go vet`, `go test -race`, `go build`), job `integration` (все тесты с настоящими PostgreSQL, Redis и Kafka из Docker Compose) и job `docker` (сборка образа и проверка, что контейнер не запускается от root).
- CD на GitHub Actions: после успешного CI на `main` образ публикуется в `ghcr.io/nikolaykonkin/notification-service` с тегами `latest` и `sha-<7 символов коммита>`.

Что это значит на практике: весь стенд поднимается одной командой `make docker-up`. gRPC доступен на `localhost:50051`, `/health` и `/metrics` на `localhost:8080`, Prometheus на `localhost:9090`, Grafana на `localhost:3000` (admin/admin, только локально) с готовым дашбордом. Эмуляция отправки не завершается ошибкой, поэтому `notifications_failed_total` в ручной проверке остается нулем (путь `failed` проверяется интеграционным тестом с управляемой заглушкой). Образ публикуется только для `linux/amd64`. Включен gRPC reflection для удобства `grpcurl`. Все остальное перечислено в разделе Roadmap.

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

## Мониторинг

Prometheus раз в 15 секунд забирает `/metrics` сервиса, Grafana рисует графики. Источник данных и дашборд `notification-service` описаны файлами в `monitoring/` и подключаются при старте Grafana (provisioning), поэтому после `make docker-up` дашборд появляется сам.

Решения и особенности:

- Дашборд хранится как JSON в git. Он воспроизводим (не пропадает вместе с томом Grafana), его изменения видны в diff и откатываются. Минус: JSON многословный, запросы лежат внутри строк с экранированием, diff после экспорта получается шумным. Для большого числа дашбордов обычно переходят на генерацию (например, grafonnet), для одного дашборда это избыточно.
- Интерфейс Grafana для этого дашборда работает только на чтение (`allowUiUpdates: false`), иначе правка мышкой расходилась бы с файлом и терялась при следующем запуске. Рабочий цикл: изменить панель в интерфейсе без сохранения, выгрузить через Export as JSON, заменить файл `monitoring/grafana/dashboards/notification-service.json` и закоммитить.
- Связи по идентификаторам: `uid` источника данных в `datasources/prometheus.yml` должен совпадать со ссылками в панелях, а смена `uid` дашборда создаст новый дашборд вместо обновления старого. Числовой `id` в файле не указывается: он локален для базы Grafana.
- Метка `type` у метрик принимает только `email`, `push` и `unknown`: тип приходит из Kafka, и без ограничения мусорные значения создавали бы неограниченное число временных рядов (проблема кардинальности).
- Квантили на дашборде оценочные: Prometheus хранит счетчики корзин гистограммы и предполагает равномерное распределение внутри корзины. Эмуляция отправки занимает около 0,5 с и попадает в корзину от 0,5 до 1 с, поэтому p50 на графике около 0,75 с. Для точных значений нужны корзины, подобранные под реальное время обработки.
- Логин и пароль Grafana (`admin` / `admin`) заданы только для локального стенда, порты Prometheus и Grafana открыты лишь на `127.0.0.1`.

## Тесты

Уровни:

- unit: логика с заглушками (`api`, `processor`, `metrics`, `httpserver`), выполняются за секунды без внешних сервисов;
- integration: настоящие PostgreSQL, Redis и Kafka (`storage`, `cache`, `ratelimit`, `kafka` и сквозные сценарии в `internal/integration`);
- e2e (запущенный контейнер, вызов по сети) не автоматизирован, это ручной smoke-тест через grpcurl.

Особенности интеграционных тестов:

- Тесты, которым нужны внешние сервисы, пропускаются без переменных `TEST_DATABASE_URL`, `TEST_REDIS_URL` и `TEST_KAFKA_BROKERS`, поэтому обычный `go test` работает везде. `make test-integration` поднимает инфраструктуру, создает топики и запускает все тесты; в CI выполняется отдельный job.
- Отправка заменена управляемой заглушкой (`Sender` это интерфейс): она может задержать обработку выбранного пользователя, поэтому состояние `pending` и сброс кеша проверяются детерминированно, и может вернуть ошибку для проверки `failed`.
- Результат асинхронной обработки ждут опросом с таймаутом, а не `sleep`. Ожидание статуса опрашивает PostgreSQL напрямую: чтение через `GetNotification` могло бы само положить в кеш устаревший `pending` во время обработки (известная узкая гонка cache-aside), и тест стал бы нестабильным.
- Топики Kafka общие, поэтому у каждого теста свой `user_id`, а consumer group в интеграционных тестах фиксированная, чтобы после первого запуска не перечитывать накопленные сообщения.
- На время тестов контейнер сервиса останавливается: он читает те же топики и перезаписывал бы статусы. После тестов его можно вернуть командой `docker compose start notification-service`.
- Результаты тестов с внешним состоянием не кешируются (`-count=1`).

## CI/CD

CI (`.github/workflows/ci.yml`) запускается на push в `main` и на pull request, три job идут параллельно:

- `test`: `go vet`, `go test -race`, `go build`. Тесты, которым нужны внешние сервисы, здесь пропускаются, зато результат приходит быстро.
- `integration`: `make test-integration`, то есть PostgreSQL, Redis и Kafka из `docker-compose.yml` и все тесты, включая интеграционные. При падении в лог выводятся логи контейнеров.
- `docker`: сборка образа и проверка, что контейнер не запускается от root. Кеш слоев хранится в GitHub Actions.

CD (`.github/workflows/cd.yml`) публикует образ в `ghcr.io/nikolaykonkin/notification-service`:

```bash
docker pull ghcr.io/nikolaykonkin/notification-service:latest
```

Решения и особенности:

- CI не имеет побочных эффектов и прав на запись, поэтому запускается для любого pull request. CD вынесен в отдельный workflow и запускается через `workflow_run` только после успешного CI на push в `main`, чтобы образ не уходил в реестр с непроверенного коммита и из pull request.
- Для входа в реестр используется временный `GITHUB_TOKEN`. Право `packages: write` выдано только job-у публикации, у всего workflow права только на чтение.
- Теги: `latest` (подвижный, перезаписывается) и `sha-<7 символов коммита>` (не перезаписывается, однозначно указывает на коммит). Образ связан с репозиторием меткой `org.opencontainers.image.source`, коммит записан в `org.opencontainers.image.revision`.
- В контексте `workflow_run` переменная `GITHUB_SHA` указывает на последний коммит `main`, а не на проверенный CI, поэтому CD берет `head_sha` из события и собирает именно проверенный коммит. Блок `concurrency` выстраивает публикации в очередь, чтобы старая сборка не перезаписала `latest`.
- Ограничения: образ публикуется только для `linux/amd64`; CD пересобирает образ из проверенного коммита (слои берутся из кеша), а не публикует тот же артефакт, который собрал CI; кеши `--mount=type=cache` Dockerfile (модули и сборка Go) в кеш слоев GitHub Actions не попадают, поэтому после изменения кода шаг `go build` в CI выполняется с нуля; образ не подписан и не сканируется на уязвимости.

## Стек

Используется сейчас:

- Go 1.26
- gRPC, Protocol Buffers (`protoc`, `protoc-gen-go`, `protoc-gen-go-grpc`)
- PostgreSQL 17, драйвер pgx
- Redis 7, клиент go-redis
- Kafka 3.9 (KRaft, без ZooKeeper), клиент kafka-go
- Prometheus (client_golang в сервисе, сервер в Docker Compose), Grafana
- Docker, Docker Compose, GitHub Container Registry (ghcr.io)
- GitHub Actions

## Структура проекта

```
.
├── cmd/notification-service/   точка входа: сборка зависимостей, запуск, graceful shutdown (готово)
├── internal/
│   ├── api/                    gRPC-сервер (готово)
│   ├── cache/                  кеш уведомлений в Redis (готово)
│   ├── config/                 конфигурация из env (готово)
│   ├── httpserver/             HTTP: /metrics и /health (готово)
│   ├── integration/            интеграционные тесты с настоящими сервисами (готово)
│   ├── kafka/                  producer, consumer, проверка доступности (готово)
│   ├── metrics/                метрики Prometheus (готово)
│   ├── processor/              обработка события: отправка, статус, сброс кеша (готово)
│   ├── ratelimit/              rate limiting на Redis (готово)
│   └── storage/                репозиторий PostgreSQL (готово)
├── migrations/                 001_init.sql
├── monitoring/
│   ├── prometheus/             конфигурация Prometheus
│   └── grafana/                provisioning и дашборд Grafana
├── proto/notification/v1/      контракт и сгенерированный код
├── .github/workflows/
│   ├── ci.yml                  проверки: vet, тесты, интеграционные тесты, образ
│   └── cd.yml                  публикация образа в ghcr.io
├── Dockerfile                  сборка образа сервиса
├── .dockerignore
├── docker-compose.yml          весь стенд
├── Makefile
└── .env.example                настройки для запуска из консоли (make run)
```

## Быстрый старт

Нужны Go 1.26, Docker и make. Сгенерированный protobuf-код лежит в репозитории, поэтому `protoc` нужен только для `make proto`.

Поднять весь стенд (при первом запуске собирается образ сервиса, это занимает пару минут):

```bash
make docker-up
docker compose ps -a
```

Через 30-60 секунд `postgres`, `redis`, `kafka` и `notification-service` должны быть в статусе `healthy`, а `kafka-init` в статусе `Exited (0)`: он создал топики и завершился.

Адреса:

- gRPC: `localhost:50051`
- `/health` и `/metrics`: `localhost:8080`
- Prometheus: `localhost:9090` (`make metrics` открывает его в браузере)
- Grafana: `localhost:3000`, логин и пароль `admin` / `admin` (только для локального стенда), дашборд `notification-service`

Миграция из `migrations/` выполняется автоматически, но только при первом запуске на пустой базе. Чтобы применить ее заново, пересоздайте том: `docker compose down -v`, затем `make docker-up`.

Вызвать сервис через [grpcurl](https://github.com/fullstorydev/grpcurl):

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

Запустить сервис из консоли вместо контейнера (например, для отладки). Порты 50051 и 8080 заняты контейнером, поэтому его нужно остановить:

```bash
cp .env.example .env
docker compose stop notification-service
make run
```

Запустить тесты:

```bash
make vet
make test
```

Без переменных окружения тесты PostgreSQL, Redis, Kafka и интеграционные пропускаются (`SKIP`). Чтобы выполнить все тесты с настоящими сервисами, одной командой:

```bash
make test-integration
```

Она поднимает PostgreSQL, Redis и Kafka из Docker Compose, создает топики, останавливает контейнер сервиса (он читает те же топики и мешал бы тестам) и запускает `go test -race -count=1 ./...`. Первый запуск дольше остальных: consumer ждет вступления в группу и обрабатывает накопленные сообщения. Вернуть сервис после тестов: `docker compose start notification-service`.

Остановить стенд:

```bash
make docker-down
```

## Roadmap

- Полная версия README
- Финальная проверка: `make vet`, `make test-race`, `docker compose up`, smoke-тест через grpcurl