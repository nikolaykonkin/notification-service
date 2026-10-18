# Файл схемы gRPC-сервиса
PROTO_FILE := proto/notification/v1/notification.proto

# Путь к собранному бинарнику
BIN := bin/notification-service

# Адреса инфраструктуры из docker-compose.yml для тестов, которым нужны настоящие сервисы
TEST_DATABASE_URL  ?= postgres://notification:notification@localhost:5432/notification?sslmode=disable
TEST_REDIS_URL     ?= redis://localhost:6379/0
TEST_KAFKA_BROKERS ?= localhost:9092

# Генерация Go-кода из .proto: структуры (*.pb.go) и gRPC-код (*_grpc.pb.go)
# paths=source_relative кладет результат рядом с .proto-файлом
.PHONY: proto
proto:
	protoc \
		--proto_path=. \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(PROTO_FILE)

# Сборка бинарника в bin/
.PHONY: build
build:
	go build -o $(BIN) ./cmd/notification-service

# Запуск с настройками из .env (set -a экспортирует все переменные из файла)
# Если сервис уже запущен в Docker Compose, сначала остановите его (порты заняты):
#   docker compose stop notification-service
.PHONY: run
run:
	@set -a; . ./.env; set +a; go run ./cmd/notification-service

# Тесты, которым нужны внешние сервисы, без TEST_* пропускаются
.PHONY: test
test:
	go test ./...

# Тесты с детектором гонок данных (race detector)
.PHONY: test-race
test-race:
	go test -race ./...

# Все тесты, включая интеграционные, с настоящими PostgreSQL, Redis и Kafka из Docker Compose
# Контейнер сервиса останавливается: он читает те же топики и подменял бы результаты тестов
# После тестов его можно вернуть: docker compose start notification-service
# -count=1 отключает кеш результатов Go: результат зависит от внешнего состояния
.PHONY: test-integration
test-integration:
	docker compose up -d --wait postgres redis kafka
	docker compose run --rm kafka-init
	docker compose stop notification-service
	TEST_DATABASE_URL='$(TEST_DATABASE_URL)' \
	TEST_REDIS_URL='$(TEST_REDIS_URL)' \
	TEST_KAFKA_BROKERS='$(TEST_KAFKA_BROKERS)' \
	go test -race -count=1 -timeout 5m ./...

# Статический анализ
.PHONY: vet
vet:
	go vet ./...

# Поднять весь стенд: инфраструктуру, сервис, Prometheus и Grafana (образ сервиса пересобирается)
.PHONY: docker-up
docker-up:
	docker compose up -d --build

# Остановить и удалить контейнеры (данные PostgreSQL сохраняются в томе)
# Чтобы удалить и данные: docker compose down -v
.PHONY: docker-down
docker-down:
	docker compose down

# Открыть Prometheus в браузере (macOS: open, Linux: xdg-open)
.PHONY: metrics
metrics:
	@open http://localhost:9090 2>/dev/null || xdg-open http://localhost:9090