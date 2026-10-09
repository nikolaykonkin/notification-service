# Файл схемы gRPC-сервиса
PROTO_FILE := proto/notification/v1/notification.proto

# Путь к собранному бинарнику
BIN := bin/notification-service

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
.PHONY: run
run:
	@set -a; . ./.env; set +a; go run ./cmd/notification-service

# Тесты
.PHONY: test
test:
	go test ./...

# Тесты с детектором гонок данных (race detector)
.PHONY: test-race
test-race:
	go test -race ./...

# Статический анализ
.PHONY: vet
vet:
	go vet ./...
# Поднять локальную инфраструктуру (PostgreSQL, Redis, Kafka) в фоне
.PHONY: docker-up
docker-up:
	docker compose up -d

# Остановить и удалить контейнеры (данные PostgreSQL сохраняются в томе)
# Чтобы удалить и данные: docker compose down -v
.PHONY: docker-down
docker-down:
	docker compose down