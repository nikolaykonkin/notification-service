# Этап 1. Сборка: здесь есть компилятор Go, в итоговый образ он не попадет
FROM golang:1.26-alpine AS builder

WORKDIR /src

# Сначала только файлы зависимостей: этот слой пересобирается, только когда они меняются
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 дает статический бинарник без зависимости от системной libc
# -trimpath убирает локальные пути, -s -w вырезает отладочную информацию (меньше размер)
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/notification-service ./cmd/notification-service

# Этап 2. Итоговый образ: только Alpine и готовый бинарник
FROM alpine:3.22

# Непривилегированный пользователь: при взломе у процесса не будет прав root
RUN addgroup -S app && adduser -S -G app -u 10001 app

COPY --from=builder /out/notification-service /usr/local/bin/notification-service

USER app

# EXPOSE только документирует порты, наружу их открывает compose (ports)
EXPOSE 50051 8080

# Exec-форма: бинарник становится процессом с PID 1 и сам получает SIGTERM от docker stop
ENTRYPOINT ["/usr/local/bin/notification-service"]