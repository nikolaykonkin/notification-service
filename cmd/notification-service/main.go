// Команда notification-service запускает сервис уведомлений
package main

import (
	"log/slog"
	"os"

	"github.com/nikolaykonkin/notification-service/internal/config"
)

func main() {
	// JSON-логгер: логи удобно парсить в Loki, ELK и других системах
	// Пишем в stdout: так принято для контейнеров (Docker сам соберет логи)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// Fail-fast: если настройки некорректны, завершаемся сразу с кодом 1
	cfg, err := config.Load()
	if err != nil {
		slog.Error("не удалось загрузить конфигурацию", "error", err)
		os.Exit(1)
	}

	// Логируем только безопасные поля: DATABASE_URL содержит пароль
	slog.Info("configuration loaded",
		"grpc_port", cfg.GRPCPort,
		"http_port", cfg.HTTPPort,
		"rate_limit_per_hour", cfg.RateLimitPerHour,
	)

	// Заглушка: настоящая сборка зависимостей появится потом
	slog.Info("notification-service starting")
}
