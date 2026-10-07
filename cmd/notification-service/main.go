// Команда notification-service запускает сервис уведомлений
package main

import (
	"log/slog"
	"os"
)

func main() {
	// JSON-логгер: логи удобно парсить в Loki, ELK и других системах
	// Пишем в stdout: так принято для контейнеров (Docker сам соберет логи)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// Заглушка: настоящая сборка зависимостей появится потом
	slog.Info("notification-service starting")
}
