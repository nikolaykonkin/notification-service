// Пакет httpserver содержит HTTP-сервер сервиса: /metrics для Prometheus и /health
package httpserver

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// Check это одна проверка зависимости для /health
type Check struct {
	Name string
	Fn   func(ctx context.Context) error
}

// healthResponse это тело ответа /health
type healthResponse struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}

// NewHandler собирает маршруты сервиса
// Шаблон "GET /path" отвечает кодом 405 на другие методы и 404 на неизвестные пути
func NewHandler(metrics http.Handler, checks []Check, checkTimeout time.Duration, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics)
	mux.HandleFunc("GET /health", healthHandler(checks, checkTimeout, log))
	return mux
}

// healthHandler выполняет проверки и отвечает 200, если все прошли, иначе 503
// Тексты ошибок в ответ не попадают: эндпоинт не защищен, подробности пишем в лог
func healthHandler(checks []Check, timeout time.Duration, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Один общий таймаут на все проверки: зависшая база не должна подвесить /health
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		resp := healthResponse{Status: "ok", Checks: make(map[string]string, len(checks))}
		code := http.StatusOK

		for _, c := range checks {
			if err := c.Fn(ctx); err != nil {
				log.Warn("проверка здоровья не пройдена", "check", c.Name, "error", err)
				resp.Checks[c.Name] = "fail"
				resp.Status = "unavailable"
				code = http.StatusServiceUnavailable
				continue
			}
			resp.Checks[c.Name] = "ok"
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(resp)
	}
}

// NewServer создает http.Server с таймаутами
// Без них медленный клиент может держать соединение открытым сколь угодно долго (атака Slowloris)
func NewServer(h http.Handler) *http.Server {
	return &http.Server{
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}
