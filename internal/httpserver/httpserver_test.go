package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testHandler(checks ...Check) http.Handler {
	metrics := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("metrics-body"))
	})
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewHandler(metrics, checks, 100*time.Millisecond, log)
}

func okCheck(name string) Check {
	return Check{Name: name, Fn: func(context.Context) error { return nil }}
}

func failCheck(name string) Check {
	return Check{Name: name, Fn: func(context.Context) error {
		return errors.New("secret-host:5432 connection refused")
	}}
}

func do(h http.Handler, method, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
	return rec
}

func decodeHealth(t *testing.T, rec *httptest.ResponseRecorder) healthResponse {
	t.Helper()
	var resp healthResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("разбор ответа: %v", err)
	}
	return resp
}

func TestHealth_AllChecksPass(t *testing.T) {
	rec := do(testHandler(okCheck("postgresql"), okCheck("redis")), http.MethodGet, "/health")

	if rec.Code != http.StatusOK {
		t.Fatalf("код ответа: получено %d, ожидался 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type: получено %q", ct)
	}
	resp := decodeHealth(t, rec)
	if resp.Status != "ok" || resp.Checks["postgresql"] != "ok" || resp.Checks["redis"] != "ok" {
		t.Errorf("неверный ответ: %+v", resp)
	}
}

func TestHealth_FailingCheckReturns503(t *testing.T) {
	rec := do(testHandler(okCheck("postgresql"), failCheck("redis")), http.MethodGet, "/health")

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("код ответа: получено %d, ожидался 503", rec.Code)
	}

	body := rec.Body.String()
	// Подробности ошибки не должны попадать в ответ
	if strings.Contains(body, "secret-host") {
		t.Errorf("текст ошибки попал в ответ: %s", body)
	}
	if !strings.Contains(body, `"redis":"fail"`) || !strings.Contains(body, `"postgresql":"ok"`) ||
		!strings.Contains(body, `"status":"unavailable"`) {
		t.Errorf("неверный ответ: %s", body)
	}
}

func TestHealth_CheckTimeout(t *testing.T) {
	hang := Check{Name: "redis", Fn: func(ctx context.Context) error {
		<-ctx.Done() // проверка ждет, пока не истечет таймаут
		return ctx.Err()
	}}

	begin := time.Now()
	rec := do(testHandler(hang), http.MethodGet, "/health")

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("код ответа: получено %d, ожидался 503", rec.Code)
	}
	if elapsed := time.Since(begin); elapsed > 2*time.Second {
		t.Errorf("зависшая проверка задержала ответ на %v", elapsed)
	}
}

func TestMetricsRoute(t *testing.T) {
	rec := do(testHandler(), http.MethodGet, "/metrics")

	if rec.Code != http.StatusOK || rec.Body.String() != "metrics-body" {
		t.Errorf("получено %d %q", rec.Code, rec.Body.String())
	}
}

func TestRoutes_MethodsAndPaths(t *testing.T) {
	h := testHandler(okCheck("postgresql"))

	if code := do(h, http.MethodPost, "/health").Code; code != http.StatusMethodNotAllowed {
		t.Errorf("POST /health: получено %d, ожидался 405", code)
	}
	if code := do(h, http.MethodGet, "/unknown").Code; code != http.StatusNotFound {
		t.Errorf("GET /unknown: получено %d, ожидался 404", code)
	}
}

func TestNewServer_SetsTimeouts(t *testing.T) {
	srv := NewServer(http.NotFoundHandler())

	if srv.ReadHeaderTimeout <= 0 || srv.ReadTimeout <= 0 || srv.WriteTimeout <= 0 || srv.IdleTimeout <= 0 {
		t.Errorf("все таймауты должны быть заданы: %+v", srv)
	}
}
