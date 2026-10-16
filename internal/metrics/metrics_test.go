package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nikolaykonkin/notification-service/internal/storage"
)

// scrape запрашивает /metrics так же, как это делает Prometheus, и возвращает текст
func scrape(t *testing.T, m *Metrics) string {
	t.Helper()

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("код ответа: получено %d, ожидался 200", rec.Code)
	}
	return rec.Body.String()
}

func requireContains(t *testing.T, body string, lines ...string) {
	t.Helper()
	for _, line := range lines {
		if !strings.Contains(body, line) {
			t.Errorf("в выводе /metrics нет строки %q", line)
		}
	}
}

func TestMetrics_InitialValuesAreZero(t *testing.T) {
	body := scrape(t, New())

	requireContains(t, body,
		`notifications_sent_total{type="email"} 0`,
		`notifications_sent_total{type="push"} 0`,
		`notifications_failed_total{type="email"} 0`,
		`notifications_failed_total{type="push"} 0`,
		`notification_duration_seconds_count{type="email"} 0`,
		`notification_duration_seconds_count{type="push"} 0`,
	)
}

func TestMetrics_ProcessedSent(t *testing.T) {
	m := New()
	m.Processed(storage.TypeEmail, storage.StatusSent, 250*time.Millisecond)

	body := scrape(t, m)
	requireContains(t, body,
		`notifications_sent_total{type="email"} 1`,
		`notifications_failed_total{type="email"} 0`,
		`notification_duration_seconds_count{type="email"} 1`,
		`notification_duration_seconds_sum{type="email"} 0.25`,
		// Корзина le="X" считает наблюдения не больше X: 0,25 с попадает
		// в корзины от 0,25 и выше, но не в корзину 0,1
		`notification_duration_seconds_bucket{type="email",le="0.1"} 0`,
		`notification_duration_seconds_bucket{type="email",le="0.25"} 1`,
		`notification_duration_seconds_bucket{type="email",le="+Inf"} 1`,
	)
}

func TestMetrics_ProcessedFailed(t *testing.T) {
	m := New()
	m.Processed(storage.TypePush, storage.StatusFailed, 2*time.Second)

	body := scrape(t, m)
	requireContains(t, body,
		`notifications_failed_total{type="push"} 1`,
		`notifications_sent_total{type="push"} 0`,
		`notification_duration_seconds_count{type="push"} 1`,
	)
}

func TestMetrics_OtherStatusesAreIgnored(t *testing.T) {
	m := New()
	m.Processed(storage.TypeEmail, storage.StatusPending, time.Second)
	m.Processed(storage.TypeEmail, storage.Status(""), time.Second)

	body := scrape(t, m)
	requireContains(t, body,
		`notifications_sent_total{type="email"} 0`,
		`notifications_failed_total{type="email"} 0`,
		`notification_duration_seconds_count{type="email"} 0`,
	)
}

func TestMetrics_UnknownTypeIsBounded(t *testing.T) {
	m := New()
	m.Processed(storage.Type("sms"), storage.StatusSent, time.Second)
	m.Processed(storage.Type("whatever"), storage.StatusSent, time.Second)

	body := scrape(t, m)
	// Любые неизвестные типы попадают в один ряд, а не создают новые
	requireContains(t, body, `notifications_sent_total{type="unknown"} 2`)
	if strings.Contains(body, `type="sms"`) || strings.Contains(body, `type="whatever"`) {
		t.Error("значения из внешнего ввода не должны попадать в метки")
	}
}

func TestMetrics_IncludesRuntimeMetrics(t *testing.T) {
	// Gauge из стандартного коллектора Go: число горутин
	requireContains(t, scrape(t, New()), "go_goroutines")
}
