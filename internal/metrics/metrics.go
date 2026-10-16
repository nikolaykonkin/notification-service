// Пакет metrics содержит метрики Prometheus и их выдачу по HTTP
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/nikolaykonkin/notification-service/internal/storage"
)

// labelUnknown это значение метки type для всего, что не email и не push
// Тип приходит из Kafka, то есть извне
// Если подставлять его в метку как есть, мусорные значения создадут неограниченное число рядов
const labelUnknown = "unknown"

// Metrics содержит метрики сервиса и собственный реестр
type Metrics struct {
	registry *prometheus.Registry
	sent     *prometheus.CounterVec
	failed   *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

// New создает метрики и регистрирует их в отдельном реестре (не глобальном)
func New() *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		sent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_sent_total",
			Help: "Количество успешно отправленных уведомлений.",
		}, []string{"type"}),
		failed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_failed_total",
			Help: "Количество уведомлений, отправка которых завершилась ошибкой.",
		}, []string{"type"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "notification_duration_seconds",
			Help: "Время обработки уведомления: отправка и запись результата в PostgreSQL.",
			// Стандартные корзины: от 5 мс до 10 с.
			Buckets: prometheus.DefBuckets,
		}, []string{"type"}),
	}

	// MustRegister паникует при ошибке в описании метрик (например, повторяющиеся имена)
	// Это ошибка в коде, а не во внешних условиях: она проявляется при каждом запуске
	// и находится сразу, в том числе в тестах
	m.registry.MustRegister(
		m.sent, m.failed, m.duration,
		collectors.NewGoCollector(),                                       // горутины, память, сборщик мусора
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}), // ресурсы процесса
	)

	// Метрика с метками появляется в выводе только после первого использования
	// Создаем ряды заранее, чтобы на графиках был ноль, а не "нет данных"
	for _, t := range []storage.Type{storage.TypeEmail, storage.TypePush} {
		label := string(t)
		m.sent.WithLabelValues(label)
		m.failed.WithLabelValues(label)
		m.duration.WithLabelValues(label)
	}

	return m
}

// Processed фиксирует результат обработки уведомления
// Подходит под интерфейс processor.Recorder
// Учитываются только итоговые статусы sent и failed
func (m *Metrics) Processed(typ storage.Type, status storage.Status, d time.Duration) {
	label := typeLabel(typ)

	switch status {
	case storage.StatusSent:
		m.sent.WithLabelValues(label).Inc()
	case storage.StatusFailed:
		m.failed.WithLabelValues(label).Inc()
	default:
		return
	}
	m.duration.WithLabelValues(label).Observe(d.Seconds())
}

// Handler возвращает HTTP-обработчик для эндпоинта /metrics
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// typeLabel возвращает значение метки type с ограниченным набором значений
func typeLabel(t storage.Type) string {
	switch t {
	case storage.TypeEmail, storage.TypePush:
		return string(t)
	default:
		return labelUnknown
	}
}
