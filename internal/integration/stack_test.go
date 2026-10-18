package integration

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/nikolaykonkin/notification-service/internal/api"
	"github.com/nikolaykonkin/notification-service/internal/cache"
	"github.com/nikolaykonkin/notification-service/internal/kafka"
	"github.com/nikolaykonkin/notification-service/internal/metrics"
	"github.com/nikolaykonkin/notification-service/internal/processor"
	"github.com/nikolaykonkin/notification-service/internal/ratelimit"
	"github.com/nikolaykonkin/notification-service/internal/storage"
	notificationv1 "github.com/nikolaykonkin/notification-service/proto/notification/v1"
)

const (
	// integrationGroup это фиксированная consumer group
	// С новой группой каждый запуск перечитывал бы все сообщения, накопленные в топиках;
	// с фиксированной, после первого запуска, читаются только новые
	integrationGroup = "notification-service-integration-test"

	// rateLimit и rateWindow задают лимит стека: маленький, чтобы его было легко превысить
	// Окно в минуту: ключи счетчиков, оставшиеся от тестов, быстро исчезают сами
	rateLimit  = 3
	rateWindow = time.Minute

	// waitTimeout это время, которое тест ждет асинхронного результата
	// При первом запуске consumer может сначала обработать накопленные сообщения
	waitTimeout = 30 * time.Second
)

// stack это сервис, собранный из настоящих компонентов, и клиент к нему
type stack struct {
	client  notificationv1.NotificationServiceClient
	repo    *storage.Repository
	cache   *cache.NotificationCache
	pool    *pgxpool.Pool
	metrics *metrics.Metrics
	sender  *scriptedSender
}

// requireEnv читает адреса инфраструктуры или пропускает тест
func requireEnv(t *testing.T) (dbURL, redisURL string, brokers []string) {
	t.Helper()

	dbURL = os.Getenv("TEST_DATABASE_URL")
	redisURL = os.Getenv("TEST_REDIS_URL")
	rawBrokers := os.Getenv("TEST_KAFKA_BROKERS")
	if dbURL == "" || redisURL == "" || rawBrokers == "" {
		t.Skip("интеграционные тесты пропущены: нужны TEST_DATABASE_URL, TEST_REDIS_URL и TEST_KAFKA_BROKERS")
	}

	for _, b := range strings.Split(rawBrokers, ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	return dbURL, redisURL, brokers
}

// newStack собирает сервис так же, как main.go, но с управляемой заглушкой вместо отправки
// Остановка идет через t.Cleanup в обратном порядке регистрации:
// сначала gRPC-сервер, затем consumer, producer, Redis и PostgreSQL
func newStack(t *testing.T) *stack {
	t.Helper()

	dbURL, redisURL, brokers := requireEnv(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	startCtx, cancelStart := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancelStart)

	pool, err := storage.NewPool(startCtx, dbURL)
	if err != nil {
		t.Fatalf("подключение к PostgreSQL: %v", err)
	}
	t.Cleanup(pool.Close)

	redisClient, err := cache.NewClient(startCtx, redisURL)
	if err != nil {
		t.Fatalf("подключение к Redis: %v", err)
	}
	t.Cleanup(func() { _ = redisClient.Close() })

	if err := kafka.Ping(startCtx, brokers); err != nil {
		t.Fatalf("подключение к Kafka: %v", err)
	}

	repo := storage.NewRepository(pool)
	notificationCache := cache.NewNotificationCache(redisClient, cache.DefaultTTL)
	limiter := ratelimit.New(redisClient, rateLimit, rateWindow)

	producer := kafka.NewProducer(brokers)
	t.Cleanup(func() { _ = producer.Close() })

	m := metrics.New()
	sender := newScriptedSender()
	proc := processor.New(repo, notificationCache, sender, m, log)
	consumer := kafka.NewConsumer(brokers, integrationGroup, proc, log)

	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	consumerDone := make(chan error, 1)
	go func() { consumerDone <- consumer.Run(consumerCtx) }()
	t.Cleanup(func() {
		cancelConsumer()
		if err := <-consumerDone; err != nil {
			t.Errorf("consumer остановился с ошибкой: %v", err)
		}
		_ = consumer.Close()
	})

	// gRPC-сервер через bufconn: настоящий gRPC-стек, но без сети и портов
	lis := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	notificationv1.RegisterNotificationServiceServer(grpcServer,
		api.New(repo, notificationCache, limiter, producer, log))
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("создание gRPC-клиента: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return &stack{
		client:  notificationv1.NewNotificationServiceClient(conn),
		repo:    repo,
		cache:   notificationCache,
		pool:    pool,
		metrics: m,
		sender:  sender,
	}
}

// get читает уведомление через gRPC, то есть через кеш, как это делает клиент
func (s *stack) get(t *testing.T, id string) *notificationv1.Notification {
	t.Helper()

	resp, err := s.client.GetNotification(testCtx(t), &notificationv1.GetNotificationRequest{Id: id})
	if err != nil {
		t.Fatalf("GetNotification(%s): %v", id, err)
	}
	return resp.GetNotification()
}

// metricValue находит в выводе /metrics значение ряда, например
// `notifications_failed_total{type="push"}`
func (s *stack) metricValue(t *testing.T, series string) float64 {
	t.Helper()

	rec := httptest.NewRecorder()
	s.metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if rest, ok := strings.CutPrefix(line, series+" "); ok {
			v, err := strconv.ParseFloat(strings.TrimSpace(rest), 64)
			if err != nil {
				t.Fatalf("значение ряда %s: %v", series, err)
			}
			return v
		}
	}
	t.Fatalf("в выводе /metrics нет ряда %s", series)
	return 0
}

// scriptedSender это управляемая замена отправки
// Для выбранного пользователя она может задержать "отправку" или вернуть ошибку,
// для остальных сразу сообщает об успехе
type scriptedSender struct {
	mu   sync.Mutex
	hold map[int64]chan struct{}
	fail map[int64]error
}

func newScriptedSender() *scriptedSender {
	return &scriptedSender{hold: map[int64]chan struct{}{}, fail: map[int64]error{}}
}

// Hold задерживает обработку уведомлений пользователя, пока не вызвана возвращенная функция
func (s *scriptedSender) Hold(userID int64) (release func()) {
	ch := make(chan struct{})

	s.mu.Lock()
	s.hold[userID] = ch
	s.mu.Unlock()

	var once sync.Once
	return func() { once.Do(func() { close(ch) }) }
}

// FailWith заставляет "отправку" уведомлений пользователя завершаться ошибкой
func (s *scriptedSender) FailWith(userID int64, err error) {
	s.mu.Lock()
	s.fail[userID] = err
	s.mu.Unlock()
}

// Send реализует processor.Sender
func (s *scriptedSender) Send(ctx context.Context, e kafka.Event) error {
	s.mu.Lock()
	hold, err := s.hold[e.UserID], s.fail[e.UserID]
	s.mu.Unlock()

	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done(): // остановка consumer не должна зависнуть на задержанном событии
			return ctx.Err()
		}
	}
	return err
}

var userSeq atomic.Int64

// newUser возвращает уникальный user_id
// Топики и таблица общие, и данные прошлых запусков и других тестов не должны влиять на результат
func newUser() int64 {
	return time.Now().UnixNano() + userSeq.Add(1)
}

// testCtx возвращает контекст с таймаутом, чтобы зависший тест не блокировал остальные
func testCtx(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// eventually опрашивает условие, пока оно не выполнится или не истечет waitTimeout
// Для асинхронных результатов это надежнее, чем time.Sleep на фиксированное время
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(waitTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("не дождались за %v: %s", waitTimeout, what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
