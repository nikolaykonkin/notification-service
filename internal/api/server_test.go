package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/nikolaykonkin/notification-service/internal/cache"
	"github.com/nikolaykonkin/notification-service/internal/kafka"
	"github.com/nikolaykonkin/notification-service/internal/ratelimit"
	"github.com/nikolaykonkin/notification-service/internal/storage"
	notificationv1 "github.com/nikolaykonkin/notification-service/proto/notification/v1"
)

// Проверка на этапе компиляции: реальные типы подходят под наши интерфейсы
// Если сигнатура где-то разойдется, ошибка появится здесь, а не при сборке main
var (
	_ Store     = (*storage.Repository)(nil)
	_ Cache     = (*cache.NotificationCache)(nil)
	_ Limiter   = (*ratelimit.Limiter)(nil)
	_ Publisher = (*kafka.Producer)(nil)
)

// Заглушки: запоминают, что с ними делали, и возвращают заданные значения

type fakeStore struct {
	createCalls int
	createUser  int64
	createType  storage.Type
	createBody  string
	createRes   storage.Notification
	createErr   error

	getCalls int
	getRes   storage.Notification
	getErr   error

	updateCalls  int
	updateID     string
	updateStatus storage.Status
	updateMsg    string
}

func (f *fakeStore) Create(_ context.Context, userID int64, typ storage.Type, payload string) (storage.Notification, error) {
	f.createCalls++
	f.createUser, f.createType, f.createBody = userID, typ, payload
	return f.createRes, f.createErr
}

func (f *fakeStore) GetByID(_ context.Context, _ string) (storage.Notification, error) {
	f.getCalls++
	return f.getRes, f.getErr
}

func (f *fakeStore) UpdateStatus(_ context.Context, id string, st storage.Status, msg string) error {
	f.updateCalls++
	f.updateID, f.updateStatus, f.updateMsg = id, st, msg
	return nil
}

type fakeCache struct {
	getCalls int
	getRes   storage.Notification
	getFound bool
	getErr   error
	sets     []storage.Notification
}

func (f *fakeCache) Get(_ context.Context, _ string) (storage.Notification, bool, error) {
	f.getCalls++
	return f.getRes, f.getFound, f.getErr
}

func (f *fakeCache) Set(_ context.Context, n storage.Notification) error {
	f.sets = append(f.sets, n)
	return nil
}

type fakeLimiter struct {
	calls   int
	allowed bool
	err     error
}

func (f *fakeLimiter) Allow(_ context.Context, _ int64) (bool, error) {
	f.calls++
	return f.allowed, f.err
}

type fakePublisher struct {
	events []kafka.Event
	err    error
}

func (f *fakePublisher) Publish(_ context.Context, e kafka.Event) error {
	f.events = append(f.events, e)
	return f.err
}

const sampleID = "11111111-1111-1111-1111-111111111111"

func sampleNotification() storage.Notification {
	ts := time.Date(2026, 10, 12, 10, 0, 0, 0, time.UTC)
	return storage.Notification{
		ID:        sampleID,
		UserID:    42,
		Type:      storage.TypeEmail,
		Status:    storage.StatusPending,
		Payload:   `{"to": "user@example.com"}`,
		CreatedAt: ts,
		UpdatedAt: ts,
	}
}

// fixture собирает сервер с заглушками и клиента, подключенного через bufconn
type fixture struct {
	store   *fakeStore
	cache   *fakeCache
	limiter *fakeLimiter
	pub     *fakePublisher
	client  notificationv1.NotificationServiceClient
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	f := &fixture{
		store:   &fakeStore{createRes: sampleNotification(), getRes: sampleNotification()},
		cache:   &fakeCache{},
		limiter: &fakeLimiter{allowed: true},
		pub:     &fakePublisher{},
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := New(f.store, f.cache, f.limiter, f.pub, log)

	// bufconn это listener в памяти: запрос проходит через настоящий gRPC
	// (сериализация, коды ошибок), но без сети и портов
	lis := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	notificationv1.RegisterNotificationServiceServer(grpcServer, srv)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	// Схема passthrough отключает DNS-разрешение адреса: нам нужен только наш dialer
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("создание клиента: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	f.client = notificationv1.NewNotificationServiceClient(conn)
	return f
}

// testCtx возвращает контекст с таймаутом, чтобы зависший тест не блокировал остальные
func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func requireCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("код ошибки: получено %v (%v), ожидался %v", got, err, want)
	}
}

var emailReq = &notificationv1.SendNotificationRequest{
	UserId:  42,
	Type:    notificationv1.NotificationType_NOTIFICATION_TYPE_EMAIL,
	Payload: `{"to":"user@example.com"}`,
}

// --- SendNotification ---

func TestSendNotification_Success(t *testing.T) {
	f := newFixture(t)

	resp, err := f.client.SendNotification(testCtx(t), emailReq)
	if err != nil {
		t.Fatalf("SendNotification: %v", err)
	}

	if resp.GetId() != sampleID || resp.GetStatus() != notificationv1.NotificationStatus_NOTIFICATION_STATUS_PENDING {
		t.Errorf("неверный ответ: %+v", resp)
	}
	if f.limiter.calls != 1 {
		t.Errorf("лимит должен быть проверен один раз, проверок: %d", f.limiter.calls)
	}
	if f.store.createCalls != 1 || f.store.createUser != 42 ||
		f.store.createType != storage.TypeEmail || f.store.createBody != emailReq.Payload {
		t.Errorf("запись в базу выполнена неверно: %+v", f.store)
	}
	if len(f.pub.events) != 1 {
		t.Fatalf("ожидалась одна публикация, получено %d", len(f.pub.events))
	}
	if e := f.pub.events[0]; e.ID != sampleID || e.UserID != 42 || e.Type != storage.TypeEmail {
		t.Errorf("неверное событие: %+v", e)
	}
}

func TestSendNotification_Validation(t *testing.T) {
	email := notificationv1.NotificationType_NOTIFICATION_TYPE_EMAIL

	tests := []struct {
		name string
		req  *notificationv1.SendNotificationRequest
	}{
		{"нулевой user_id", &notificationv1.SendNotificationRequest{Type: email, Payload: `{}`}},
		{"тип не указан", &notificationv1.SendNotificationRequest{UserId: 1, Payload: `{}`}},
		{"пустой payload", &notificationv1.SendNotificationRequest{UserId: 1, Type: email}},
		{"payload не JSON", &notificationv1.SendNotificationRequest{UserId: 1, Type: email, Payload: "oops"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)

			_, err := f.client.SendNotification(testCtx(t), tt.req)
			requireCode(t, err, codes.InvalidArgument)

			// Плохой запрос не должен тратить ни лимит, ни базу, ни очередь
			if f.limiter.calls != 0 || f.store.createCalls != 0 || len(f.pub.events) != 0 {
				t.Errorf("запрос не должен был дойти дальше валидации: %+v", f)
			}
		})
	}
}

func TestSendNotification_RateLimited(t *testing.T) {
	f := newFixture(t)
	f.limiter.allowed = false

	_, err := f.client.SendNotification(testCtx(t), emailReq)
	requireCode(t, err, codes.ResourceExhausted)

	if f.store.createCalls != 0 || len(f.pub.events) != 0 {
		t.Error("при превышении лимита запись и публикация не нужны")
	}
}

func TestSendNotification_LimiterErrorFailsOpen(t *testing.T) {
	f := newFixture(t)
	f.limiter.err = errors.New("redis down")

	if _, err := f.client.SendNotification(testCtx(t), emailReq); err != nil {
		t.Fatalf("при сбое лимитера запрос должен пройти (fail-open): %v", err)
	}
	if f.store.createCalls != 1 || len(f.pub.events) != 1 {
		t.Error("уведомление должно быть сохранено и опубликовано")
	}
}

func TestSendNotification_CreateError(t *testing.T) {
	f := newFixture(t)
	f.store.createErr = errors.New("db: relation notifications broken")

	_, err := f.client.SendNotification(testCtx(t), emailReq)
	requireCode(t, err, codes.Internal)

	if len(f.pub.events) != 0 {
		t.Error("без записи в базе публиковать нельзя")
	}
	// Внутренние детали не должны попадать к клиенту
	if strings.Contains(status.Convert(err).Message(), "relation") {
		t.Errorf("текст ошибки базы попал к клиенту: %v", err)
	}
}

func TestSendNotification_PublishErrorMarksFailed(t *testing.T) {
	f := newFixture(t)
	f.pub.err = errors.New("kafka down")

	_, err := f.client.SendNotification(testCtx(t), emailReq)
	requireCode(t, err, codes.Unavailable)

	// Запись, которую не удалось поставить в очередь, не должна оставаться в pending
	if f.store.updateCalls != 1 || f.store.updateID != sampleID ||
		f.store.updateStatus != storage.StatusFailed || f.store.updateMsg == "" {
		t.Errorf("запись должна быть помечена failed с причиной: %+v", f.store)
	}
}

// --- GetNotification ---

func TestGetNotification_FromCache(t *testing.T) {
	f := newFixture(t)
	f.cache.getFound = true
	f.cache.getRes = sampleNotification()

	resp, err := f.client.GetNotification(testCtx(t), &notificationv1.GetNotificationRequest{Id: sampleID})
	if err != nil {
		t.Fatalf("GetNotification: %v", err)
	}

	got := resp.GetNotification()
	if got.GetId() != sampleID || got.GetUserId() != 42 ||
		got.GetType() != notificationv1.NotificationType_NOTIFICATION_TYPE_EMAIL {
		t.Errorf("неверный ответ: %+v", got)
	}
	if !got.GetCreatedAt().AsTime().Equal(sampleNotification().CreatedAt) {
		t.Errorf("время создания неверно: %v", got.GetCreatedAt().AsTime())
	}
	if f.store.getCalls != 0 {
		t.Error("при попадании в кеш в базу ходить не нужно")
	}
}

func TestGetNotification_CacheMissFillsCache(t *testing.T) {
	f := newFixture(t) // кеш пуст, база вернет sampleNotification

	resp, err := f.client.GetNotification(testCtx(t), &notificationv1.GetNotificationRequest{Id: sampleID})
	if err != nil {
		t.Fatalf("GetNotification: %v", err)
	}

	if resp.GetNotification().GetId() != sampleID {
		t.Errorf("неверный ответ: %+v", resp)
	}
	if f.store.getCalls != 1 {
		t.Errorf("при промахе нужно прочитать из базы, чтений: %d", f.store.getCalls)
	}
	if len(f.cache.sets) != 1 || f.cache.sets[0].ID != sampleID {
		t.Errorf("запись должна быть положена в кеш: %+v", f.cache.sets)
	}
}

func TestGetNotification_CacheErrorFallsBackToStore(t *testing.T) {
	f := newFixture(t)
	f.cache.getErr = errors.New("redis down")

	resp, err := f.client.GetNotification(testCtx(t), &notificationv1.GetNotificationRequest{Id: sampleID})
	if err != nil {
		t.Fatalf("при сбое кеша ответ должен прийти из базы: %v", err)
	}
	if resp.GetNotification().GetId() != sampleID || f.store.getCalls != 1 {
		t.Errorf("ожидалось чтение из базы: %+v", resp)
	}
}

func TestGetNotification_NotFound(t *testing.T) {
	f := newFixture(t)
	f.store.getErr = storage.ErrNotFound

	_, err := f.client.GetNotification(testCtx(t), &notificationv1.GetNotificationRequest{Id: sampleID})
	requireCode(t, err, codes.NotFound)

	if len(f.cache.sets) != 0 {
		t.Error("отсутствующую запись в кеш класть не нужно")
	}
}

func TestGetNotification_InvalidID(t *testing.T) {
	for _, id := range []string{"", "not-a-uuid"} {
		t.Run("id="+id, func(t *testing.T) {
			f := newFixture(t)

			_, err := f.client.GetNotification(testCtx(t), &notificationv1.GetNotificationRequest{Id: id})
			requireCode(t, err, codes.InvalidArgument)

			if f.cache.getCalls != 0 || f.store.getCalls != 0 {
				t.Error("плохой id не должен доходить до кеша и базы")
			}
		})
	}
}

func TestGetNotification_StoreError(t *testing.T) {
	f := newFixture(t)
	f.store.getErr = errors.New("db down")

	_, err := f.client.GetNotification(testCtx(t), &notificationv1.GetNotificationRequest{Id: sampleID})
	requireCode(t, err, codes.Internal)
}
