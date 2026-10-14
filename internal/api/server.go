// Пакет api содержит реализацию gRPC-сервера: прием событий и выдачу статусов
package api

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nikolaykonkin/notification-service/internal/kafka"
	"github.com/nikolaykonkin/notification-service/internal/storage"
	notificationv1 "github.com/nikolaykonkin/notification-service/proto/notification/v1"
)

// markFailedTimeout это время на запись статуса failed, когда публикация в Kafka не удалась
const markFailedTimeout = 5 * time.Second

// Интерфейсы объявлены здесь, на стороне потребителя, и содержат только то, что нужно обработчикам
// Реальные типы подходят под них без изменений, а в тестах подставляются заглушки

// Store работает с историей уведомлений в PostgreSQL
type Store interface {
	Create(ctx context.Context, userID int64, typ storage.Type, payload string) (storage.Notification, error)
	GetByID(ctx context.Context, id string) (storage.Notification, error)
	UpdateStatus(ctx context.Context, id string, status storage.Status, errMsg string) error
}

// Cache кеширует уведомления по id
type Cache interface {
	Get(ctx context.Context, id string) (n storage.Notification, found bool, err error)
	Set(ctx context.Context, n storage.Notification) error
}

// Limiter ограничивает число уведомлений на пользователя
type Limiter interface {
	Allow(ctx context.Context, userID int64) (bool, error)
}

// Publisher отправляет событие в очередь
type Publisher interface {
	Publish(ctx context.Context, e kafka.Event) error
}

// Server реализует gRPC-сервис NotificationService
type Server struct {
	// Встраивание обязательно для protoc-gen-go-grpc: новые методы в .proto
	// не ломают компиляцию, а возвращают клиенту код Unimplemented
	notificationv1.UnimplementedNotificationServiceServer

	store     Store
	cache     Cache
	limiter   Limiter
	publisher Publisher
	log       *slog.Logger
}

// New создает сервер
// Регистрация в grpc.Server выполняется снаружи (в main)
func New(store Store, cache Cache, limiter Limiter, publisher Publisher, log *slog.Logger) *Server {
	return &Server{store: store, cache: cache, limiter: limiter, publisher: publisher, log: log}
}

// SendNotification проверяет запрос, сохраняет уведомление со статусом pending и ставит его в очередь
// Сама отправка выполняется асинхронно в consumer
func (s *Server) SendNotification(ctx context.Context, req *notificationv1.SendNotificationRequest) (*notificationv1.SendNotificationResponse, error) {
	typ, err := validateSend(req)
	if err != nil {
		return nil, err
	}
	userID := req.GetUserId()

	// Лимит: если Redis недоступен, пропускаем запрос (fail-open): защита вспомогательная,
	// и ее сбой не должен останавливать основную функцию
	allowed, err := s.limiter.Allow(ctx, userID)
	switch {
	case err != nil:
		s.log.Warn("не удалось проверить лимит, запрос пропущен", "user_id", userID, "error", err)
	case !allowed:
		return nil, status.Error(codes.ResourceExhausted, "превышен лимит уведомлений для пользователя")
	}

	// Сначала база, потом очередь: иначе consumer мог бы получить событие раньше,
	// чем появится запись, не найти ее и пропустить событие
	n, err := s.store.Create(ctx, userID, typ, req.GetPayload())
	if err != nil {
		return nil, s.internalErr(ctx, "не удалось сохранить уведомление", err, "user_id", userID)
	}

	if err := s.publisher.Publish(ctx, kafka.NewEvent(n)); err != nil {
		// Запись уже в базе со статусом pending: без пометки она осталась бы такой навсегда
		s.markFailed(ctx, n.ID, "не удалось поставить в очередь")

		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, status.FromContextError(ctxErr).Err()
		}
		s.log.Error("не удалось опубликовать событие", "id", n.ID, "error", err)
		return nil, status.Error(codes.Unavailable, "очередь недоступна, повторите запрос позже")
	}

	return &notificationv1.SendNotificationResponse{
		Id:     n.ID,
		Status: notificationv1.NotificationStatus_NOTIFICATION_STATUS_PENDING,
	}, nil
}

// GetNotification возвращает уведомление: сначала из кеша, при промахе из PostgreSQL
func (s *Server) GetNotification(ctx context.Context, req *notificationv1.GetNotificationRequest) (*notificationv1.GetNotificationResponse, error) {
	id := req.GetId()
	if err := validateID(id); err != nil {
		return nil, err
	}

	// Кеш нужен только для ускорения, а данные хранятся в PostgreSQL
	// При сбое кеша читаем из базы, а не возвращаем ошибку клиенту
	cached, found, err := s.cache.Get(ctx, id)
	if err != nil {
		s.log.Warn("кеш недоступен, читаем из PostgreSQL", "id", id, "error", err)
	} else if found {
		return &notificationv1.GetNotificationResponse{Notification: notificationToProto(cached)}, nil
	}

	n, err := s.store.GetByID(ctx, id)
	switch {
	case errors.Is(err, storage.ErrNotFound):
		return nil, status.Error(codes.NotFound, "уведомление не найдено")
	case err != nil:
		return nil, s.internalErr(ctx, "не удалось прочитать уведомление", err, "id", id)
	}

	// Кладем в кеш на будущее, ошибка записи в кеш не мешает ответить клиенту
	if err := s.cache.Set(ctx, n); err != nil {
		s.log.Warn("не удалось записать в кеш", "id", id, "error", err)
	}

	return &notificationv1.GetNotificationResponse{Notification: notificationToProto(n)}, nil
}

// markFailed помечает уведомление как failed
// Запрос мог быть отменен клиентом, поэтому берем контекст без отмены, но с таймаутом
func (s *Server) markFailed(ctx context.Context, id, reason string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), markFailedTimeout)
	defer cancel()

	if err := s.store.UpdateStatus(ctx, id, storage.StatusFailed, reason); err != nil {
		s.log.Error("не удалось пометить уведомление как failed", "id", id, "error", err)
	}
}

// internalErr превращает внутреннюю ошибку в gRPC-статус
// Подробности пишем в лог, а клиенту отдаем общий текст: в деталях могут быть имена таблиц и адреса
// Если запрос отменен клиентом или вышел дедлайн, это не внутренняя ошибка
func (s *Server) internalErr(ctx context.Context, msg string, err error, attrs ...any) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return status.FromContextError(ctxErr).Err()
	}
	s.log.Error(msg, append(attrs, "error", err)...)
	return status.Error(codes.Internal, "внутренняя ошибка сервиса")
}
