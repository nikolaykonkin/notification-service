package integration

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nikolaykonkin/notification-service/internal/storage"
	notificationv1 "github.com/nikolaykonkin/notification-service/proto/notification/v1"
)

// Основной сценарий: уведомление принято как pending, обработано асинхронно,
// а устаревший pending из кеша сброшен
func TestSend_ProcessedAndCacheInvalidated(t *testing.T) {
	s := newStack(t)
	ctx := testCtx(t)

	// Задерживаем "отправку", чтобы состояние pending можно было наблюдать
	user := newUser()
	release := s.sender.Hold(user)
	t.Cleanup(release) // при падении теста consumer не должен остаться заблокированным

	sent, err := s.client.SendNotification(ctx, &notificationv1.SendNotificationRequest{
		UserId:  user,
		Type:    notificationv1.NotificationType_NOTIFICATION_TYPE_EMAIL,
		Payload: `{"to":"it@example.com"}`,
	})
	if err != nil {
		t.Fatalf("SendNotification: %v", err)
	}
	id := sent.GetId()
	if sent.GetStatus() != notificationv1.NotificationStatus_NOTIFICATION_STATUS_PENDING {
		t.Fatalf("после отправки ожидался pending, получено %v", sent.GetStatus())
	}

	// Пока "отправка" задержана, статус pending - чтение кладет его в кеш
	if got := s.get(t, id); got.GetStatus() != notificationv1.NotificationStatus_NOTIFICATION_STATUS_PENDING {
		t.Fatalf("до обработки ожидался pending, получено %v", got.GetStatus())
	}
	if _, found, err := s.cache.Get(ctx, id); err != nil || !found {
		t.Fatalf("pending должен лежать в кеше (found=%v, err=%v)", found, err)
	}

	// Разрешаем обработку
	// Ждем результат в PostgreSQL напрямую, а не через GetNotification:
	// чтение через кеш само могло бы положить в него устаревший pending
	release()
	eventually(t, "статус sent в PostgreSQL", func() bool {
		n, err := s.repo.GetByID(ctx, id)
		return err == nil && n.Status == storage.StatusSent
	})

	// Процессор должен сбросить устаревшую запись из кеша
	eventually(t, "сброс pending из кеша", func() bool {
		_, found, err := s.cache.Get(ctx, id)
		return err == nil && !found
	})

	// Клиент теперь видит актуальный статус, а не закешированный pending
	got := s.get(t, id)
	if got.GetStatus() != notificationv1.NotificationStatus_NOTIFICATION_STATUS_SENT || got.GetError() != "" {
		t.Errorf("ожидался sent без ошибки, получено %v (%q)", got.GetStatus(), got.GetError())
	}

	// Метрика отправленных выросла (запись в метрики идет после записи статуса)
	eventually(t, "метрика notifications_sent_total", func() bool {
		return s.metricValue(t, `notifications_sent_total{type="email"}`) >= 1
	})
}

// Сбой отправки: статус failed с причиной, метрика failed
func TestSend_FailedDelivery(t *testing.T) {
	s := newStack(t)
	ctx := testCtx(t)

	user := newUser()
	s.sender.FailWith(user, errors.New("smtp timeout"))

	sent, err := s.client.SendNotification(ctx, &notificationv1.SendNotificationRequest{
		UserId:  user,
		Type:    notificationv1.NotificationType_NOTIFICATION_TYPE_PUSH,
		Payload: `{"title":"it"}`,
	})
	if err != nil {
		t.Fatalf("SendNotification: %v", err)
	}
	id := sent.GetId()

	eventually(t, "статус failed в PostgreSQL", func() bool {
		n, err := s.repo.GetByID(ctx, id)
		return err == nil && n.Status == storage.StatusFailed
	})

	got := s.get(t, id)
	if got.GetStatus() != notificationv1.NotificationStatus_NOTIFICATION_STATUS_FAILED {
		t.Errorf("ожидался failed, получено %v", got.GetStatus())
	}
	if got.GetError() != "smtp timeout" {
		t.Errorf("причина ошибки: получено %q, ожидалось %q", got.GetError(), "smtp timeout")
	}

	// Заглушка ошибается только для этого пользователя, поэтому failed в этом процессе
	// появится только от этого теста
	eventually(t, "метрика notifications_failed_total", func() bool {
		return s.metricValue(t, `notifications_failed_total{type="push"}`) >= 1
	})
}

// Лимит: запрос сверх лимита отклоняется и не создает запись в базе
func TestSend_RateLimit(t *testing.T) {
	s := newStack(t)
	ctx := testCtx(t)

	user := newUser()
	req := &notificationv1.SendNotificationRequest{
		UserId:  user,
		Type:    notificationv1.NotificationType_NOTIFICATION_TYPE_EMAIL,
		Payload: `{}`,
	}

	for i := 1; i <= rateLimit; i++ {
		if _, err := s.client.SendNotification(ctx, req); err != nil {
			t.Fatalf("запрос %d из %d должен пройти: %v", i, rateLimit, err)
		}
	}

	_, err := s.client.SendNotification(ctx, req)
	if got := status.Code(err); got != codes.ResourceExhausted {
		t.Fatalf("запрос сверх лимита: получен код %v (%v), ожидался ResourceExhausted", got, err)
	}

	// Отклоненный запрос не должен оставить запись в PostgreSQL
	var count int64
	err = s.pool.QueryRow(ctx, "SELECT count(*) FROM notifications WHERE user_id = $1", user).Scan(&count)
	if err != nil {
		t.Fatalf("подсчет записей: %v", err)
	}
	if count != rateLimit {
		t.Errorf("в базе %d записей, ожидалось %d", count, rateLimit)
	}
}
