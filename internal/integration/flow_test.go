package integration

import (
	"testing"

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
