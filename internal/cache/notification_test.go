package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nikolaykonkin/notification-service/internal/storage"
)

// testNotification создает уведомление с уникальным id, чтобы тесты не мешали друг другу
func testNotification() storage.Notification {
	now := time.Now().UTC().Truncate(time.Millisecond)
	return storage.Notification{
		ID:        fmt.Sprintf("test-%d", time.Now().UnixNano()),
		UserID:    42,
		Type:      storage.TypeEmail,
		Status:    storage.StatusPending,
		Payload:   `{"to": "user@example.com"}`,
		Error:     "",
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// assertSame сравнивает уведомления; время сравниваем через Equal, а не ==
func assertSame(t *testing.T, got, want storage.Notification) {
	t.Helper()
	if got.ID != want.ID || got.UserID != want.UserID || got.Type != want.Type ||
		got.Status != want.Status || got.Payload != want.Payload || got.Error != want.Error ||
		!got.CreatedAt.Equal(want.CreatedAt) || !got.UpdatedAt.Equal(want.UpdatedAt) {
		t.Errorf("записи различаются:\n получено: %+v\n ожидалось: %+v", got, want)
	}
}

// Этот тест не требует Redis: проверяет, что JSON-преобразование ничего не теряет
func TestCachedNotification_JSONRoundTrip(t *testing.T) {
	want := testNotification()
	want.Status = storage.StatusFailed
	want.Error = "smtp timeout"

	data, err := json.Marshal(fromNotification(want))
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var v cachedNotification
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	assertSame(t, v.toNotification(), want)
}

// newTestCache подключается к Redis или пропускает тест
func newTestCache(t *testing.T, ttl time.Duration) (*NotificationCache, *redis.Client) {
	t.Helper()

	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		t.Skip("TEST_REDIS_URL не задан: тесты Redis пропущены")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := NewClient(ctx, url)
	if err != nil {
		t.Fatalf("подключение к Redis: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	return NewNotificationCache(client, ttl), client
}

func TestNotificationCache_SetAndGet(t *testing.T) {
	c, client := newTestCache(t, time.Minute)
	ctx := context.Background()

	want := testNotification()
	t.Cleanup(func() { client.Del(context.Background(), key(want.ID)) })

	if err := c.Set(ctx, want); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, found, err := c.Get(ctx, want.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("запись должна быть найдена")
	}
	assertSame(t, got, want)
}

func TestNotificationCache_Miss(t *testing.T) {
	c, _ := newTestCache(t, time.Minute)

	_, found, err := c.Get(context.Background(), "test-no-such-id")
	if err != nil {
		t.Fatalf("промах не должен быть ошибкой: %v", err)
	}
	if found {
		t.Error("запись не должна быть найдена")
	}
}

func TestNotificationCache_Delete(t *testing.T) {
	c, _ := newTestCache(t, time.Minute)
	ctx := context.Background()

	n := testNotification()
	if err := c.Set(ctx, n); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := c.Delete(ctx, n.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, found, _ := c.Get(ctx, n.ID); found {
		t.Error("запись должна быть удалена")
	}

	// Повторное удаление несуществующего ключа не ошибка
	if err := c.Delete(ctx, n.ID); err != nil {
		t.Errorf("повторный Delete вернул ошибку: %v", err)
	}
}

func TestNotificationCache_SetsTTL(t *testing.T) {
	const ttl = 2 * time.Minute
	c, client := newTestCache(t, ttl)
	ctx := context.Background()

	n := testNotification()
	t.Cleanup(func() { client.Del(context.Background(), key(n.ID)) })

	if err := c.Set(ctx, n); err != nil {
		t.Fatalf("Set: %v", err)
	}
	// Redis сам считает оставшееся время жизни ключа
	got := client.TTL(ctx, key(n.ID)).Val()
	if got <= 0 || got > ttl {
		t.Errorf("TTL должен быть в диапазоне (0, %v], получено %v", ttl, got)
	}
}
