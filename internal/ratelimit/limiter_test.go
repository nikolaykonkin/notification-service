package ratelimit

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// newTestLimiter подключается к Redis или пропускает тест
// Пользователь выбирается уникальным, а его ключ удаляется после теста
func newTestLimiter(t *testing.T, limit int, window time.Duration) (*Limiter, *redis.Client, int64) {
	t.Helper()

	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		t.Skip("TEST_REDIS_URL не задан: тесты Redis пропущены")
	}

	opts, err := redis.ParseURL(url)
	if err != nil {
		t.Fatalf("разбор TEST_REDIS_URL: %v", err)
	}
	client := redis.NewClient(opts)
	t.Cleanup(func() { _ = client.Close() })

	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("подключение к Redis: %v", err)
	}

	l := New(client, limit, window)
	userID := time.Now().UnixNano()
	t.Cleanup(func() { client.Del(context.Background(), l.key(userID)) })

	return l, client, userID
}

func TestLimiter_AllowsUpToLimit(t *testing.T) {
	const limit = 3
	l, _, userID := newTestLimiter(t, limit, time.Minute)
	ctx := context.Background()

	for i := 1; i <= limit; i++ {
		ok, err := l.Allow(ctx, userID)
		if err != nil {
			t.Fatalf("Allow #%d: %v", i, err)
		}
		if !ok {
			t.Errorf("попытка #%d должна быть разрешена", i)
		}
	}

	// Следующая попытка превышает лимит
	ok, err := l.Allow(ctx, userID)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if ok {
		t.Error("попытка сверх лимита должна быть отклонена")
	}
}

func TestLimiter_UsersAreIndependent(t *testing.T) {
	l, client, userA := newTestLimiter(t, 1, time.Minute)
	ctx := context.Background()

	userB := userA + 1
	t.Cleanup(func() { client.Del(context.Background(), l.key(userB)) })

	if ok, _ := l.Allow(ctx, userA); !ok {
		t.Fatal("первая попытка пользователя A должна пройти")
	}
	if ok, _ := l.Allow(ctx, userA); ok {
		t.Error("вторая попытка пользователя A должна быть отклонена")
	}
	// Лимит пользователя A не влияет на пользователя B
	if ok, _ := l.Allow(ctx, userB); !ok {
		t.Error("первая попытка пользователя B должна пройти")
	}
}

func TestLimiter_SetsExpiration(t *testing.T) {
	const window = time.Minute
	l, client, userID := newTestLimiter(t, 5, window)
	ctx := context.Background()

	if _, err := l.Allow(ctx, userID); err != nil {
		t.Fatalf("Allow: %v", err)
	}
	// У счетчика обязательно должен быть TTL, иначе он останется навсегда
	ttl := client.TTL(ctx, l.key(userID)).Val()
	if ttl <= 0 || ttl > window {
		t.Errorf("TTL должен быть в диапазоне (0, %v], получено %v", window, ttl)
	}
}

func TestLimiter_ResetsAfterWindow(t *testing.T) {
	l, _, userID := newTestLimiter(t, 1, time.Second)
	ctx := context.Background()

	if ok, _ := l.Allow(ctx, userID); !ok {
		t.Fatal("первая попытка должна пройти")
	}
	if ok, _ := l.Allow(ctx, userID); ok {
		t.Fatal("вторая попытка в том же окне должна быть отклонена")
	}

	// Ждем истечения окна: счетчик исчезнет, и лимит начнется заново
	time.Sleep(1200 * time.Millisecond)

	if ok, _ := l.Allow(ctx, userID); !ok {
		t.Error("после окончания окна попытка должна пройти")
	}
}
