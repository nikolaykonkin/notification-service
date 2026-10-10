// Пакет ratelimit ограничивает число уведомлений на пользователя (на базе Redis)
package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultWindow это длина окна ограничения: лимит считается "в час"
const DefaultWindow = time.Hour

// Limiter реализует ограничение "не больше limit попыток за window на пользователя"
// (алгоритм fixed window на счетчике Redis)
type Limiter struct {
	client *redis.Client
	limit  int
	window time.Duration
}

// New создает ограничитель
func New(client *redis.Client, limit int, window time.Duration) *Limiter {
	return &Limiter{client: client, limit: limit, window: window}
}

// key строит ключ счетчика пользователя
func (l *Limiter) key(userID int64) string {
	return fmt.Sprintf("ratelimit:user:%d", userID)
}

// Allow регистрирует попытку и сообщает, укладывается ли пользователь в лимит
// Отклоненная попытка тоже увеличивает счетчик: это безвредно, он сбросится вместе с окном
// При ошибке Redis возвращает ошибку: что делать дальше, решает вызывающий код
func (l *Limiter) Allow(ctx context.Context, userID int64) (bool, error) {
	key := l.key(userID)

	// INCR и EXPIRE NX отправляются в одной транзакции (MULTI/EXEC):
	// между ними нет чужих команд, и ключ не останется без TTL
	// NX ставит TTL только если его еще нет, поэтому окно не продлевается
	// последующими попытками и начинается с первой
	var count *redis.IntCmd
	_, err := l.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		count = pipe.Incr(ctx, key)
		pipe.ExpireNX(ctx, key, l.window)
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("проверка лимита для пользователя %d: %w", userID, err)
	}

	return count.Val() <= int64(l.limit), nil
}
