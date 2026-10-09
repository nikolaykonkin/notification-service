// Пакет cache содержит работу с Redis: подключение и кэш уведомлений
package cache

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// NewClient создает клиент Redis и проверяет, что сервер доступен (fail-fast)
// Клиент содержит пул соединений и безопасен для использования из нескольких горутин:
// на весь сервис нужен один клиент
// Закрывает его (Close) тот, кто создал
func NewClient(ctx context.Context, redisURL string) (*redis.Client, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		// Сам URL в ошибку не добавляем: в нем может быть пароль.
		return nil, fmt.Errorf("разбор REDIS_URL: %w", err)
	}

	client := redis.NewClient(opts)

	// Конструктор не обращается к серверу, поэтому проверяем связь явно.
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("проверка соединения с Redis: %w", err)
	}
	return client, nil
}
