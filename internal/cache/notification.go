package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nikolaykonkin/notification-service/internal/storage"
)

// DefaultTTL это время жизни записи в кэше
const DefaultTTL = 5 * time.Minute

// cachedNotification это формат хранения в Redis (JSON)
// Он отделен от storage.Notification: формат кэша не должен диктовать доменную структуру
type cachedNotification struct {
	ID        string    `json:"id"`
	UserID    int64     `json:"user_id"`
	Type      string    `json:"type"`
	Status    string    `json:"status"`
	Payload   string    `json:"payload"`
	Error     string    `json:"error"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func fromNotification(n storage.Notification) cachedNotification {
	return cachedNotification{
		ID:        n.ID,
		UserID:    n.UserID,
		Type:      string(n.Type),
		Status:    string(n.Status),
		Payload:   n.Payload,
		Error:     n.Error,
		CreatedAt: n.CreatedAt,
		UpdatedAt: n.UpdatedAt,
	}
}

func (c cachedNotification) toNotification() storage.Notification {
	return storage.Notification{
		ID:        c.ID,
		UserID:    c.UserID,
		Type:      storage.Type(c.Type),
		Status:    storage.Status(c.Status),
		Payload:   c.Payload,
		Error:     c.Error,
		CreatedAt: c.CreatedAt,
		UpdatedAt: c.UpdatedAt,
	}
}

// NotificationCache кэширует уведомления по id (схема cache-aside)
type NotificationCache struct {
	client *redis.Client
	ttl    time.Duration
}

// NewNotificationCache создает кэш с заданным временем жизни записей
func NewNotificationCache(client *redis.Client, ttl time.Duration) *NotificationCache {
	return &NotificationCache{client: client, ttl: ttl}
}

// key строит ключ Redis
// Префикс отделяет наши ключи от чужих
func key(id string) string {
	return "notification:" + id
}

// Get возвращает уведомление из кэша
// Если записи нет, found = false и err = nil: промах это не ошибка
func (c *NotificationCache) Get(ctx context.Context, id string) (n storage.Notification, found bool, err error) {
	data, err := c.client.Get(ctx, key(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return storage.Notification{}, false, nil
	}
	if err != nil {
		return storage.Notification{}, false, fmt.Errorf("чтение из кэша: %w", err)
	}

	var v cachedNotification
	if err := json.Unmarshal(data, &v); err != nil {
		return storage.Notification{}, false, fmt.Errorf("разбор записи кэша: %w", err)
	}
	return v.toNotification(), true, nil
}

// Set кладет уведомление в кэш с TTL
// Существующая запись перезаписывается
func (c *NotificationCache) Set(ctx context.Context, n storage.Notification) error {
	data, err := json.Marshal(fromNotification(n))
	if err != nil {
		return fmt.Errorf("сериализация записи кэша: %w", err)
	}
	if err := c.client.Set(ctx, key(n.ID), data, c.ttl).Err(); err != nil {
		return fmt.Errorf("запись в кэш: %w", err)
	}
	return nil
}

// Delete удаляет запись (инвалидация)
// Отсутствие ключа не считается ошибкой
func (c *NotificationCache) Delete(ctx context.Context, id string) error {
	if err := c.client.Del(ctx, key(id)).Err(); err != nil {
		return fmt.Errorf("удаление из кэша: %w", err)
	}
	return nil
}
