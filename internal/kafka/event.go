// Пакет kafka содержит producer и consumer для обмена событиями через Kafka
// Библиотека kafka-go импортируется с алиасом kafkago, чтобы имя не конфликтовало с пакетом
package kafka

import (
	"encoding/json"
	"fmt"

	"github.com/nikolaykonkin/notification-service/internal/storage"
)

// Названия топиков - они совпадают с теми, что создает kafka-init в docker-compose.yml
const (
	TopicEmail = "notifications.email"
	TopicPush  = "notifications.push"
)

// AllTopics возвращает все топики, которые читает consumer
func AllTopics() []string {
	return []string{TopicEmail, TopicPush}
}

// TopicFor выбирает топик по типу уведомления
func TopicFor(t storage.Type) (string, error) {
	switch t {
	case storage.TypeEmail:
		return TopicEmail, nil
	case storage.TypePush:
		return TopicPush, nil
	default:
		return "", fmt.Errorf("неизвестный тип уведомления %q", t)
	}
}

// Event - это сообщение, которое уходит в Kafka (в формате JSON)
type Event struct {
	ID      string          `json:"id"`      // id уведомления в PostgreSQL
	UserID  int64           `json:"user_id"` // он же ключ сообщения: порядок внутри пользователя
	Type    storage.Type    `json:"type"`
	Payload json.RawMessage `json:"payload"` // содержимое уведомления, вложенный JSON
}

// NewEvent собирает событие из сохраненного уведомления
func NewEvent(n storage.Notification) Event {
	return Event{
		ID:      n.ID,
		UserID:  n.UserID,
		Type:    n.Type,
		Payload: json.RawMessage(n.Payload),
	}
}
