// Пакет storage содержит репозиторий для хранения истории уведомлений в PostgreSQL
package storage

import (
	"errors"
	"time"
)

// ErrNotFound возвращается, если уведомления с таким id нет
var ErrNotFound = errors.New("уведомление не найдено")

// Type определяет канал доставки
type Type string

const (
	TypeEmail Type = "email"
	TypePush  Type = "push"
)

// Status описывает этап жизненного цикла уведомления
type Status string

const (
	StatusPending Status = "pending" // принято, еще не отправлено
	StatusSent    Status = "sent"    // отправлено
	StatusFailed  Status = "failed"  // отправка не удалась
)

// Notification это запись из таблицы notifications
type Notification struct {
	ID        string // UUID в текстовом виде
	UserID    int64
	Type      Type
	Status    Status
	Payload   string // JSON как текст
	Error     string // пустая строка, если ошибки нет
	CreatedAt time.Time
	UpdatedAt time.Time
}
