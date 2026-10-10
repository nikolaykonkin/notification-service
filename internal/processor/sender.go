// Пакет processor содержит бизнес-логику обработки событий:
// "отправить" уведомление, сохранить статус и сбросить кэш
package processor

import (
	"context"
	"time"

	"github.com/nikolaykonkin/notification-service/internal/kafka"
)

// Sender отправляет уведомление получателю
// Реальных отправителей у нас нет, интерфейс нужен, чтобы подменять
// отправку в тестах и позже заменить эмуляцию
type Sender interface {
	Send(ctx context.Context, e kafka.Event) error
}

// SimulatedSender эмулирует отправку: просто ждет Delay
type SimulatedSender struct {
	Delay time.Duration
}

// Send ждет Delay или отмены контекста
// time.Sleep здесь не подходит: его нельзя прервать,
// и при остановке сервиса мы бы ждали окончания паузы
func (s SimulatedSender) Send(ctx context.Context, _ kafka.Event) error {
	timer := time.NewTimer(s.Delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
