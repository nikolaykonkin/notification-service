package processor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/nikolaykonkin/notification-service/internal/kafka"
	"github.com/nikolaykonkin/notification-service/internal/storage"
)

// Интерфейсы объявлены здесь, на стороне потребителя, и содержат только то, что нужно процессору
// Благодаря этому в тестах можно подставить заглушки,
// а реальные Repository и NotificationCache подходят под них без изменений

// StatusUpdater сохраняет результат обработки
type StatusUpdater interface {
	UpdateStatus(ctx context.Context, id string, status storage.Status, errMsg string) error
}

// CacheInvalidator сбрасывает устаревшую запись в кэше
type CacheInvalidator interface {
	Delete(ctx context.Context, id string) error
}

// Processor обрабатывает одно событие целиком
type Processor struct {
	store  StatusUpdater
	cache  CacheInvalidator
	sender Sender
	log    *slog.Logger
}

// New создает процессор
func New(store StatusUpdater, cache CacheInvalidator, sender Sender, log *slog.Logger) *Processor {
	return &Processor{store: store, cache: cache, sender: sender, log: log}
}

// Handle обрабатывает событие
// Возвращает ошибку только тогда, когда обработку нельзя считать завершенной, например,
// не удалось сохранить статус: consumer в этом случае не зафиксирует offset, и событие придет снова
func (p *Processor) Handle(ctx context.Context, e kafka.Event) error {
	// 1. Эмулируем отправку - неудачная отправка это нормальный результат, а не ошибка сервиса:
    //    записываем статус failed и причину, повторять событие не нужно
	status, errMsg := storage.StatusSent, ""
	if err := p.sender.Send(ctx, e); err != nil {
		if ctx.Err() != nil {
			// Сервис останавливается: статус не меняем, событие обработается после перезапуска
			return ctx.Err()
		}
		status, errMsg = storage.StatusFailed, err.Error()
	}

	// 2. Сохраняем результат в PostgreSQL
	err := p.store.UpdateStatus(ctx, e.ID, status, errMsg)
	switch {
	case errors.Is(err, storage.ErrNotFound):
		// Повторять бессмысленно: такого уведомления нет - пропускаем
		p.log.Warn("уведомление не найдено, событие пропущено", "id", e.ID)
		return nil
	case err != nil:
		return fmt.Errorf("сохранение статуса %q для %s: %w", status, e.ID, err)
	}

	// 3. Сбрасываем кэш, чтобы читатели не видели устаревший статус pending
	//    Сбой кэша не критичен: запись исчезнет сама по TTL
	if err := p.cache.Delete(ctx, e.ID); err != nil {
		p.log.Warn("не удалось сбросить кэш", "id", e.ID, "error", err)
	}

	p.log.Info("уведомление обработано", "id", e.ID, "status", status)
	return nil
}
