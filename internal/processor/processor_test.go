package processor

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/nikolaykonkin/notification-service/internal/kafka"
	"github.com/nikolaykonkin/notification-service/internal/storage"
)

// Заглушки: записывают, что с ними сделали, и возвращают заданную ошибку

type fakeStore struct {
	calls  int
	id     string
	status storage.Status
	errMsg string
	err    error
}

func (f *fakeStore) UpdateStatus(_ context.Context, id string, status storage.Status, errMsg string) error {
	f.calls++
	f.id, f.status, f.errMsg = id, status, errMsg
	return f.err
}

type fakeCache struct {
	deleted []string
	err     error
}

func (f *fakeCache) Delete(_ context.Context, id string) error {
	f.deleted = append(f.deleted, id)
	return f.err
}

type fakeSender struct{ err error }

func (f fakeSender) Send(context.Context, kafka.Event) error { return f.err }

func newProcessor(store *fakeStore, cache *fakeCache, sender Sender) *Processor {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(store, cache, sender, log)
}

var testEvent = kafka.Event{ID: "n-1", UserID: 7, Type: storage.TypeEmail}

func TestHandle_Success(t *testing.T) {
	store, cache := &fakeStore{}, &fakeCache{}
	p := newProcessor(store, cache, fakeSender{})

	if err := p.Handle(context.Background(), testEvent); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if store.id != "n-1" || store.status != storage.StatusSent || store.errMsg != "" {
		t.Errorf("статус сохранен неверно: %+v", store)
	}
	if len(cache.deleted) != 1 || cache.deleted[0] != "n-1" {
		t.Errorf("кэш должен быть сброшен для n-1, получено %v", cache.deleted)
	}
}

func TestHandle_SendFailureMarksFailed(t *testing.T) {
	store, cache := &fakeStore{}, &fakeCache{}
	p := newProcessor(store, cache, fakeSender{err: errors.New("smtp timeout")})

	// Сбой отправки это не ошибка обработки: событие считается обработанным
	if err := p.Handle(context.Background(), testEvent); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if store.status != storage.StatusFailed || store.errMsg != "smtp timeout" {
		t.Errorf("ожидались failed и текст ошибки, получено %+v", store)
	}
	if len(cache.deleted) != 1 {
		t.Errorf("кэш должен быть сброшен и для failed, получено %v", cache.deleted)
	}
}

func TestHandle_NotFoundIsSkipped(t *testing.T) {
	store, cache := &fakeStore{err: storage.ErrNotFound}, &fakeCache{}
	p := newProcessor(store, cache, fakeSender{})

	if err := p.Handle(context.Background(), testEvent); err != nil {
		t.Fatalf("ErrNotFound не должна быть ошибкой обработки: %v", err)
	}
	if len(cache.deleted) != 0 {
		t.Errorf("кэш не должен сбрасываться, получено %v", cache.deleted)
	}
}

func TestHandle_StoreErrorIsReturned(t *testing.T) {
	dbErr := errors.New("db down")
	store, cache := &fakeStore{err: dbErr}, &fakeCache{}
	p := newProcessor(store, cache, fakeSender{})

	err := p.Handle(context.Background(), testEvent)
	if !errors.Is(err, dbErr) {
		t.Fatalf("ожидалась ошибка db down, получено %v", err)
	}
	if len(cache.deleted) != 0 {
		t.Errorf("при ошибке базы кэш сбрасывать не нужно, получено %v", cache.deleted)
	}
}

func TestHandle_CacheErrorIsNotFatal(t *testing.T) {
	store, cache := &fakeStore{}, &fakeCache{err: errors.New("redis down")}
	p := newProcessor(store, cache, fakeSender{})

	if err := p.Handle(context.Background(), testEvent); err != nil {
		t.Fatalf("сбой кэша не должен ломать обработку: %v", err)
	}
}

func TestHandle_ContextCancelledDoesNotTouchStatus(t *testing.T) {
	store, cache := &fakeStore{}, &fakeCache{}
	p := newProcessor(store, cache, fakeSender{err: context.Canceled})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := p.Handle(ctx, testEvent); err == nil {
		t.Fatal("ожидалась ошибка при отмене контекста")
	}
	if store.calls != 0 {
		t.Errorf("статус не должен меняться при остановке, вызовов: %d", store.calls)
	}
}

func TestSimulatedSender(t *testing.T) {
	t.Run("ждет заданное время", func(t *testing.T) {
		start := time.Now()
		err := SimulatedSender{Delay: 50 * time.Millisecond}.Send(context.Background(), testEvent)
		if err != nil {
			t.Fatalf("Send: %v", err)
		}
		if time.Since(start) < 40*time.Millisecond {
			t.Error("отправка завершилась слишком быстро")
		}
	})

	t.Run("прерывается отменой контекста", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		start := time.Now()
		err := SimulatedSender{Delay: time.Minute}.Send(ctx, testEvent)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("ожидалась context.Canceled, получено %v", err)
		}
		if time.Since(start) > time.Second {
			t.Error("отмена должна прерывать ожидание сразу")
		}
	})
}
