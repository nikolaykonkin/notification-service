package kafka

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/nikolaykonkin/notification-service/internal/storage"
)

// recorder передает полученные события в канал, чтобы тест мог их проверить
type recorder struct {
	events chan Event
}

func newRecorder() *recorder {
	return &recorder{events: make(chan Event, 100)}
}

func (r *recorder) Handle(ctx context.Context, e Event) error {
	select {
	case r.events <- e:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// startConsumer запускает consumer с уникальной группой в отдельной горутине
// и возвращает функцию остановки, которая возвращает результат Run
func startConsumer(t *testing.T, brokers []string, h Handler) (stop func() error) {
	t.Helper()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := NewConsumer(brokers, uniqueID("test-group"), h, log)
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel) // сработает раньше Close: cleanup выполняются в обратном порядке

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	return func() error {
		cancel()
		return <-done
	}
}

// waitForEvent ждет событие с нужным id
// События из прошлых запусков тестов (они остаются в общих топиках) пропускаем
func waitForEvent(ctx context.Context, t *testing.T, r *recorder, id string) Event {
	t.Helper()
	for {
		select {
		case e := <-r.events:
			if e.ID == id {
				return e
			}
		case <-ctx.Done():
			t.Fatalf("не дождались события %s: %v", id, ctx.Err())
		}
	}
}

func TestConsumer_ReceivesPublishedEvent(t *testing.T) {
	brokers := testBrokers(t)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	rec := newRecorder()
	stop := startConsumer(t, brokers, rec)

	p := NewProducer(brokers)
	t.Cleanup(func() { _ = p.Close() })

	want := Event{
		ID:      uniqueID("test"),
		UserID:  7,
		Type:    storage.TypePush,
		Payload: json.RawMessage(`{"title":"hi"}`),
	}
	if err := p.Publish(ctx, want); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	got := waitForEvent(ctx, t, rec, want.ID)
	if got.UserID != want.UserID || got.Type != want.Type || string(got.Payload) != string(want.Payload) {
		t.Errorf("событие изменилось:\n получено: %+v\n ожидалось: %+v", got, want)
	}

	// Штатная остановка по отмене контекста не должна быть ошибкой
	if err := stop(); err != nil {
		t.Errorf("Run вернул ошибку при остановке: %v", err)
	}
}

func TestConsumer_SkipsInvalidMessage(t *testing.T) {
	brokers := testBrokers(t)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	rec := newRecorder()
	stop := startConsumer(t, brokers, rec)

	// Сначала кладем мусор напрямую, в обход Producer
	raw := &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Topic:        TopicEmail,
		BatchTimeout: 10 * time.Millisecond,
	}
	t.Cleanup(func() { _ = raw.Close() })
	if err := raw.WriteMessages(ctx, kafkago.Message{Value: []byte("this is not json")}); err != nil {
		t.Fatalf("запись мусора: %v", err)
	}

	// Затем корректное событие: consumer не должен застрять на мусоре
	p := NewProducer(brokers)
	t.Cleanup(func() { _ = p.Close() })

	want := Event{ID: uniqueID("test"), UserID: 1, Type: storage.TypeEmail, Payload: json.RawMessage(`{}`)}
	if err := p.Publish(ctx, want); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	waitForEvent(ctx, t, rec, want.ID)

	if err := stop(); err != nil {
		t.Errorf("Run вернул ошибку: %v", err)
	}
}
