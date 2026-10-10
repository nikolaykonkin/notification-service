package kafka

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/nikolaykonkin/notification-service/internal/storage"
)

// readUntil читает топик с начала отдельным читателем (с уникальной группой)
// и возвращает первое сообщение с нужным id события, чужие сообщения пропускает
func readUntil(ctx context.Context, t *testing.T, brokers []string, topic, id string) kafkago.Message {
	t.Helper()

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     uniqueID("test-reader"),
		StartOffset: kafkago.FirstOffset,
	})
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("сообщение %s в топике %s не найдено: %v", id, topic, err)
		}
		var e Event
		if json.Unmarshal(msg.Value, &e) == nil && e.ID == id {
			return msg
		}
	}
}

func TestProducer_PublishRoutesByType(t *testing.T) {
	brokers := testBrokers(t)

	tests := []struct {
		name  string
		typ   storage.Type
		topic string
	}{
		{"email", storage.TypeEmail, TopicEmail},
		{"push", storage.TypePush, TopicPush},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewProducer(brokers)
			t.Cleanup(func() { _ = p.Close() })

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			want := Event{
				ID:      uniqueID("test"),
				UserID:  42,
				Type:    tt.typ,
				Payload: json.RawMessage(`{"k":"v"}`),
			}
			if err := p.Publish(ctx, want); err != nil {
				t.Fatalf("Publish: %v", err)
			}

			msg := readUntil(ctx, t, brokers, tt.topic, want.ID)
			if msg.Topic != tt.topic {
				t.Errorf("топик: получено %q, ожидалось %q", msg.Topic, tt.topic)
			}
			if string(msg.Key) != "42" {
				t.Errorf("ключ: получено %q, ожидалось \"42\"", msg.Key)
			}
		})
	}
}

func TestProducer_PublishUnknownType(t *testing.T) {
	// Брокер для этого теста не нужен: ошибка возникает до обращения к сети
	p := NewProducer([]string{"localhost:1"})
	t.Cleanup(func() { _ = p.Close() })

	err := p.Publish(context.Background(), Event{ID: "x", UserID: 1, Type: storage.Type("sms")})
	if err == nil {
		t.Fatal("ожидалась ошибка для неизвестного типа")
	}
}
