package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// Producer публикует события в Kafka
type Producer struct {
	writer *kafkago.Writer
}

// NewProducer создает producer
// Соединения устанавливаются при первой записи
// Один producer на весь сервис: он безопасен для использования из нескольких горутин
func NewProducer(brokers []string) *Producer {
	return &Producer{
		writer: &kafkago.Writer{
			Addr: kafkago.TCP(brokers...),
			// Топик не задан здесь: он указывается в каждом сообщении (email или push)
			// Hash выбирает партицию по хэшу ключа: один ключ, одна партиция, порядок сохраняется
			Balancer: &kafkago.Hash{},
			// Подтверждение от всех реплик: запись не теряется при сбое брокера
			RequiredAcks: kafkago.RequireAll,
			// По умолчанию writer копит пачку до 1 секунды
			// Для синхронного Publish это лишняя секунда на каждый вызов, поэтому ждем минимум
			BatchTimeout: 10 * time.Millisecond,
			WriteTimeout: 10 * time.Second,
		},
	}
}

// Publish отправляет событие в топик, выбранный по типу уведомления
// Метод блокируется, пока брокер не подтвердит запись: nil означает, что сообщение сохранено
func (p *Producer) Publish(ctx context.Context, e Event) error {
	topic, err := TopicFor(e.Type)
	if err != nil {
		return err
	}

	value, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("сериализация события %s: %w", e.ID, err)
	}

	err = p.writer.WriteMessages(ctx, kafkago.Message{
		Topic: topic,
		Key:   []byte(strconv.FormatInt(e.UserID, 10)),
		Value: value,
	})
	if err != nil {
		return fmt.Errorf("публикация события %s в %s: %w", e.ID, topic, err)
	}
	return nil
}

// Close закрывает producer и освобождает ресурсы
// Публикации после закрытия невозможны, поэтому вызывать его нужно в последнюю очередь
func (p *Producer) Close() error {
	return p.writer.Close()
}
