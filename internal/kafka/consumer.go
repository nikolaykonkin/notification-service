package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	kafkago "github.com/segmentio/kafka-go"
)

// Handler обрабатывает одно событие
// Реализация лежит в пакете processor: consumer ничего не знает ни о PostgreSQL, ни о Redis
type Handler interface {
	Handle(ctx context.Context, e Event) error
}

// Consumer читает события из обоих топиков в составе consumer group
type Consumer struct {
	reader  *kafkago.Reader
	handler Handler
	log     *slog.Logger
}

// NewConsumer создает consumer
// Экземпляры с одним groupID делят партиции между собой
func NewConsumer(brokers []string, groupID string, handler Handler, log *slog.Logger) *Consumer {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		GroupID: groupID,
		// Один читатель на оба топика
		GroupTopics: AllTopics(),
		// Если у группы еще нет сохраненного offset, читаем с начала
		StartOffset: kafkago.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10 << 20,
	})
	return &Consumer{reader: reader, handler: handler, log: log}
}

// Run читает и обрабатывает сообщения, пока не отменен ctx (штатная остановка, возвращает nil)
// или пока не произойдет ошибка, после которой продолжать нельзя
//
// Гарантия доставки: at-least-once
// Offset фиксируется ПОСЛЕ обработки, поэтому при сбое между обработкой и фиксацией
// событие придет повторно
func (c *Consumer) Run(ctx context.Context) error {
	for {
		// FetchMessage читает без автоматической фиксации offset
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("чтение сообщения из Kafka: %w", err)
		}

		if err := c.process(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil // остановка: offset не зафиксирован, событие придет после перезапуска
			}
			// Не фиксируем offset и останавливаемся
			// Offset хранит позицию всей партиции, поэтому фиксация следующего сообщения
			// пометила бы и это как обработанное, и оно было бы потеряно
			return err
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("фиксация offset: %w", err)
		}
	}
}

// process разбирает сообщение и передает его обработчику
// Возвращает nil, если сообщение обработано или заведомо необрабатываемо
// (в обоих случаях offset нужно зафиксировать)
func (c *Consumer) process(ctx context.Context, msg kafkago.Message) error {
	var e Event
	if err := json.Unmarshal(msg.Value, &e); err != nil {
		// Некорректное сообщение разобрать не получится при любом числе повторов
		// Пропускаем его, чтобы оно не блокировало остальные сообщения партиции
		c.log.Error("некорректное сообщение пропущено",
			"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "error", err)
		return nil
	}
	if e.ID == "" {
		c.log.Error("сообщение без id пропущено",
			"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)
		return nil
	}

	if err := c.handler.Handle(ctx, e); err != nil {
		return fmt.Errorf("обработка события %s: %w", e.ID, err)
	}
	return nil
}

// Close сообщает брокеру о выходе из группы и освобождает ресурсы
// Порядок остановки: отмена контекста, возврат из Run, затем Close
func (c *Consumer) Close() error {
	return c.reader.Close()
}
