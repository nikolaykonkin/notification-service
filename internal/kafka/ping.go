package kafka

import (
	"context"
	"errors"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"
)

// Ping проверяет, что хотя бы к одному брокеру из списка можно подключиться по TCP
// Reader и Writer подключаются к Kafka при первом обращении, а Reader при недоступности
// брокера сам повторяет попытки и не возвращает ошибку наружу
// Без этой проверки сервис запустился бы при выключенной Kafka и молча простаивал
func Ping(ctx context.Context, brokers []string) error {
	if len(brokers) == 0 {
		return errors.New("список брокеров пуст")
	}

	var errs []error
	for _, addr := range brokers {
		conn, err := kafkago.DialContext(ctx, "tcp", addr)
		if err != nil {
			errs = append(errs, fmt.Errorf("брокер %s: %w", addr, err))
			continue
		}
		_ = conn.Close()
		return nil // достаточно одного доступного брокера
	}
	return fmt.Errorf("нет доступных брокеров Kafka: %w", errors.Join(errs...))
}
