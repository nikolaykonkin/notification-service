package kafka

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestPing_Success(t *testing.T) {
	brokers := testBrokers(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := Ping(ctx, brokers); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestPing_SkipsUnavailableBroker(t *testing.T) {
	brokers := testBrokers(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Первый адрес недоступен, второй рабочий: проверка должна пройти
	if err := Ping(ctx, append([]string{"localhost:1"}, brokers...)); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestPing_AllBrokersUnavailable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// На портах 1 и 2 никто не слушает: соединение отклоняется сразу
	err := Ping(ctx, []string{"localhost:1", "localhost:2"})
	if err == nil {
		t.Fatal("ожидалась ошибка")
	}
	// В ошибке должны быть названы оба брокера
	for _, addr := range []string{"localhost:1", "localhost:2"} {
		if !strings.Contains(err.Error(), addr) {
			t.Errorf("в ошибке нет %s: %v", addr, err)
		}
	}
}

func TestPing_EmptyList(t *testing.T) {
	if err := Ping(context.Background(), nil); err == nil {
		t.Fatal("ожидалась ошибка для пустого списка")
	}
}
