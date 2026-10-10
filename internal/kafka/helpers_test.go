package kafka

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// testBrokers возвращает адреса брокеров из TEST_KAFKA_BROKERS или пропускает тест
func testBrokers(t *testing.T) []string {
	t.Helper()

	raw := os.Getenv("TEST_KAFKA_BROKERS")
	if raw == "" {
		t.Skip("TEST_KAFKA_BROKERS не задан: тесты Kafka пропущены")
	}

	var brokers []string
	for _, b := range strings.Split(raw, ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	return brokers
}

// uniqueID возвращает уникальный id: так тест находит в общем топике только свои сообщения,
// а не оставшиеся от прошлых запусков
func uniqueID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}
