package config

import (
	"slices"
	"strings"
	"testing"
)

// Все переменные, которые читает Load
var allKeys = []string{
	"DATABASE_URL", "REDIS_URL", "KAFKA_BROKERS",
	"GRPC_PORT", "HTTP_PORT", "RATE_LIMIT_PER_HOUR",
}

// setEnv сначала очищает все переменные конфига, затем выставляет нужные
// t.Setenv сам вернет исходные значения после завершения теста
func setEnv(t *testing.T, env map[string]string) {
	t.Helper()
	for _, k := range allKeys {
		t.Setenv(k, "")
	}
	for k, v := range env {
		t.Setenv(k, v)
	}
}

// requiredEnv возвращает минимальный набор обязательных переменных
func requiredEnv() map[string]string {
	return map[string]string{
		"DATABASE_URL":  "postgres://u:p@localhost:5432/db",
		"REDIS_URL":     "redis://localhost:6379/0",
		"KAFKA_BROKERS": "localhost:9092",
	}
}

func TestLoad_Defaults(t *testing.T) {
	setEnv(t, requiredEnv())

	cfg, err := Load()
	if err != nil {
		t.Fatalf("неожиданная ошибка: %v", err)
	}
	if cfg.GRPCPort != 50051 || cfg.HTTPPort != 8080 || cfg.RateLimitPerHour != 10 {
		t.Errorf("значения по умолчанию неверны: %+v", cfg)
	}
	if cfg.GRPCAddr() != ":50051" || cfg.HTTPAddr() != ":8080" {
		t.Errorf("адреса неверны: %s, %s", cfg.GRPCAddr(), cfg.HTTPAddr())
	}
}

func TestLoad_CustomValues(t *testing.T) {
	env := requiredEnv()
	env["KAFKA_BROKERS"] = "k1:9092, k2:9092 ,,"
	env["GRPC_PORT"] = "6000"
	env["HTTP_PORT"] = "6001"
	env["RATE_LIMIT_PER_HOUR"] = "3"
	setEnv(t, env)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("неожиданная ошибка: %v", err)
	}
	// Пробелы и пустые элементы списка должны быть отброшены
	if want := []string{"k1:9092", "k2:9092"}; !slices.Equal(cfg.KafkaBrokers, want) {
		t.Errorf("брокеры: получено %v, ожидалось %v", cfg.KafkaBrokers, want)
	}
	if cfg.GRPCPort != 6000 || cfg.HTTPPort != 6001 || cfg.RateLimitPerHour != 3 {
		t.Errorf("значения неверны: %+v", cfg)
	}
}

func TestLoad_MissingRequired(t *testing.T) {
	setEnv(t, nil) // ничего не задано

	cfg, err := Load()
	if err == nil {
		t.Fatalf("ожидалась ошибка, получен конфиг %+v", cfg)
	}
	// Должны быть названы все три обязательные переменные сразу
	for _, key := range []string{"DATABASE_URL", "REDIS_URL", "KAFKA_BROKERS"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("в ошибке нет %s: %v", key, err)
		}
	}
}

func TestLoad_InvalidValues(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{"порт не число", "GRPC_PORT", "abc"},
		{"порт ноль", "GRPC_PORT", "0"},
		{"порт отрицательный", "HTTP_PORT", "-1"},
		{"порт слишком большой", "HTTP_PORT", "70000"},
		{"лимит не число", "RATE_LIMIT_PER_HOUR", "много"},
		{"лимит ноль", "RATE_LIMIT_PER_HOUR", "0"},
		{"брокеры только запятые", "KAFKA_BROKERS", " , ,"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := requiredEnv()
			env[tt.key] = tt.value
			setEnv(t, env)

			_, err := Load()
			if err == nil {
				t.Fatal("ожидалась ошибка")
			}
			if !strings.Contains(err.Error(), tt.key) {
				t.Errorf("в ошибке нет имени переменной %s: %v", tt.key, err)
			}
		})
	}
}
