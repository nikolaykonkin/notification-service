// Пакет config загружает настройки сервиса из переменных окружения
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Значения по умолчанию для некритичных настроек
const (
	defaultGRPCPort         = 50051
	defaultHTTPPort         = 8080
	defaultRateLimitPerHour = 10
)

// Config содержит все настройки сервиса
type Config struct {
	DatabaseURL      string   // строка подключения к PostgreSQL (содержит пароль, не логировать!)
	RedisURL         string   // адрес Redis
	KafkaBrokers     []string // адреса брокеров Kafka
	GRPCPort         int      // порт gRPC-сервера
	HTTPPort         int      // порт HTTP-сервера (/metrics и /health)
	RateLimitPerHour int      // сколько уведомлений в час можно отправить одному user_id
}

// GRPCAddr возвращает адрес для net.Listen, например ":50051"
func (c *Config) GRPCAddr() string {
	return fmt.Sprintf(":%d", c.GRPCPort)
}

// HTTPAddr возвращает адрес HTTP-сервера, например ":8080"
func (c *Config) HTTPAddr() string {
	return fmt.Sprintf(":%d", c.HTTPPort)
}

// Load читает настройки из окружения и проверяет их
// Если есть проблемы, возвращает все ошибки сразу (fail-fast на старте)
func Load() (*Config, error) {
	cfg := &Config{}
	var errs []error

	// Каждый вызов ниже читает одну переменную
	// Ошибку не обрабатываем сразу, а копим в errs, чтобы показать пользователю
	// все проблемы за один запуск
	var err error

	cfg.DatabaseURL, err = requiredString("DATABASE_URL")
	errs = appendErr(errs, err)

	cfg.RedisURL, err = requiredString("REDIS_URL")
	errs = appendErr(errs, err)

	cfg.KafkaBrokers, err = requiredList("KAFKA_BROKERS")
	errs = appendErr(errs, err)

	cfg.GRPCPort, err = port("GRPC_PORT", defaultGRPCPort)
	errs = appendErr(errs, err)

	cfg.HTTPPort, err = port("HTTP_PORT", defaultHTTPPort)
	errs = appendErr(errs, err)

	cfg.RateLimitPerHour, err = positiveInt("RATE_LIMIT_PER_HOUR", defaultRateLimitPerHour)
	errs = appendErr(errs, err)

	// errors.Join вернет nil, если ошибок нет, и объединенную ошибку иначе
	if joined := errors.Join(errs...); joined != nil {
		return nil, fmt.Errorf("некорректная конфигурация: %w", joined)
	}
	return cfg, nil
}

// appendErr добавляет ошибку в список, только если она не nil
func appendErr(errs []error, err error) []error {
	if err != nil {
		return append(errs, err)
	}
	return errs
}

// lookup возвращает значение переменной без пробелов по краям
// Пустая и незаданная переменная для нас одно и то же
func lookup(key string) string {
	return strings.TrimSpace(os.Getenv(key))
}

// requiredString читает обязательную строковую переменную
// Значение в текст ошибки не попадает: в нем может быть пароль
func requiredString(key string) (string, error) {
	v := lookup(key)
	if v == "" {
		return "", fmt.Errorf("%s: обязательная переменная не задана", key)
	}
	return v, nil
}

// requiredList читает обязательный список через запятую: "host1:9092, host2:9092"
func requiredList(key string) ([]string, error) {
	var items []string
	for _, part := range strings.Split(lookup(key), ",") {
		if p := strings.TrimSpace(part); p != "" {
			items = append(items, p)
		}
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("%s: обязательная переменная не задана или пуста", key)
	}
	return items, nil
}

// positiveInt читает целое число больше нуля; если переменной нет, возвращает def
func positiveInt(key string, def int) (int, error) {
	raw := lookup(key)
	if raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: ожидается целое число, получено %q", key, raw)
	}
	if n <= 0 {
		return 0, fmt.Errorf("%s: значение должно быть больше нуля, получено %d", key, n)
	}
	return n, nil
}

// port читает номер порта и проверяет допустимый диапазон
func port(key string, def int) (int, error) {
	n, err := positiveInt(key, def)
	if err != nil {
		return 0, err
	}
	if n > 65535 {
		return 0, fmt.Errorf("%s: порт должен быть в диапазоне 1-65535, получено %d", key, n)
	}
	return n, nil
}
