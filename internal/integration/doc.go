// Пакет integration содержит интеграционные тесты: настоящие PostgreSQL, Redis и Kafka,
// собранные вместе с реальными компонентами сервиса
// Обычного кода в пакете нет
// Тесты пропускаются, если не заданы TEST_DATABASE_URL, TEST_REDIS_URL и TEST_KAFKA_BROKERS
package integration
