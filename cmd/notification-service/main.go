// Команда notification-service запускает сервис уведомлений
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/nikolaykonkin/notification-service/internal/api"
	"github.com/nikolaykonkin/notification-service/internal/cache"
	"github.com/nikolaykonkin/notification-service/internal/config"
	"github.com/nikolaykonkin/notification-service/internal/httpserver"
	"github.com/nikolaykonkin/notification-service/internal/kafka"
	"github.com/nikolaykonkin/notification-service/internal/metrics"
	"github.com/nikolaykonkin/notification-service/internal/processor"
	"github.com/nikolaykonkin/notification-service/internal/ratelimit"
	"github.com/nikolaykonkin/notification-service/internal/storage"
	notificationv1 "github.com/nikolaykonkin/notification-service/proto/notification/v1"
)

const (
	// consumerGroup это имя consumer group: экземпляры с одним именем делят партиции
	consumerGroup = "notification-service"
	// sendDelay это пауза, которой эмулируется отправка уведомления
	sendDelay = 500 * time.Millisecond
	// startupTimeout ограничивает подключение к PostgreSQL, Redis и Kafka на старте
	startupTimeout = 15 * time.Second
	// healthCheckTimeout это общее время на проверки зависимостей в /health
	healthCheckTimeout = 2 * time.Second
	// grpcStopTimeout и httpStopTimeout это время, которое серверы ждут завершения текущих запросов
	// Docker после SIGTERM ждет 10 секунд и затем убивает процесс,
	// поэтому в сумме (5 + 2) укладываемся в меньшее
	grpcStopTimeout = 5 * time.Second
	httpStopTimeout = 2 * time.Second
)

func main() {
	// JSON-логгер: логи удобно парсить, пишем в stdout (так принято для контейнеров)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// Fail-fast: если настройки некорректны, завершаемся сразу с кодом 1
	cfg, err := config.Load()
	if err != nil {
		slog.Error("не удалось загрузить конфигурацию", "error", err)
		os.Exit(1)
	}

	// Логируем только безопасные поля: DATABASE_URL содержит пароль
	slog.Info("configuration loaded",
		"grpc_port", cfg.GRPCPort,
		"http_port", cfg.HTTPPort,
		"rate_limit_per_hour", cfg.RateLimitPerHour,
	)

	// Контекст отменяется по SIGINT (Ctrl+C) и SIGTERM (docker stop, оркестратор)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	go func() {
		// После первого сигнала возвращаем обычное поведение: повторный Ctrl+C
		// во время остановки завершит процесс сразу
		<-ctx.Done()
		stop()
	}()

	err = run(ctx, cfg, logger)
	stop()

	// os.Exit не выполняет defer, поэтому вызываем его только здесь, после возврата из run
	if err != nil {
		slog.Error("сервис завершился с ошибкой", "error", err)
		os.Exit(1)
	}
	slog.Info("сервис остановлен")
}

// run собирает зависимости (composition root), запускает consumer, gRPC- и HTTP-серверы
// и ждет сигнала остановки или падения компонента
//
// Компоненты создаются в порядке зависимостей, и сразу после создания регистрируется defer
// Defer выполняются в обратном порядке, поэтому остановка идет в порядке, обратном запуску:
// HTTP-сервер, gRPC-сервер, consumer, producer, Redis, PostgreSQL
// Закрытие работает и при ошибке запуска
func run(ctx context.Context, cfg *config.Config, log *slog.Logger) error {
	// 1. Внешние системы
	// Проверяем доступность сразу (fail-fast), но не дольше startupTimeout
	startCtx, cancelStart := context.WithTimeout(ctx, startupTimeout)
	defer cancelStart()

	pool, err := storage.NewPool(startCtx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("подключение к PostgreSQL: %w", err)
	}
	defer func() {
		pool.Close() // ждет, пока все соединения вернутся в пул
		log.Info("остановлен", "component", "PostgreSQL")
	}()

	redisClient, err := cache.NewClient(startCtx, cfg.RedisURL)
	if err != nil {
		return fmt.Errorf("подключение к Redis: %w", err)
	}
	defer closeLog(log, "Redis", redisClient)

	if err := kafka.Ping(startCtx, cfg.KafkaBrokers); err != nil {
		return fmt.Errorf("подключение к Kafka: %w", err)
	}

	// 2. Компоненты
	m := metrics.New()
	repo := storage.NewRepository(pool)
	notificationCache := cache.NewNotificationCache(redisClient, cache.DefaultTTL)
	limiter := ratelimit.New(redisClient, cfg.RateLimitPerHour, ratelimit.DefaultWindow)

	producer := kafka.NewProducer(cfg.KafkaBrokers)
	defer closeLog(log, "Kafka producer", producer)

	// 3. Consumer: читает события и передает их процессору, который пишет метрики в m
	proc := processor.New(repo, notificationCache, processor.SimulatedSender{Delay: sendDelay}, m, log)
	consumer := kafka.NewConsumer(cfg.KafkaBrokers, consumerGroup, proc, log)

	consumerCtx, cancelConsumer := context.WithCancel(ctx)
	consumerTask := startTask(func() error { return consumer.Run(consumerCtx) })
	defer func() {
		// Сначала отменяем контекст и ждем возврата из Run, и только затем закрываем reader
		cancelConsumer()
		<-consumerTask.done
		closeLog(log, "Kafka consumer", consumer)
	}()

	// 4. gRPC-сервер
	// Порт занимаем сразу: если он занят, узнаем об этом на старте
	grpcLis, err := net.Listen("tcp", cfg.GRPCAddr())
	if err != nil {
		return fmt.Errorf("прослушивание %s: %w", cfg.GRPCAddr(), err)
	}

	grpcServer := grpc.NewServer()
	notificationv1.RegisterNotificationServiceServer(grpcServer,
		api.New(repo, notificationCache, limiter, producer, log))
	// Reflection позволяет grpcurl узнавать методы у самого сервера
	// В реальной эксплуатации его обычно отключают, потому что сервер раскрывает свою схему
	reflection.Register(grpcServer)

	grpcTask := startTask(func() error { return grpcServer.Serve(grpcLis) })
	defer func() {
		stopGRPC(grpcServer, grpcStopTimeout, log)
		<-grpcTask.done
		log.Info("остановлен", "component", "gRPC-сервер")
	}()

	// 5. HTTP-сервер: /metrics для Prometheus и /health для проверки состояния
	httpLis, err := net.Listen("tcp", cfg.HTTPAddr())
	if err != nil {
		return fmt.Errorf("прослушивание %s: %w", cfg.HTTPAddr(), err)
	}

	checks := []httpserver.Check{
		{Name: "postgresql", Fn: pool.Ping},
		{Name: "redis", Fn: func(ctx context.Context) error { return redisClient.Ping(ctx).Err() }},
	}
	httpSrv := httpserver.NewServer(httpserver.NewHandler(m.Handler(), checks, healthCheckTimeout, log))

	httpTask := startTask(func() error {
		// ErrServerClosed это штатный результат Shutdown, а не ошибка
		err := httpSrv.Serve(httpLis)
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	})
	defer func() {
		stopHTTP(httpSrv, httpStopTimeout, log)
		<-httpTask.done
		log.Info("остановлен", "component", "HTTP-сервер")
	}()

	log.Info("сервис запущен",
		"grpc_addr", cfg.GRPCAddr(),
		"http_addr", cfg.HTTPAddr(),
		"consumer_group", consumerGroup,
	)

	// 6. Ждем сигнала остановки или падения одного из компонентов
	select {
	case <-ctx.Done():
		log.Info("получен сигнал остановки, начинаем graceful shutdown")
		return nil
	case <-consumerTask.done:
		if ctx.Err() != nil {
			return nil // остановка по сигналу, consumer вышел штатно
		}
		return stopReason("consumer", consumerTask.err)
	case <-grpcTask.done:
		if ctx.Err() != nil {
			return nil
		}
		return stopReason("gRPC-сервер", grpcTask.err)
	case <-httpTask.done:
		if ctx.Err() != nil {
			return nil
		}
		return stopReason("HTTP-сервер", httpTask.err)
	}
}

// stopReason формирует ошибку для компонента, который остановился сам, без сигнала
func stopReason(name string, err error) error {
	if err == nil {
		return fmt.Errorf("%s неожиданно завершился без ошибки", name)
	}
	return fmt.Errorf("%s остановился: %w", name, err)
}

// closeLog закрывает компонент и пишет в лог результат
func closeLog(log *slog.Logger, name string, c io.Closer) {
	if err := c.Close(); err != nil {
		log.Error("ошибка при закрытии", "component", name, "error", err)
		return
	}
	log.Info("остановлен", "component", name)
}
