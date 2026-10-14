package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestStartTask_ReturnsResult(t *testing.T) {
	want := errors.New("boom")
	tk := startTask(func() error { return want })

	select {
	case <-tk.done:
	case <-time.After(time.Second):
		t.Fatal("задача не завершилась")
	}
	if !errors.Is(tk.err, want) {
		t.Errorf("ошибка: получено %v, ожидалась %v", tk.err, want)
	}
}

func TestStartTask_DoneWaitsForFunction(t *testing.T) {
	release := make(chan struct{})
	tk := startTask(func() error {
		<-release
		return nil
	})

	select {
	case <-tk.done:
		t.Fatal("done закрыт до завершения функции")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case <-tk.done:
	case <-time.After(time.Second):
		t.Fatal("done не закрылся после завершения функции")
	}
}

// newHealthServer поднимает gRPC-сервер со стандартным health-сервисом через bufconn
// Его метод Watch это серверный стрим: он остается открытым, пока клиент не уйдет
func newHealthServer(t *testing.T) (*grpc.Server, healthpb.HealthClient) {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	healthpb.RegisterHealthServer(srv, health.NewServer())
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("создание клиента: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return srv, healthpb.NewHealthClient(conn)
}

func TestStopGRPC_GracefulWhenIdle(t *testing.T) {
	srv, _ := newHealthServer(t)

	begin := time.Now()
	stopGRPC(srv, 5*time.Second, discardLogger())

	// Активных запросов нет, поэтому ждать таймаут не нужно
	if elapsed := time.Since(begin); elapsed > time.Second {
		t.Errorf("остановка простаивающего сервера заняла %v", elapsed)
	}
}

func TestStopGRPC_ForcesStopWhenStreamStaysOpen(t *testing.T) {
	srv, client := newHealthServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	// Дожидаемся первого сообщения: к этому моменту стрим точно открыт
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("первое сообщение стрима: %v", err)
	}

	const timeout = 200 * time.Millisecond
	begin := time.Now()
	stopGRPC(srv, timeout, discardLogger())
	elapsed := time.Since(begin)

	// GracefulStop должен был ждать открытый стрим до таймаута, затем сработал Stop
	if elapsed < timeout {
		t.Errorf("остановка заняла %v: сервер не дождался таймаута", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Errorf("остановка заняла %v: принудительная остановка не сработала", elapsed)
	}
	if _, err := stream.Recv(); err == nil {
		t.Error("стрим должен быть закрыт после остановки сервера")
	}
}
