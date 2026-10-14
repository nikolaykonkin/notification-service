package main

import (
	"log/slog"
	"time"

	"google.golang.org/grpc"
)

// task это функция, запущенная в отдельной горутине
// Когда она завершится, канал done закроется
// Результат (err) можно читать только после этого
type task struct {
	done chan struct{}
	err  error
}

// startTask запускает f в горутине
func startTask(f func() error) *task {
	t := &task{done: make(chan struct{})}
	go func() {
		// Закрытие канала происходит после записи err: читатель, дождавшийся done,
		// гарантированно видит результат, и гонки данных нет
		defer close(t.done)
		t.err = f()
	}()
	return t
}

// stopGRPC останавливает gRPC-сервер аккуратно: перестает принимать новые запросы и
// ждет завершения текущих - если они не завершились за timeout (например, клиент
// держит открытый стрим), останавливает принудительно, чтобы не зависнуть навсегда
func stopGRPC(srv *grpc.Server, timeout time.Duration, log *slog.Logger) {
	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
	case <-timer.C:
		log.Warn("gRPC-запросы не завершились вовремя, остановка принудительная", "timeout", timeout)
		srv.Stop()
		<-done // Stop прерывает ожидание в GracefulStop
	}
}
