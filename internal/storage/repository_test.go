package storage

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

// Эти тесты работают с настоящим PostgreSQL
// Если TEST_DATABASE_URL не задан, они пропускаются, чтобы обычный go test ./... не требовал базу

// newTestRepo подключается к базе или пропускает тест
func newTestRepo(t *testing.T) *Repository {
	t.Helper()

	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL не задан: тесты PostgreSQL пропущены")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := NewPool(ctx, url)
	if err != nil {
		t.Fatalf("подключение к тестовой базе: %v", err)
	}
	t.Cleanup(pool.Close)

	return NewRepository(pool)
}

// createTestNotification создает запись и удаляет ее после теста
func createTestNotification(t *testing.T, repo *Repository) Notification {
	t.Helper()
	ctx := context.Background()

	// JSONB нормализует текст (добавляет пробел после двоеточия),
	// поэтому сразу пишем JSON в том виде, в котором его вернет база
	n, err := repo.Create(ctx, 42, TypeEmail, `{"to": "user@example.com"}`)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() {
		_, _ = repo.pool.Exec(context.Background(), `DELETE FROM notifications WHERE id = $1::uuid`, n.ID)
	})
	return n
}

func TestCreateAndGetByID(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	created := createTestNotification(t, repo)

	if created.ID == "" {
		t.Error("id не должен быть пустым")
	}
	if created.UserID != 42 || created.Type != TypeEmail || created.Status != StatusPending {
		t.Errorf("неверные поля созданной записи: %+v", created)
	}

	got, err := repo.GetByID(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.ID != created.ID || got.Payload != `{"to": "user@example.com"}` {
		t.Errorf("прочитана другая запись: %+v", got)
	}
	if got.Error != "" {
		t.Errorf("error должен быть пустым, получено %q", got.Error)
	}
}

func TestGetByID_NotFound(t *testing.T) {
	repo := newTestRepo(t)

	tests := []struct {
		name string
		id   string
	}{
		{"валидный UUID, которого нет", "00000000-0000-0000-0000-000000000000"},
		{"строка не является UUID", "not-a-uuid"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := repo.GetByID(context.Background(), tt.id)
			if !errors.Is(err, ErrNotFound) {
				t.Errorf("ожидалась ErrNotFound, получено %v", err)
			}
		})
	}
}

func TestUpdateStatus(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()
	created := createTestNotification(t, repo)

	// pending -> sent
	if err := repo.UpdateStatus(ctx, created.ID, StatusSent, ""); err != nil {
		t.Fatalf("UpdateStatus(sent): %v", err)
	}
	got, err := repo.GetByID(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Status != StatusSent {
		t.Errorf("статус: получено %q, ожидалось %q", got.Status, StatusSent)
	}
	if got.UpdatedAt.Before(got.CreatedAt) {
		t.Errorf("updated_at (%v) раньше created_at (%v)", got.UpdatedAt, got.CreatedAt)
	}

	// sent -> failed с текстом ошибки
	if err := repo.UpdateStatus(ctx, created.ID, StatusFailed, "smtp timeout"); err != nil {
		t.Fatalf("UpdateStatus(failed): %v", err)
	}
	got, _ = repo.GetByID(ctx, created.ID)
	if got.Status != StatusFailed || got.Error != "smtp timeout" {
		t.Errorf("ожидались failed и текст ошибки, получено %+v", got)
	}
}

func TestUpdateStatus_NotFound(t *testing.T) {
	repo := newTestRepo(t)

	err := repo.UpdateStatus(context.Background(), "00000000-0000-0000-0000-000000000000", StatusSent, "")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("ожидалась ErrNotFound, получено %v", err)
	}
}

func TestCreate_InvalidType(t *testing.T) {
	repo := newTestRepo(t)

	// CHECK в таблице не пропустит неизвестный тип
	_, err := repo.Create(context.Background(), 1, Type("sms"), `{}`)
	if err == nil {
		t.Fatal("ожидалась ошибка из-за недопустимого типа")
	}
}
