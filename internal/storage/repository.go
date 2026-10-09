package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Код ошибки PostgreSQL "invalid_text_representation":
// значение нельзя разобрать в нужный тип (например, строка не является UUID)
const pgInvalidTextRepresentation = "22P02"

// columns список колонок для SELECT и RETURNING
// Приведение к text позволяет читать uuid и jsonb в обычные строки Go
const columns = `id::text, user_id, type, status, payload::text, error, created_at, updated_at`

// NewPool создает пул соединений и проверяет, что база доступна (fail-fast)
// Закрывать пул (Close) должен тот, кто его создал
func NewPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("создание пула соединений: %w", err)
	}
	// pgxpool.New не открывает соединение сразу, поэтому проверяем доступность базы явно
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("проверка соединения с PostgreSQL: %w", err)
	}
	return pool, nil
}

// Repository работает с таблицей notifications
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository создает репозиторий поверх готового пула
func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// Create добавляет уведомление со статусом pending и возвращает сохраненную запись
// (id и время генерирует база)
// payload должен быть корректным JSON
func (r *Repository) Create(ctx context.Context, userID int64, typ Type, payload string) (Notification, error) {
	const q = `
		INSERT INTO notifications (user_id, type, status, payload)
		VALUES ($1, $2, $3, $4::jsonb)
		RETURNING ` + columns

	// Значения передаются отдельно от текста запроса ($1..$4): защита от SQL-инъекций
	n, err := scanNotification(r.pool.QueryRow(ctx, q, userID, string(typ), string(StatusPending), payload))
	if err != nil {
		return Notification{}, fmt.Errorf("создание уведомления: %w", err)
	}
	return n, nil
}

// GetByID возвращает уведомление по id или ErrNotFound.
func (r *Repository) GetByID(ctx context.Context, id string) (Notification, error) {
	const q = `SELECT ` + columns + ` FROM notifications WHERE id = $1::uuid`

	n, err := scanNotification(r.pool.QueryRow(ctx, q, id))
	if err != nil {
		if isNotFound(err) {
			return Notification{}, ErrNotFound
		}
		return Notification{}, fmt.Errorf("получение уведомления %s: %w", id, err)
	}
	return n, nil
}

// UpdateStatus меняет статус и текст ошибки, обновляя updated_at
// Если уведомления нет, возвращает ErrNotFound
func (r *Repository) UpdateStatus(ctx context.Context, id string, status Status, errMsg string) error {
	const q = `
		UPDATE notifications
		SET status = $2, error = $3, updated_at = now()
		WHERE id = $1::uuid`

	tag, err := r.pool.Exec(ctx, q, id, string(status), errMsg)
	if err != nil {
		if isNotFound(err) {
			return ErrNotFound
		}
		return fmt.Errorf("обновление статуса уведомления %s: %w", id, err)
	}
	// Запрос выполнился, но ни одна строка не подошла под WHERE
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// scanNotification читает одну строку результата в структуру
func scanNotification(row pgx.Row) (Notification, error) {
	var (
		n          Notification
		typ, state string
	)
	err := row.Scan(&n.ID, &n.UserID, &typ, &state, &n.Payload, &n.Error, &n.CreatedAt, &n.UpdatedAt)
	if err != nil {
		return Notification{}, err
	}
	n.Type = Type(typ)
	n.Status = Status(state)
	return n, nil
}

// isNotFound сообщает, что такой записи нет: либо строка не найдена,
// либо id не является корректным UUID
func isNotFound(err error) bool {
	if errors.Is(err, pgx.ErrNoRows) {
		return true
	}
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgInvalidTextRepresentation
}
