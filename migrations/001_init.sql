-- Таблица истории уведомлений
CREATE TABLE IF NOT EXISTS notifications (
    -- UUID генерирует сама база (встроенная функция, PostgreSQL 13+)
    id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id    BIGINT      NOT NULL,
    type       TEXT        NOT NULL CHECK (type IN ('email', 'push')),
    status     TEXT        NOT NULL CHECK (status IN ('pending', 'sent', 'failed')),
    payload    JSONB       NOT NULL,
    -- Пустая строка означает "ошибки нет": так проще, чем NULL
    error      TEXT        NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- История по пользователю
CREATE INDEX IF NOT EXISTS idx_notifications_user_id ON notifications (user_id);

-- Выборка по статусу (например, поиск зависших pending)
CREATE INDEX IF NOT EXISTS idx_notifications_status ON notifications (status);

-- Выборки по времени (например, статистика)
CREATE INDEX IF NOT EXISTS idx_notifications_created_at ON notifications (created_at);