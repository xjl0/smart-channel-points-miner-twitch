-- Начальная схема. Время хранится как DATETIME, чтобы драйвер отображал его в time.Time.

-- Единственный аккаунт Twitch, на который работает майнер (всегда id = 1).
CREATE TABLE account (
    id           INTEGER PRIMARY KEY CHECK (id = 1),
    login        TEXT     NOT NULL,
    user_id      TEXT     NOT NULL,
    access_token TEXT     NOT NULL,
    device_id    TEXT     NOT NULL,
    updated_at   DATETIME NOT NULL
);

-- Отслеживаемые каналы. id — это id канала (пользователя) Twitch.
CREATE TABLE channels (
    id            TEXT PRIMARY KEY,
    login         TEXT     NOT NULL UNIQUE,
    display_name  TEXT     NOT NULL,
    enabled       BOOLEAN  NOT NULL DEFAULT 1,
    points        INTEGER  NOT NULL DEFAULT 0, -- последний известный баланс баллов канала
    streak_length INTEGER  NOT NULL DEFAULT 0, -- последняя известная длина стрика просмотра
    added_at      DATETIME NOT NULL
);

-- Одна строка на каждый стрим (трансляцию) отслеживаемого канала.
CREATE TABLE broadcasts (
    id                TEXT PRIMARY KEY,
    channel_id        TEXT     NOT NULL REFERENCES channels (id) ON DELETE CASCADE,
    started_at        DATETIME NOT NULL,
    ended_at          DATETIME,
    streak_claimed_at DATETIME,          -- задаётся, когда стрик просмотра за этот стрим получен
    streak_attempts   INTEGER  NOT NULL DEFAULT 0
);
CREATE INDEX broadcasts_channel ON broadcasts (channel_id, started_at);

-- Непрерывный период просмотра одного канала в одном слоте.
CREATE TABLE watch_sessions (
    id           INTEGER PRIMARY KEY,
    channel_id   TEXT     NOT NULL REFERENCES channels (id) ON DELETE CASCADE,
    broadcast_id TEXT     NOT NULL REFERENCES broadcasts (id) ON DELETE CASCADE,
    reason       TEXT     NOT NULL CHECK (reason IN ('streak', 'fill')),
    started_at   DATETIME NOT NULL,
    ended_at     DATETIME,
    seconds      INTEGER  NOT NULL DEFAULT 0 -- подтверждённое время просмотра (успешные minute-watched)
);
CREATE INDEX watch_sessions_broadcast ON watch_sessions (broadcast_id);

-- История событий ленты. payload — сериализованный protobuf LiveMessage.
-- seq — порядковый номер live-потока (назначается live-шиной; не каждое
-- live-сообщение сохраняется, поэтому пропуски ожидаемы).
CREATE TABLE events (
    seq        INTEGER PRIMARY KEY,
    ts         DATETIME NOT NULL,
    type       TEXT     NOT NULL,
    channel_id TEXT,
    payload    BLOB     NOT NULL
);
CREATE INDEX events_channel ON events (channel_id, seq);
