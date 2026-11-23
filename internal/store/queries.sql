-- name: GetAccount :one
SELECT * FROM account WHERE id = 1;

-- name: SaveAccount :exec
INSERT INTO account (id, login, user_id, access_token, device_id, updated_at)
VALUES (1, ?, ?, ?, ?, ?)
ON CONFLICT (id) DO UPDATE SET
    login = excluded.login,
    user_id = excluded.user_id,
    access_token = excluded.access_token,
    device_id = excluded.device_id,
    updated_at = excluded.updated_at;

-- name: DeleteAccount :exec
DELETE FROM account;

-- name: ListChannels :many
SELECT * FROM channels ORDER BY login;

-- name: InsertChannel :one
INSERT INTO channels (id, login, display_name, added_at)
VALUES (?, ?, ?, ?)
RETURNING *;

-- name: DeleteChannel :exec
DELETE FROM channels WHERE id = ?;

-- name: SetChannelDisplayName :exec
UPDATE channels SET display_name = ? WHERE id = ?;

-- name: SetChannelEnabled :exec
UPDATE channels SET enabled = ? WHERE id = ?;

-- name: SetChannelPoints :exec
UPDATE channels SET points = ? WHERE id = ?;

-- name: SetChannelStreak :exec
UPDATE channels SET streak_length = ?, streak_expires_at = ? WHERE id = ?;

-- name: UpsertBroadcast :one
INSERT INTO broadcasts (id, channel_id, started_at)
VALUES (?, ?, ?)
ON CONFLICT (id) DO UPDATE SET ended_at = NULL, started_at = MIN(started_at, excluded.started_at)
RETURNING *;

-- name: OpenBroadcasts :many
SELECT * FROM broadcasts WHERE ended_at IS NULL ORDER BY started_at;

-- name: DeleteBroadcastsEndedBefore :execrows
DELETE FROM broadcasts WHERE ended_at IS NOT NULL AND ended_at < ?;

-- name: EndBroadcast :exec
UPDATE broadcasts SET ended_at = ? WHERE id = ? AND ended_at IS NULL;

-- name: EndOpenBroadcasts :exec
UPDATE broadcasts SET ended_at = ? WHERE channel_id = ? AND ended_at IS NULL;

-- name: RetryBroadcastStreak :exec
UPDATE broadcasts SET streak_claimed_at = NULL, streak_base_seconds = ? WHERE id = ?;

-- name: AddStreakBase :exec
UPDATE broadcasts SET streak_base_seconds = streak_base_seconds + ? WHERE id = ?;

-- name: MarkStreakClaimed :exec
UPDATE broadcasts SET streak_claimed_at = ? WHERE id = ? AND streak_claimed_at IS NULL;

-- name: StartWatchSession :one
INSERT INTO watch_sessions (channel_id, broadcast_id, reason, started_at)
VALUES (?, ?, ?, ?)
RETURNING id;

-- name: AddWatchSeconds :exec
UPDATE watch_sessions SET seconds = seconds + ? WHERE id = ?;

-- name: EndWatchSession :exec
UPDATE watch_sessions SET ended_at = ? WHERE id = ? AND ended_at IS NULL;

-- name: EndAllWatchSessions :exec
UPDATE watch_sessions SET ended_at = ? WHERE ended_at IS NULL;

-- name: BroadcastWatchSeconds :one
SELECT CAST(COALESCE(SUM(seconds), 0) AS INTEGER) AS seconds
FROM watch_sessions WHERE broadcast_id = ?;

-- name: InsertEvent :exec
INSERT INTO events (seq, ts, type, channel_id, payload)
VALUES (?, ?, ?, ?, ?);

-- name: ListEvents :many
SELECT * FROM events
WHERE seq < sqlc.arg(before_seq)
  AND (CAST(sqlc.arg(channel_id) AS TEXT) = '' OR channel_id = sqlc.arg(channel_id))
ORDER BY seq DESC
LIMIT sqlc.arg(limit);

-- name: DeleteEventsBefore :execrows
DELETE FROM events WHERE ts < ?;

-- name: LastEventSeq :one
SELECT CAST(COALESCE(MAX(seq), 0) AS INTEGER) AS seq FROM events;

-- name: GetSettings :one
SELECT data FROM settings WHERE id = 1;

-- name: SaveSettings :exec
INSERT INTO settings (id, data, updated_at) VALUES (1, ?, ?)
ON CONFLICT (id) DO UPDATE SET data = excluded.data, updated_at = excluded.updated_at;

-- ================================ Справочник запросов ================================
-- Пояснения собраны здесь, а не над запросами: sqlc (движок SQLite) неверно считает
-- позиции, если перед запросом в файле встречаются не-ASCII символы, и ломает разбор.
-- После последнего запроса русский текст ему не мешает.
--
-- Запросы, которые использует майнер. sqlc превращает каждый в типизированный Go-метод.
--
-- Аккаунт
--   GetAccount — аккаунт Twitch (всегда одна строка, id = 1).
--   SaveAccount — сохраняет аккаунт после входа (вставка или обновление).
--   DeleteAccount — выход: забывает токен.
--
-- Каналы
--   ListChannels — все отслеживаемые каналы по алфавиту.
--   InsertChannel — добавляет канал.
--   DeleteChannel — удаляет канал вместе с его трансляциями и сессиями (каскадом).
--   SetChannelDisplayName — отображаемое имя из Twitch.
--   SetChannelEnabled — пауза / возобновление канала.
--   SetChannelPoints — последний известный баланс баллов.
--   SetChannelStreak — длина стрика и срок, когда он сгорит.
--
-- Трансляции
--   UpsertBroadcast — Создаёт трансляцию при первом обнаружении; в любом случае возвращает сохранённую строку.
--     started_at только сдвигается раньше (время первого обнаружения заменяется реальным началом).
--   OpenBroadcasts — Ещё не закрытые трансляции, сначала старые (побеждает самая новая для канала).
--   DeleteBroadcastsEndedBefore — Старые завершённые трансляции; их сессии просмотра удаляются вместе с ними (ON DELETE
--     CASCADE).
--   EndBroadcast — закрывает одну трансляцию.
--   EndOpenBroadcasts — Закрывает все открытые трансляции канала (используется, когда он уходит в офлайн).
--   RetryBroadcastStreak — кнопка «Повторить стрик»: снимает стрик и запоминает уже просмотренное время.
--   AddStreakBase — минута просмотра без PubSub: Twitch её не засчитал, в отсчёт стрика она не идёт.
--   MarkStreakClaimed — отмечает стрик трансляции полученным (только первый раз).
--
-- Сессии просмотра
--   StartWatchSession — начало просмотра канала в слоте.
--   AddWatchSeconds — +секунды за успешный minute-watched.
--   EndWatchSession — конец просмотра.
--   EndAllWatchSessions — Закрывает сессии, оставшиеся открытыми после падения или перезапуска.
--   BroadcastWatchSeconds — сколько мы смотрели трансляцию в сумме (по всем сессиям).
--
-- События
--   InsertEvent — запись события в историю (seq задаёт шина live).
--   ListEvents — Страница истории, сначала новые, строго старше before_seq; пустой channel_id означает все
--     каналы.
--   DeleteEventsBefore — ежесуточная очистка истории.
--   LastEventSeq — последний seq, чтобы шина продолжила нумерацию после рестарта.
--
-- Настройки
--   GetSettings — сохранённые из админки таймеры (protobuf miner.v1.Settings); нет строки — по умолчанию.
--   SaveSettings — сохраняет таймеры после изменения в админке.
