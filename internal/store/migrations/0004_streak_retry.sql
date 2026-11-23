-- Секунды просмотра, уже потраченные на момент запроса повтора стрика: таймер
-- "считать полученным через N минут" отсчитывается отсюда, а не с нуля.
ALTER TABLE broadcasts ADD COLUMN streak_base_seconds INTEGER NOT NULL DEFAULT 0;
