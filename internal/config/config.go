// Package config читает настройки времени выполнения из переменных окружения.
//
// У каждой настройки есть разумное значение по умолчанию, чтобы сервис запускался
// без какой-либо конфигурации во время разработки.
package config

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config содержит все настройки времени выполнения майнера.
type Config struct {
	// Addr — HTTP-адрес прослушивания для API и UI админки.
	Addr string
	// DBPath — расположение файла базы данных SQLite.
	DBPath string
	// LogLevel управляет подробностью slog: debug, info, warn, error.
	LogLevel slog.Level
	// TraceDir, если задан, получает сырой трафик Twitch в виде почасовых JSONL-файлов,
	// ограниченных TraceMaxAge и TraceMaxBytes.
	TraceDir      string
	TraceMaxAge   time.Duration
	TraceMaxBytes int64
	// AdminUser и AdminPassword — HTTP Basic авторизация админки; без пароля она выключена.
	AdminUser     string
	AdminPassword string
}

// Load собирает Config из переменных окружения:
//
//	MINER_ADDR       адрес прослушивания    (по умолчанию ":8080")
//	MINER_DB         файл SQLite            (по умолчанию "data/miner.db")
//	MINER_LOG_LEVEL  уровень логирования    (по умолчанию "info")
//	MINER_TRACE_DIR  каталог сырого трафика (по умолчанию: трассировка выключена)
//	MINER_TRACE_MAX_AGE  напр. "72h" (по умолчанию 72h)
//	MINER_TRACE_MAX_MB   общий лимит размера в МБ (по умолчанию 200)
//	MINER_ADMIN_USER     логин админки (по умолчанию "admin")
//	MINER_ADMIN_PASSWORD пароль админки (по умолчанию пусто: авторизация выключена)
func Load() Config {
	return Config{
		Addr:          env("MINER_ADDR", ":8080"),
		DBPath:        env("MINER_DB", "data/miner.db"),
		LogLevel:      parseLevel(env("MINER_LOG_LEVEL", "info")),
		TraceDir:      env("MINER_TRACE_DIR", ""),
		TraceMaxAge:   parseDuration(env("MINER_TRACE_MAX_AGE", "72h"), 72*time.Hour),
		TraceMaxBytes: parseMB(env("MINER_TRACE_MAX_MB", "200"), 200),
		AdminUser:     env("MINER_ADMIN_USER", "admin"),
		AdminPassword: os.Getenv("MINER_ADMIN_PASSWORD"), // пробелы в пароле значимы — без TrimSpace
	}
}

// env возвращает значение переменной или fallback, если она не задана или пуста.
func env(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

// parseDuration разбирает s или возвращает fallback, если значение некорректно.
func parseDuration(s string, fallback time.Duration) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

// parseMB переводит размер в мегабайтах в байты или возвращает fallback МБ.
func parseMB(s string, fallback int64) int64 {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 {
		n = fallback
	}
	return n << 20
}

// parseLevel преобразует текстовый уровень в slog.Level, по умолчанию Info.
func parseLevel(s string) slog.Level {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(s)); err != nil {
		return slog.LevelInfo
	}
	return lvl
}
