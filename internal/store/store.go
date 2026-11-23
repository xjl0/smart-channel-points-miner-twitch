// Package store владеет базой SQLite: открывает её, применяет миграции
// и предоставляет типизированные запросы, сгенерированные sqlc. Никакой другой пакет не пишет SQL.
package store

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	_ "modernc.org/sqlite" // драйвер SQLite на чистом Go, регистрирует "sqlite"

	"github.com/xjl0/smart-channel-points-miner-twitch/internal/store/db"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Store объединяет сырое соединение со сгенерированными запросами.
type Store struct {
	*db.Queries
	DB *sql.DB
}

// Open открывает (при необходимости создавая) файл базы и мигрирует его до
// последней схемы. Для тестов используйте ":memory:".
func Open(ctx context.Context, path string, log *slog.Logger) (*Store, error) {
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, fmt.Errorf("create db dir: %w", err)
		}
	}

	// WAL позволяет админ-API читать, пока движок пишет; busy_timeout
	// избавляет от "database is locked" при кратковременной конкуренции за запись.
	dsn := path + "?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"
	conn, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// Одно соединение упрощает семантику SQLite (нет гонок писателей,
	// а базы ":memory:" общие для всех запросов).
	conn.SetMaxOpenConns(1)

	if err := migrate(ctx, conn, log); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &Store{Queries: db.New(conn), DB: conn}, nil
}

// Maintain сливает WAL обратно в файл базы (чтобы не держать
// большой -wal файл) и даёт SQLite обновить статистику планировщика.
func (s *Store) Maintain(ctx context.Context) error {
	if _, err := s.DB.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return fmt.Errorf("wal checkpoint: %w", err)
	}
	if _, err := s.DB.ExecContext(ctx, "PRAGMA optimize"); err != nil {
		return fmt.Errorf("optimize: %w", err)
	}
	return nil
}

// Close освобождает базу данных.
func (s *Store) Close() error { return s.DB.Close() }

// migrate применяет каждый встроенный файл NNNN_*.sql, чей номер больше
// PRAGMA user_version, каждый в своей транзакции.
func migrate(ctx context.Context, conn *sql.DB, log *slog.Logger) error {
	var current int
	if err := conn.QueryRowContext(ctx, "PRAGMA user_version").Scan(&current); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}

	files, err := fs.Glob(migrationsFS, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(files)

	for _, file := range files {
		version, err := strconv.Atoi(strings.SplitN(filepath.Base(file), "_", 2)[0])
		if err != nil {
			return fmt.Errorf("bad migration name %q: %w", file, err)
		}
		if version <= current {
			continue
		}
		body, err := migrationsFS.ReadFile(file)
		if err != nil {
			return err
		}
		if err := applyMigration(ctx, conn, version, string(body)); err != nil {
			return fmt.Errorf("migration %s: %w", file, err)
		}
		log.Info("database migrated", "version", version, "file", filepath.Base(file))
	}
	return nil
}

// applyMigration выполняет один скрипт миграции и атомарно увеличивает user_version.
func applyMigration(ctx context.Context, conn *sql.DB, version int, script string) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, script); err != nil {
		return err
	}
	// PRAGMA не принимает связанные параметры, version — это разобранный нами int.
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", version)); err != nil {
		return err
	}
	return tx.Commit()
}
