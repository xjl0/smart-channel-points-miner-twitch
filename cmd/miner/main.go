// Command miner — единственная точка входа сервиса: связывает
// базу данных, клиенты Twitch, движок просмотра и админский API
// и запускает их до получения SIGINT/SIGTERM.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xjl0/smart-channel-points-miner-twitch/internal/api"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/config"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/engine"
	minerv1 "github.com/xjl0/smart-channel-points-miner-twitch/internal/gen/miner/v1"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/live"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/store"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/trace"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/twitch"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/twitch/chat"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/twitch/pubsub"
)

func main() {
	cfg := config.Load()
	stdout := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})
	base := slog.New(stdout)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, stdout, base); err != nil {
		base.Error("miner stopped with error", "err", err)
		os.Exit(1)
	}
	base.Info("miner stopped")
}

// run собирает все компоненты и блокируется, пока ctx не отменён.
//
// Два логгера существуют намеренно: base пишет только в stdout и используется
// хранилищем и live-шиной; log дополнительно транслирует каждую запись
// в админ-панель через шину. Шина не должна логировать через саму себя.
func run(ctx context.Context, cfg config.Config, stdout slog.Handler, base *slog.Logger) error {
	st, err := store.Open(ctx, cfg.DBPath, base)
	if err != nil {
		return err
	}
	defer st.Close()

	bus, err := live.New(ctx, st, base)
	if err != nil {
		return err
	}
	log := slog.New(live.NewLogHandler(stdout, bus))
	slog.SetDefault(log)

	// Если движок упал, останавливаем и HTTP-сервер.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	tr := trace.New(cfg.TraceDir, trace.Limits{MaxAge: cfg.TraceMaxAge, MaxBytes: cfg.TraceMaxBytes}, log)
	chatClient := chat.New(log, tr)
	go proxyChat(ctx, chatClient, bus)
	policy := engine.DefaultPolicy() // таймеры потом берутся из настроек в БД (админка «Настройки»)
	eng := engine.New(log, policy, st, twitch.New(log, tr), pubsub.New(log, tr), chatClient, bus)
	engineErr := make(chan error, 1)
	go func() {
		engineErr <- eng.Run(ctx)
		cancel()
	}()

	logProxy(log)
	if cfg.AdminPassword == "" {
		log.Warn("admin panel has no password: set MINER_ADMIN_PASSWORD")
	}
	creds := api.Credentials{User: cfg.AdminUser, Password: cfg.AdminPassword}
	httpErr := serveHTTP(ctx, cfg.Addr, api.Handler(eng, bus, creds, log), log)
	if err := <-engineErr; err != nil {
		return err
	}
	return httpErr
}

// logProxy пишет, через какой прокси идёт трафик к Twitch (пароль скрыт).
// Прокси берётся из стандартных HTTPS_PROXY/HTTP_PROXY: их понимают и
// HTTP-клиент, и WebSocket-клиент (PubSub, чат).
func logProxy(log *slog.Logger) {
	raw := os.Getenv("HTTPS_PROXY")
	if raw == "" {
		raw = os.Getenv("HTTP_PROXY")
	}
	if raw == "" {
		log.Info("twitch traffic goes direct (no proxy)")
		return
	}
	u, err := url.Parse(raw)
	if err != nil {
		log.Error("invalid proxy url in HTTPS_PROXY", "err", err)
		return
	}
	log.Info("twitch traffic goes through a proxy", "proxy", u.Redacted())
}

// proxyChat пересылает строки чата каналов, к которым выполнен JOIN, в админ-панель
// как эфемерные live-сообщения (показываются, но не сохраняются).
func proxyChat(ctx context.Context, c *chat.Client, bus *live.Bus) {
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-c.Messages():
			bus.Broadcast(&minerv1.LiveMessage{Body: &minerv1.LiveMessage_ChatMessage{ChatMessage: &minerv1.ChatMessage{
				ChannelLogin: m.Channel, User: m.User, Color: m.Color, Text: m.Text, Badges: m.Badges, FirstMsg: m.FirstMsg,
			}}})
		}
	}
}

// serveHTTP запускает HTTP-сервер и корректно останавливает его, когда ctx завершается.
func serveHTTP(ctx context.Context, addr string, h http.Handler, log *slog.Logger) error {
	srv := &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 10 * time.Second}

	errCh := make(chan error, 1)
	go func() {
		log.Info("http server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	log.Info("http server shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}
