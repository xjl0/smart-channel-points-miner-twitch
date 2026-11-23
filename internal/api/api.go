// Package api открывает движок админ-панели: команды через Connect
// RPC, live-обновления через Server-Sent Events (sse.go) и саму панель.
package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"connectrpc.com/connect"

	"github.com/xjl0/smart-channel-points-miner-twitch/internal/engine"
	minerv1 "github.com/xjl0/smart-channel-points-miner-twitch/internal/gen/miner/v1"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/gen/miner/v1/minerv1connect"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/live"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/twitch"
)

// Server реализует minerv1connect.MinerServiceHandler.
type Server struct {
	eng *engine.Engine
	bus *live.Bus
	log *slog.Logger
}

var _ minerv1connect.MinerServiceHandler = (*Server)(nil)

// Handler возвращает HTTP-обработчик API: Connect RPC и SSE-поток (/live) за
// Basic-авторизацией (если задан пароль; второй рубеж после Caddy, который
// передаёт заголовок дальше) и открытый /healthz для healthcheck Docker.
// Админку раздаёт Caddy (web/Caddyfile), бэкенд статику не отдаёт.
func Handler(eng *engine.Engine, bus *live.Bus, creds Credentials, log *slog.Logger) http.Handler {
	s := &Server{eng: eng, bus: bus, log: log.With("component", "api")}
	api := http.NewServeMux()
	path, h := minerv1connect.NewMinerServiceHandler(s)
	api.Handle(path, h)
	api.HandleFunc("GET /live", s.live)

	root := http.NewServeMux()
	root.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	root.Handle("/", basicAuth(api, creds, s.log))
	return root
}

func (s *Server) AddChannel(ctx context.Context, req *connect.Request[minerv1.AddChannelRequest]) (*connect.Response[minerv1.AddChannelResponse], error) {
	ch, err := s.eng.AddChannel(ctx, req.Msg.Login)
	if err != nil {
		return nil, toConnect(err)
	}
	return connect.NewResponse(&minerv1.AddChannelResponse{Channel: ch}), nil
}

func (s *Server) RemoveChannel(_ context.Context, req *connect.Request[minerv1.RemoveChannelRequest]) (*connect.Response[minerv1.RemoveChannelResponse], error) {
	if err := s.eng.RemoveChannel(req.Msg.ChannelId); err != nil {
		return nil, toConnect(err)
	}
	return connect.NewResponse(&minerv1.RemoveChannelResponse{}), nil
}

func (s *Server) SetChannelEnabled(_ context.Context, req *connect.Request[minerv1.SetChannelEnabledRequest]) (*connect.Response[minerv1.SetChannelEnabledResponse], error) {
	if err := s.eng.SetChannelEnabled(req.Msg.ChannelId, req.Msg.Enabled); err != nil {
		return nil, toConnect(err)
	}
	return connect.NewResponse(&minerv1.SetChannelEnabledResponse{}), nil
}

func (s *Server) RetryStreak(_ context.Context, req *connect.Request[minerv1.RetryStreakRequest]) (*connect.Response[minerv1.RetryStreakResponse], error) {
	if err := s.eng.RetryStreak(req.Msg.ChannelId); err != nil {
		return nil, toConnect(err)
	}
	return connect.NewResponse(&minerv1.RetryStreakResponse{}), nil
}

func (s *Server) UpdateSettings(ctx context.Context, req *connect.Request[minerv1.UpdateSettingsRequest]) (*connect.Response[minerv1.UpdateSettingsResponse], error) {
	set, err := s.eng.UpdateSettings(ctx, req.Msg.Settings)
	if err != nil {
		return nil, toConnect(err)
	}
	return connect.NewResponse(&minerv1.UpdateSettingsResponse{Settings: set}), nil
}

func (s *Server) ListEvents(ctx context.Context, req *connect.Request[minerv1.ListEventsRequest]) (*connect.Response[minerv1.ListEventsResponse], error) {
	limit := int(req.Msg.Limit)
	if limit <= 0 {
		limit = 50
	}
	events, err := s.bus.History(ctx, req.Msg.BeforeSeq, req.Msg.ChannelId, min(limit, 200))
	if err != nil {
		return nil, toConnect(err)
	}
	return connect.NewResponse(&minerv1.ListEventsResponse{Events: events}), nil
}

func (s *Server) StartLogin(ctx context.Context, _ *connect.Request[minerv1.StartLoginRequest]) (*connect.Response[minerv1.StartLoginResponse], error) {
	dl, err := s.eng.StartLogin(ctx)
	if err != nil {
		return nil, toConnect(err)
	}
	return connect.NewResponse(&minerv1.StartLoginResponse{Login: dl}), nil
}

func (s *Server) Logout(context.Context, *connect.Request[minerv1.LogoutRequest]) (*connect.Response[minerv1.LogoutResponse], error) {
	if err := s.eng.Logout(); err != nil {
		return nil, toConnect(err)
	}
	return connect.NewResponse(&minerv1.LogoutResponse{}), nil
}

// toConnect отображает доменные ошибки в коды статуса Connect.
func toConnect(err error) error {
	code := connect.CodeInternal
	switch {
	case errors.Is(err, engine.ErrNotFound), errors.Is(err, twitch.ErrChannelNotFound):
		code = connect.CodeNotFound
	case errors.Is(err, engine.ErrInvalidSettings):
		code = connect.CodeInvalidArgument
	case errors.Is(err, engine.ErrExists):
		code = connect.CodeAlreadyExists
	case errors.Is(err, engine.ErrNotLoggedIn), errors.Is(err, twitch.ErrUnauthorized), errors.Is(err, engine.ErrOffline):
		code = connect.CodeFailedPrecondition
	case errors.Is(err, engine.ErrStopped):
		code = connect.CodeUnavailable
	}
	return connect.NewError(code, err)
}
