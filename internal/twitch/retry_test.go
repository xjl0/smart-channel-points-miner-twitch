package twitch

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// Первое соединение сервер обрывает без ответа (как протухшее соединение
// после сна или по таймауту простоя у Twitch) — minute-watched должен
// пройти со второй попытки.
func TestBeaconSurvivesBrokenConnection(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close() // клиент получит EOF
			}
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	c := New(slog.New(slog.NewTextHandler(io.Discard, nil)), nil, WithEndpoints(srv.URL+"/gql", srv.URL+"/spade"))
	c.SetAuth(Auth{Token: "t", UserID: "1"})
	err := c.SendMinuteWatched(t.Context(), MinuteWatched{ChannelID: "1", ChannelLogin: "a", BroadcastID: "b"})
	if err != nil {
		t.Fatalf("beacon failed despite retry: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("server saw %d requests, want 2 (broken + retry)", got)
	}
}
