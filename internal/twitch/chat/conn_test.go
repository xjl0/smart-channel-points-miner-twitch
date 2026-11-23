package chat

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// TestClientOverRealWebSocket: вход в IRC, повторный JOIN, ответ на PING и
// доставка сообщения чата — на настоящем WebSocket-соединении.
func TestClientOverRealWebSocket(t *testing.T) {
	got := make(chan []string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		ctx := r.Context()
		read := func() string {
			_, b, err := conn.Read(ctx)
			if err != nil {
				return ""
			}
			return strings.TrimSpace(string(b))
		}
		var lines []string
		for range 4 { // CAP, PASS, NICK, JOIN
			lines = append(lines, read())
		}
		_ = conn.Write(ctx, websocket.MessageText, []byte("PING :tmi.twitch.tv\r\n"))
		lines = append(lines, read()) // ожидаем PONG
		got <- lines
		_ = conn.Write(ctx, websocket.MessageText,
			[]byte("@display-name=Viewer;color=#FF0000 :viewer!viewer@viewer.tmi.twitch.tv PRIVMSG #chan :привет\r\n"))
		<-ctx.Done()
	}))
	defer srv.Close()
	endpoint = "ws" + strings.TrimPrefix(srv.URL, "http")

	c := New(slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	c.SetAuth("Me", "tok")
	c.Join("Chan")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go c.Run(ctx)

	select {
	case lines := <-got:
		want := []string{"CAP REQ :twitch.tv/tags twitch.tv/commands", "PASS oauth:tok", "NICK me", "JOIN #chan", "PONG :tmi.twitch.tv"}
		if !slices.Equal(lines, want) {
			t.Fatalf("client sent %q, want %q", lines, want)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for the login sequence")
	}
	select {
	case m := <-c.Messages():
		if m.Channel != "chan" || m.User != "Viewer" || m.Text != "привет" {
			t.Fatalf("message = %+v", m)
		}
	case <-ctx.Done():
		t.Fatal("chat message not delivered")
	}
}
