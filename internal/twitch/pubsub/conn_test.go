package pubsub

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// fakePubSub — WebSocket-сервер в роли Twitch PubSub. Первое соединение:
// принимает подписки, шлёт события (одно больше 32 КиБ) и просит
// переподключиться. Второе — просто держится открытым.
type fakePubSub struct {
	mu       sync.Mutex
	listens  [][]string // топики каждого кадра LISTEN
	withAuth []string   // топики, подписанные с токеном
	sessions atomic.Int32
}

func (f *fakePubSub) handle(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer conn.CloseNow()
	n := f.sessions.Add(1)
	ctx := r.Context()
	for got := 0; got < 5; got++ { // 4 пользовательских топика + 1 топик канала
		var frame struct {
			Type string `json:"type"`
			Data struct {
				Topics    []string `json:"topics"`
				AuthToken string   `json:"auth_token"`
			} `json:"data"`
		}
		if err := wsjson.Read(ctx, conn, &frame); err != nil {
			return
		}
		f.mu.Lock()
		f.listens = append(f.listens, frame.Data.Topics)
		if frame.Data.AuthToken != "" {
			f.withAuth = append(f.withAuth, frame.Data.Topics...)
		}
		f.mu.Unlock()
	}
	if n > 1 {
		<-ctx.Done() // второе соединение: просто держим
		return
	}
	send := func(topic, message string) {
		_ = wsjson.Write(ctx, conn, map[string]any{"type": "MESSAGE", "data": map[string]string{"topic": topic, "message": message}})
	}
	send("video-playback-by-id.42", `{"type":"stream-up"}`)
	// Сообщение больше лимита coder/websocket по умолчанию (32 КиБ).
	send("video-playback-by-id.42", `{"type":"viewcount","viewers":7,"pad":"`+strings.Repeat("x", 40<<10)+`"}`)
	_ = wsjson.Write(ctx, conn, map[string]string{"type": "RECONNECT"})
	time.Sleep(200 * time.Millisecond)
}

func TestClientOverRealWebSocket(t *testing.T) {
	f := &fakePubSub{}
	srv := httptest.NewServer(http.HandlerFunc(f.handle))
	defer srv.Close()
	endpoint = "ws" + strings.TrimPrefix(srv.URL, "http")

	c := New(slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	c.SetAuth("tok", "1000")
	c.Listen(PlaybackTopic("42"))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	go c.Run(ctx)

	want := []Event{StreamUp{ChannelID: "42"}, ViewCount{ChannelID: "42", Viewers: 7}, Reconnected{}}
	for i, w := range want {
		select {
		case ev := <-c.Events():
			if ev != w {
				t.Fatalf("event %d = %#v, want %#v", i, ev, w)
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %#v", w)
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	for _, topics := range f.listens {
		if len(topics) != 1 {
			t.Fatalf("LISTEN frame with %d topics: %v (one per frame, else Twitch drops us)", len(topics), topics)
		}
	}
	raw, _ := json.Marshal(f.withAuth)
	if strings.Contains(string(raw), "video-playback") || !strings.Contains(string(raw), "community-points-user-v1.1000") {
		t.Fatalf("token sent with wrong topics: %s", raw)
	}
}
