package api

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	minerv1 "github.com/xjl0/smart-channel-points-miner-twitch/internal/gen/miner/v1"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/live"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/store"
)

func tick(id string, secs int64) *minerv1.LiveMessage {
	return &minerv1.LiveMessage{Body: &minerv1.LiveMessage_WatchTick{WatchTick: &minerv1.WatchTick{ChannelId: id, WatchedSeconds: secs}}}
}

// sseEvent — одно разобранное событие: его строка id и JSON-тело.
type sseEvent struct {
	id   string
	data map[string]any
}

// readEvents читает n событий из тела SSE.
func readEvents(t *testing.T, sc *bufio.Scanner, n int) []sseEvent {
	t.Helper()
	var out []sseEvent
	var cur sseEvent
	for len(out) < n && sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "id: "):
			cur.id = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "data: "):
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &cur.data); err != nil {
				t.Fatal(err)
			}
		case line == "" && cur.data != nil:
			out = append(out, cur)
			cur = sseEvent{}
		}
	}
	if len(out) < n {
		t.Fatalf("got %d events, want %d (err %v)", len(out), n, sc.Err())
	}
	return out
}

// TestLiveSSEResume: снапшот при первом подключении, затем при переподключении с
// Last-Event-ID — ровно пропущенные дельты, через настоящий HTTP.
func TestLiveSSEResume(t *testing.T) {
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	st, err := store.Open(ctx, ":memory:", log)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	bus, err := live.New(ctx, st, log)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(Handler(nil, bus, Credentials{User: "admin", Password: "secret"}, log))
	defer srv.Close()

	open := func(lastID string) (*http.Response, *bufio.Scanner) {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/live", nil)
		req.SetBasicAuth("admin", "secret")
		if lastID != "" {
			req.Header.Set("Last-Event-ID", lastID)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
			t.Fatalf("content-type %q", ct)
		}
		return resp, bufio.NewScanner(resp.Body)
	}

	bus.Publish(tick("a", 60)) // seq 1
	resp, sc := open("")
	ev := readEvents(t, sc, 1)[0]
	if ev.id != "1" || ev.data["snapshot"] == nil {
		t.Fatalf("expected snapshot with id 1, got %+v", ev)
	}
	bus.Publish(tick("a", 120)) // seq 2
	if ev := readEvents(t, sc, 1)[0]; ev.id != "2" || ev.data["watchTick"] == nil {
		t.Fatalf("expected live tick id 2, got %+v", ev)
	}
	resp.Body.Close()

	bus.Publish(tick("a", 180)) // seq 3, пропущено
	bus.Publish(tick("a", 240)) // seq 4, пропущено

	resp, sc = open("2") // что делает EventSource при переподключении
	defer resp.Body.Close()
	evs := readEvents(t, sc, 2)
	if evs[0].id != "3" || evs[1].id != "4" || evs[0].data["watchTick"] == nil {
		t.Fatalf("expected replay of 3 and 4, got %+v", evs)
	}
}
