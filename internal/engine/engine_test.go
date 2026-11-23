package engine

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/xjl0/smart-channel-points-miner-twitch/internal/live"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/store"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/store/db"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/twitch"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/twitch/chat"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/twitch/pubsub"
)

// newTestEngine собирает движок из реальных компонентов без сети: база данных
// в памяти и клиенты Twitch/чата/PubSub, которые никогда не подключаются
// (нет учётных данных). Сам тест играет роль горутины Run.
func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	st, err := store.Open(context.Background(), ":memory:", log)
	if err != nil {
		t.Fatal(err)
	}
	bus, err := live.New(context.Background(), st, log)
	if err != nil {
		t.Fatal(err)
	}
	p := DefaultPolicy()
	p.Jitter = func(d time.Duration) time.Duration { return d }
	e := New(log, p, st, twitch.New(log, nil), pubsub.New(log, nil), chat.New(log, nil), bus)
	ctx, cancel := context.WithCancel(context.Background())
	e.ctx, e.authed = ctx, true
	t.Cleanup(func() {
		cancel()         // останавливает циклы событий minute-watched до их первого запроса
		close(e.stopped) // освобождает фоновые результаты, которые никто не применяет
		_ = st.Close()
	})
	return e
}

// goLive добавляет канал (если он новый) и выводит его в онлайн с трансляцией.
func goLive(t *testing.T, e *Engine, login, broadcast string, startedAgo time.Duration) *channel {
	t.Helper()
	ch := e.channels[login]
	if ch == nil {
		row, err := e.store.InsertChannel(dbctx(), db.InsertChannelParams{ID: login, Login: login, DisplayName: login, AddedAt: time.Now()})
		if err != nil {
			t.Fatal(err)
		}
		ch = &channel{Channel: row}
		e.channels[login] = ch
	}
	e.goOnline(ch, &twitch.Stream{BroadcastID: broadcast, StartedAt: time.Now().Add(-startedAgo)})
	ch.streakKnown = true // это сделала бы проверка RewardList
	e.reconcile()
	return ch
}

// assertWatching проверяет жёсткий инвариант: просматриваются ровно эти каналы,
// и мы находимся в чатах ровно этих каналов.
func assertWatching(t *testing.T, e *Engine, want ...string) {
	t.Helper()
	sort.Strings(want)
	var got []string
	for id := range e.viewing {
		got = append(got, id)
	}
	sort.Strings(got)
	if len(got) > e.policy.MaxSlots {
		t.Fatalf("more than %d viewings: %v", e.policy.MaxSlots, got)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("watching %v, want %v", got, want)
	}
	if chats := e.chat.Joined(); !slices.Equal(chats, want) {
		t.Fatalf("in chats %v, want %v", chats, want)
	}
}

func TestEngineNeverWatchesMoreThanTwo(t *testing.T) {
	e := newTestEngine(t)

	goLive(t, e, "a", "a1", time.Hour)
	goLive(t, e, "b", "b1", 50*time.Minute)
	assertWatching(t, e, "a", "b")

	// Третий стример выходит в эфир: сначала доводятся до конца текущие попытки.
	goLive(t, e, "c", "c1", time.Minute)
	assertWatching(t, e, "a", "b")
	if !slices.Equal(e.queue, []string{"c"}) {
		t.Fatalf("queue = %v, want [c]", e.queue)
	}

	// a уходит в офлайн: он полностью освобождается (выход из чата), c занимает его слот.
	e.goOffline(e.channels["a"], "test")
	e.reconcile()
	assertWatching(t, e, "b", "c")

	// b перезапускает стрим: просмотр перезапускается с новым id трансляции.
	old := e.viewing["b"]
	goLive(t, e, "b", "b2", 0)
	assertWatching(t, e, "b", "c")
	if e.viewing["b"] == old {
		t.Fatal("viewing of b was not restarted for the new broadcast")
	}
	if got := e.viewing["b"].beacon.Load().BroadcastID; got != "b2" {
		t.Fatalf("beacon broadcast = %s, want b2", got)
	}

	// Удаление просматриваемого канала тоже его освобождает.
	if err := e.store.DeleteChannel(dbctx(), "c"); err != nil {
		t.Fatal(err)
	}
	e.stopViewing("c", e.viewing["c"])
	delete(e.channels, "c")
	e.reconcile()
	assertWatching(t, e, "b")
}

func TestStaleBeaconIsNotCounted(t *testing.T) {
	e := newTestEngine(t)
	goLive(t, e, "a", "a1", time.Hour)
	first := e.viewing["a"]

	// Просмотр заменяется (офлайн → снова онлайн, та же трансляция).
	e.goOffline(e.channels["a"], "test")
	e.reconcile()
	goLive(t, e, "a", "a1", time.Hour)

	before := e.channels["a"].watched
	e.onWatched(first, nil) // запоздалый результат события minute-watched старого просмотра
	if e.channels["a"].watched != before {
		t.Fatal("a beacon of a finished viewing was counted")
	}
	e.onWatched(e.viewing["a"], nil)
	if e.channels["a"].watched != before+60 {
		t.Fatal("a beacon of the current viewing was not counted")
	}
}
