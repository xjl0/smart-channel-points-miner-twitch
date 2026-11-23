package store

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/xjl0/smart-channel-points-miner-twitch/internal/store/db"
)

// openTest возвращает свежую базу в памяти.
func openTest(t *testing.T) *Store {
	t.Helper()
	s, err := Open(context.Background(), ":memory:", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestMigrateIsIdempotent(t *testing.T) {
	s := openTest(t)
	if err := migrate(context.Background(), s.DB, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
}

func TestWatchSecondsPerBroadcast(t *testing.T) {
	ctx := context.Background()
	s := openTest(t)
	now := time.Now().UTC().Truncate(time.Second)

	if _, err := s.InsertChannel(ctx, db.InsertChannelParams{ID: "1", Login: "a", DisplayName: "A", AddedAt: now}); err != nil {
		t.Fatal(err)
	}
	b, err := s.UpsertBroadcast(ctx, db.UpsertBroadcastParams{ID: "b1", ChannelID: "1", StartedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	if !b.StartedAt.Equal(now) {
		t.Fatalf("started_at round-trip: got %v want %v", b.StartedAt, now)
	}

	// Две сессии одной трансляции суммируются.
	for _, secs := range []int64{60, 120} {
		id, err := s.StartWatchSession(ctx, db.StartWatchSessionParams{ChannelID: "1", BroadcastID: "b1", Reason: "fill", StartedAt: now})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.AddWatchSeconds(ctx, db.AddWatchSecondsParams{Seconds: secs, ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.BroadcastWatchSeconds(ctx, "b1")
	if err != nil {
		t.Fatal(err)
	}
	if got != 180 {
		t.Fatalf("watch seconds = %d, want 180", got)
	}

	// Получение стрика переживает повторный upsert той же трансляции (перезапуск).
	if err := s.MarkStreakClaimed(ctx, db.MarkStreakClaimedParams{StreakClaimedAt: &now, ID: "b1"}); err != nil {
		t.Fatal(err)
	}
	b, err = s.UpsertBroadcast(ctx, db.UpsertBroadcastParams{ID: "b1", ChannelID: "1", StartedAt: now})
	if err != nil || b.StreakClaimedAt == nil {
		t.Fatalf("streak lost on upsert: %+v, err %v", b, err)
	}
}

func TestEventsPaging(t *testing.T) {
	ctx := context.Background()
	s := openTest(t)
	ch := "1"
	for i := 0; i < 5; i++ {
		var cid *string
		if i%2 == 0 {
			cid = &ch
		}
		if err := s.InsertEvent(ctx, db.InsertEventParams{Seq: int64(i + 1), Ts: time.Now(), Type: "t", ChannelID: cid, Payload: []byte{1}}); err != nil {
			t.Fatal(err)
		}
	}
	all, _ := s.ListEvents(ctx, db.ListEventsParams{BeforeSeq: 100, Limit: 10})
	only, _ := s.ListEvents(ctx, db.ListEventsParams{BeforeSeq: 100, ChannelID: ch, Limit: 10})
	older, _ := s.ListEvents(ctx, db.ListEventsParams{BeforeSeq: 3, Limit: 10})
	if len(all) != 5 || len(only) != 3 || len(older) != 2 || older[0].Seq != 2 {
		t.Fatalf("all=%d only=%d older=%d", len(all), len(only), len(older))
	}
}

func TestDeleteEventsBefore(t *testing.T) {
	ctx := context.Background()
	s := openTest(t)
	now := time.Now().UTC()
	for i, ts := range []time.Time{now.Add(-100 * 24 * time.Hour), now} {
		if err := s.InsertEvent(ctx, db.InsertEventParams{Seq: int64(i + 1), Ts: ts, Type: "t", Payload: []byte{1}}); err != nil {
			t.Fatal(err)
		}
	}
	n, err := s.DeleteEventsBefore(ctx, now.Add(-90*24*time.Hour))
	if err != nil || n != 1 {
		t.Fatalf("deleted %d, err %v", n, err)
	}
}

func TestOldBroadcastsGoWithTheirSessions(t *testing.T) {
	ctx := context.Background()
	s := openTest(t)
	now := time.Now().UTC()
	if _, err := s.InsertChannel(ctx, db.InsertChannelParams{ID: "1", Login: "a", DisplayName: "A", AddedAt: now}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"old", "recent", "live"} {
		if _, err := s.UpsertBroadcast(ctx, db.UpsertBroadcastParams{ID: id, ChannelID: "1", StartedAt: now}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.StartWatchSession(ctx, db.StartWatchSessionParams{ChannelID: "1", BroadcastID: id, Reason: "fill", StartedAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	old, recent := now.Add(-200*24*time.Hour), now.Add(-time.Hour)
	_ = s.EndBroadcast(ctx, db.EndBroadcastParams{EndedAt: &old, ID: "old"})
	_ = s.EndBroadcast(ctx, db.EndBroadcastParams{EndedAt: &recent, ID: "recent"})

	cutoff := now.Add(-180 * 24 * time.Hour)
	n, err := s.DeleteBroadcastsEndedBefore(ctx, &cutoff)
	if err != nil || n != 1 {
		t.Fatalf("deleted %d broadcasts, err %v; want 1", n, err)
	}
	var sessions int
	if err := s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM watch_sessions").Scan(&sessions); err != nil || sessions != 2 {
		t.Fatalf("watch sessions left %d (err %v), want 2: the old broadcast's session must cascade", sessions, err)
	}
	if err := s.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
}
