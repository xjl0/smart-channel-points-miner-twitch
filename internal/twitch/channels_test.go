package twitch

import (
	"testing"
	"time"
)

// В overlay-запросе нет времени начала; оно должно браться из WithIsStreamLiveQuery,
// иначе определение стрика (достижение после начала трансляции) никогда не сработает.
func TestToStreamTakesStartFromLiveQuery(t *testing.T) {
	var info streamInfoData
	var start streamStartData
	mustJSON(t, `{"user":{"displayName":"ChannelA","stream":{"id":"317331339094","viewersCount":24},"broadcastSettings":{"title":"t","game":{"id":"1","name":"VALORANT"}}}}`, &info)
	mustJSON(t, `{"user":{"stream":{"createdAt":"2026-11-22T20:31:03Z"}}}`, &start)

	s, err := toStream("channel_a", info, start)
	if err != nil || s == nil {
		t.Fatalf("stream = %v, err %v", s, err)
	}
	if want := time.Date(2026, 11, 22, 20, 31, 3, 0, time.UTC); !s.StartedAt.Equal(want) {
		t.Fatalf("started = %v, want %v", s.StartedAt, want)
	}
	ws := WatchStreak{AchievedAt: time.Date(2026, 11, 22, 21, 46, 16, 0, time.UTC)}
	if !ws.ClaimedFor(s.StartedAt) {
		t.Fatal("streak achieved during the broadcast not detected")
	}
}

func TestToStreamBannedChannel(t *testing.T) {
	if _, err := toStream("banned_channel", streamInfoData{}, streamStartData{}); err == nil {
		t.Fatal("expected ErrChannelNotFound for a missing user")
	}
}
