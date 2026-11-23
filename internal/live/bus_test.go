package live

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	minerv1 "github.com/xjl0/smart-channel-points-miner-twitch/internal/gen/miner/v1"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/store"
)

func newBus(t *testing.T) *Bus {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	st, err := store.Open(context.Background(), ":memory:", log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	b, err := New(context.Background(), st, log)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func channelUpdated(id string) *minerv1.LiveMessage {
	return &minerv1.LiveMessage{Body: &minerv1.LiveMessage_ChannelUpdated{ChannelUpdated: &minerv1.ChannelUpdated{
		Channel: &minerv1.Channel{Id: id},
	}}}
}

func TestSnapshotFoldsState(t *testing.T) {
	b := newBus(t)
	b.Publish(channelUpdated("a"))
	b.Publish(channelUpdated("b"))
	b.Publish(&minerv1.LiveMessage{Body: &minerv1.LiveMessage_WatchTick{WatchTick: &minerv1.WatchTick{ChannelId: "a", WatchedSeconds: 60}}})
	b.Publish(&minerv1.LiveMessage{Body: &minerv1.LiveMessage_ChannelRemoved{ChannelRemoved: &minerv1.ChannelRemoved{ChannelId: "b"}}})

	sub := b.Subscribe(0)
	defer sub.Close()
	snap := sub.Backlog[0].GetSnapshot()
	if snap == nil || sub.Backlog[0].Seq != 4 {
		t.Fatalf("expected snapshot at seq 4, got %+v", sub.Backlog[0])
	}
	if len(snap.Channels) != 1 || snap.Channels[0].WatchedSeconds != 60 {
		t.Fatalf("bad folded state: %+v", snap.Channels)
	}
}

func TestResumeReplaysMissed(t *testing.T) {
	b := newBus(t)
	for range 5 {
		b.Publish(channelUpdated("a"))
	}
	sub := b.Subscribe(3)
	defer sub.Close()
	if len(sub.Backlog) != 2 || sub.Backlog[0].Seq != 4 || sub.Backlog[1].Seq != 5 {
		t.Fatalf("replay = %v", sub.Backlog)
	}

	// Новые сообщения приходят в C.
	b.Publish(channelUpdated("a"))
	if got := <-sub.C; got.Seq != 6 {
		t.Fatalf("live seq = %d", got.Seq)
	}
}

func TestResumeTooOldGetsSnapshot(t *testing.T) {
	b := newBus(t)
	for range ringSize + 10 {
		b.Publish(channelUpdated("a"))
	}
	sub := b.Subscribe(2)
	defer sub.Close()
	if len(sub.Backlog) != 1 || sub.Backlog[0].GetSnapshot() == nil {
		t.Fatal("expected snapshot for evicted range")
	}
}

func TestFeedEventsPersisted(t *testing.T) {
	b := newBus(t)
	b.Publish(channelUpdated("a")) // только live
	b.Publish(&minerv1.LiveMessage{Body: &minerv1.LiveMessage_PointsGained{PointsGained: &minerv1.PointsGained{ChannelId: "a", Points: 10}}})
	hist, err := b.History(context.Background(), 0, "a", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 || hist[0].GetPointsGained().GetPoints() != 10 || hist[0].Seq != 2 {
		t.Fatalf("history = %v", hist)
	}
}

func TestBroadcastIsEphemeral(t *testing.T) {
	b := newBus(t)
	b.Publish(channelUpdated("a")) // seq 1
	sub := b.Subscribe(1)
	defer sub.Close()
	b.Broadcast(&minerv1.LiveMessage{Body: &minerv1.LiveMessage_ChatMessage{ChatMessage: &minerv1.ChatMessage{Text: "hi"}}})
	if got := <-sub.C; got.GetChatMessage().GetText() != "hi" || got.Seq != 1 {
		t.Fatalf("chat = %v", got)
	}
	if b.Seq() != 1 {
		t.Fatalf("broadcast must not take a seq, seq = %d", b.Seq())
	}
	// Клиенту, возобновившему поток позже, нечего воспроизводить.
	again := b.Subscribe(1)
	defer again.Close()
	if len(again.Backlog) != 0 {
		t.Fatalf("chat replayed: %v", again.Backlog)
	}
}

func TestSlotDeadlineChangesAreNotStored(t *testing.T) {
	b := newBus(t)
	slots := func(deadline int64, reason minerv1.SlotReason) *minerv1.LiveMessage {
		return &minerv1.LiveMessage{Body: &minerv1.LiveMessage_SlotsChanged{SlotsChanged: &minerv1.SlotsChanged{
			Slots: []*minerv1.Slot{{ChannelId: "a", Reason: reason, Deadline: timestamppb.New(time.Unix(deadline, 0))}},
		}}}
	}
	b.Publish(slots(1, minerv1.SlotReason_SLOT_REASON_STREAK)) // новый состав: сохраняется
	b.Publish(slots(2, minerv1.SlotReason_SLOT_REASON_STREAK)) // только дедлайн: только live
	b.Publish(slots(3, minerv1.SlotReason_SLOT_REASON_FILL))   // причина изменилась: сохраняется
	hist, err := b.History(context.Background(), 0, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 2 {
		t.Fatalf("stored %d slot changes, want 2", len(hist))
	}
}
