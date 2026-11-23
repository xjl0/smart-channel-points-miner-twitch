package engine

import (
	"slices"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

var testPolicy = Policy{
	MaxSlots:          2,
	StreakAssumeAfter: 15 * time.Minute,
	StreakAttemptMax:  30 * time.Minute,
	StreakLinger:      3 * time.Minute,
	FillHold:          30 * time.Minute,
	Jitter:            func(d time.Duration) time.Duration { return d },
}

// online создаёт включённый канал в онлайне, трансляция которого началась за `ago` до t0.
func online(id string, ago time.Duration) ChannelState {
	return ChannelState{ID: id, Enabled: true, Online: true, StreakKnown: true, BroadcastID: "b-" + id, OnlineSince: t0.Add(-ago)}
}

func claimed(c ChannelState, watched int64) ChannelState {
	c.StreakClaimed, c.WatchedSeconds = true, watched
	return c
}

func streakSlot(id string, started time.Time) Slot {
	return Slot{ChannelID: id, BroadcastID: "b-" + id, Reason: ReasonStreak, StartedAt: started, Deadline: started.Add(15 * time.Minute)}
}

func fillSlot(id string, started time.Time) Slot {
	return Slot{ChannelID: id, BroadcastID: "b-" + id, Reason: ReasonFill, StartedAt: started, Deadline: started.Add(30 * time.Minute)}
}

// summary выводит слоты в виде "id:reason" для компактных проверок.
func summary(slots []Slot) []string {
	out := make([]string, len(slots))
	for i, s := range slots {
		out[i] = s.ChannelID + ":" + string(s.Reason)
	}
	return out
}

func TestDecide(t *testing.T) {
	cases := []struct {
		name        string
		now         time.Time
		channels    []ChannelState
		current     []Slot
		want        []string
		wantAssumed []string
	}{
		{
			name: "nobody online → watch nobody",
			now:  t0,
			channels: []ChannelState{
				{ID: "a", Enabled: true},
			},
			want: []string{},
		},
		{
			name:     "streak queue is FIFO by broadcast start, max 2 slots",
			now:      t0,
			channels: []ChannelState{online("new", time.Minute), online("old", time.Hour), online("mid", 10*time.Minute)},
			want:     []string{"old:streak", "mid:streak"},
		},
		{
			name:     "current streak attempt is not preempted by an older broadcast",
			now:      t0,
			channels: []ChannelState{online("a", time.Minute), online("b", time.Minute), online("older", time.Hour)},
			current:  []Slot{streakSlot("a", t0.Add(-time.Minute)), streakSlot("b", t0.Add(-time.Minute))},
			want:     []string{"a:streak", "b:streak"},
		},
		{
			name: "no streak after 15 min of watching → assumed received, next in queue, a stays as filler",
			now:  t0,
			channels: []ChannelState{
				func() ChannelState {
					c := online("a", time.Hour)
					c.WatchedSeconds, c.StreakSeconds = 900, 900
					return c
				}(),
				online("b", time.Minute),
			},
			current:     []Slot{streakSlot("a", t0.Add(-15*time.Minute))},
			want:        []string{"b:streak", "a:fill"},
			wantAssumed: []string{"a"},
		},
		{
			name: "15 min on the clock but only 10 credited (PubSub was down) → attempt extended by 5",
			now:  t0,
			channels: []ChannelState{
				func() ChannelState {
					c := online("a", time.Hour)
					c.WatchedSeconds, c.StreakSeconds = 900, 600
					return c
				}(),
				online("b", time.Minute),
			},
			current: []Slot{streakSlot("a", t0.Add(-15*time.Minute))},
			want:    []string{"a:streak", "b:streak"},
		},
		{
			name: "broadcast already watched 15 min (e.g. before restart) → assumed, only as filler",
			now:  t0,
			channels: []ChannelState{
				func() ChannelState {
					c := online("a", time.Hour)
					c.WatchedSeconds, c.StreakSeconds = 900, 900
					return c
				}(),
			},
			want:        []string{"a:fill"},
			wantAssumed: []string{"a"},
		},
		{
			name:     "streak arrived → linger instead of leaving at once",
			now:      t0,
			channels: []ChannelState{claimed(online("a", time.Hour), 0)},
			current:  []Slot{streakSlot("a", t0.Add(-5*time.Minute))},
			want:     []string{"a:streak"},
		},
		{
			name:     "linger over → slot turns into fill when it is the least watched",
			now:      t0,
			channels: []ChannelState{claimed(online("a", time.Hour), 600), claimed(online("b", time.Hour), 60)},
			current: []Slot{{ChannelID: "a", BroadcastID: "b-a", Reason: ReasonStreak, Lingering: true,
				StartedAt: t0.Add(-10 * time.Minute), Deadline: t0.Add(-time.Second)}},
			want: []string{"b:fill", "a:fill"},
		},
		{
			name:     "all streaks done → fill picks least watched during current broadcast",
			now:      t0,
			channels: []ChannelState{claimed(online("a", time.Hour), 3000), claimed(online("b", time.Hour), 100), claimed(online("c", time.Hour), 900)},
			want:     []string{"b:fill", "c:fill"},
		},
		{
			name:     "new streak preempts a fill slot",
			now:      t0,
			channels: []ChannelState{claimed(online("a", time.Hour), 0), claimed(online("b", time.Hour), 0), online("s", time.Minute)},
			current:  []Slot{fillSlot("a", t0.Add(-time.Minute)), fillSlot("b", t0.Add(-time.Minute))},
			want:     []string{"s:streak", "a:fill"},
		},
		{
			name:     "fill is held for the minimum time even if another is less watched",
			now:      t0,
			channels: []ChannelState{claimed(online("a", time.Hour), 5000), claimed(online("b", time.Hour), 0)},
			current:  []Slot{fillSlot("a", t0.Add(-time.Minute))},
			want:     []string{"a:fill", "b:fill"},
		},
		{
			name: "after hold, fill moves to a less watched channel",
			now:  t0,
			channels: []ChannelState{
				claimed(online("a", time.Hour), 5000), claimed(online("b", time.Hour), 4000), claimed(online("c", time.Hour), 10),
			},
			current: []Slot{fillSlot("a", t0.Add(-31*time.Minute)), fillSlot("b", t0.Add(-time.Minute))},
			want:    []string{"b:fill", "c:fill"},
		},
		{
			name:     "slot released when channel goes offline",
			now:      t0,
			channels: []ChannelState{{ID: "a", Enabled: true, Online: false}},
			current:  []Slot{streakSlot("a", t0.Add(-time.Minute))},
			want:     []string{},
		},
		{
			name: "disabled channel is never watched",
			now:  t0,
			channels: []ChannelState{
				func() ChannelState { c := online("a", time.Hour); c.Enabled = false; return c }(),
			},
			want: []string{},
		},
		{
			name: "new broadcast on a watched channel restarts the streak attempt",
			now:  t0,
			channels: []ChannelState{
				func() ChannelState { c := online("a", time.Minute); c.BroadcastID = "b-a-2"; return c }(),
			},
			current: []Slot{fillSlot("a", t0.Add(-time.Minute))},
			want:    []string{"a:streak"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := Decide(testPolicy, tc.now, tc.channels, tc.current)
			if got := summary(d.Slots); !slices.Equal(got, tc.want) {
				t.Errorf("slots = %v, want %v", got, tc.want)
			}
			if !slices.Equal(d.Assumed, tc.wantAssumed) {
				t.Errorf("assumed = %v, want %v", d.Assumed, tc.wantAssumed)
			}
			if tc.name == "streak queue is FIFO by broadcast start, max 2 slots" && !slices.Equal(d.Queue, []string{"new"}) {
				t.Errorf("queue = %v, want [new]", d.Queue)
			}
			if len(d.Slots) > testPolicy.MaxSlots {
				t.Errorf("more than %d slots: %d", testPolicy.MaxSlots, len(d.Slots))
			}
		})
	}
}

func TestDecideLingerDeadline(t *testing.T) {
	channels := []ChannelState{claimed(online("a", time.Hour), 0)}
	d := Decide(testPolicy, t0, channels, []Slot{streakSlot("a", t0.Add(-5*time.Minute))})
	s := d.Slots[0]
	if !s.Lingering || !s.Deadline.Equal(t0.Add(3*time.Minute)) {
		t.Fatalf("linger not started: %+v", s)
	}
	// Через минуту всё ещё досматривает, освобождается после дедлайна.
	if d2 := Decide(testPolicy, t0.Add(time.Minute), channels, d.Slots); d2.Slots[0].Reason != ReasonStreak {
		t.Fatalf("released too early: %v", summary(d2.Slots))
	}
	if d3 := Decide(testPolicy, t0.Add(4*time.Minute), channels, d.Slots); d3.Slots[0].Reason != ReasonFill {
		t.Fatalf("linger not released: %v", summary(d3.Slots))
	}
}

func TestDecideCountsAlreadyWatchedTime(t *testing.T) {
	c := online("a", time.Hour)
	c.WatchedSeconds, c.StreakSeconds = 600, 600 // 10 из 15 минут уже просмотрено на этой трансляции
	d := Decide(testPolicy, t0, []ChannelState{c}, nil)
	if len(d.Slots) != 1 || !d.Slots[0].Deadline.Equal(t0.Add(5*time.Minute)) {
		t.Fatalf("want streak slot with 5 min left, got %+v", d.Slots)
	}
}

func TestDecideQueuePriority(t *testing.T) {
	expiring := func(id string, in time.Duration) ChannelState {
		c := online(id, time.Minute)
		c.StreakExpires = t0.Add(in)
		return c
	}
	unchecked := online("unchecked", 2*time.Hour)
	unchecked.StreakKnown = false

	d := Decide(testPolicy, t0, []ChannelState{
		online("noexpiry", 3*time.Hour), // самая старая трансляция, но терять нечего: стрика нет
		expiring("later", 48*time.Hour),
		expiring("soon", 2*time.Hour),
		unchecked, // не ставится в очередь, пока не известно состояние его стрика
	}, nil)
	if got := summary(d.Slots); !slices.Equal(got, []string{"soon:streak", "later:streak"}) {
		t.Fatalf("slots = %v", got)
	}
	if !slices.Equal(d.Queue, []string{"noexpiry"}) {
		t.Fatalf("queue = %v", d.Queue)
	}
}

func TestDecideStreakRetryGoesFirst(t *testing.T) {
	retry := online("retry", time.Minute)
	retry.StreakRetry = true
	retry.WatchedSeconds = 3000 // до повтора просмотрено много, но таймер запущен заново
	soon := online("soon", time.Hour)
	soon.StreakExpires = t0.Add(time.Hour)

	d := Decide(testPolicy, t0, []ChannelState{soon, retry, online("other", 2*time.Hour)}, nil)
	if got := summary(d.Slots); !slices.Equal(got, []string{"retry:streak", "soon:streak"}) {
		t.Fatalf("slots = %v", got)
	}
	if !d.Slots[0].Deadline.Equal(t0.Add(15 * time.Minute)) {
		t.Fatalf("retry must get a full 15 min, deadline %v", d.Slots[0].Deadline)
	}
}

func TestDecideExtensionDeadline(t *testing.T) {
	c := online("a", time.Hour)
	c.WatchedSeconds, c.StreakSeconds = 900, 600 // 5 минут не засчитаны
	d := Decide(testPolicy, t0, []ChannelState{c}, []Slot{streakSlot("a", t0.Add(-15*time.Minute))})
	if len(d.Slots) != 1 || !d.Slots[0].Deadline.Equal(t0.Add(5*time.Minute)) {
		t.Fatalf("want the attempt extended to t0+5m, got %+v", d.Slots)
	}
}

func TestDecideAttemptTimeLimit(t *testing.T) {
	// 30 минут по часам, засчитана только 1: дальше слот не держим, стрик не засчитываем.
	c := online("a", time.Hour)
	c.WatchedSeconds, c.StreakSeconds = 1800, 60
	d := Decide(testPolicy, t0, []ChannelState{c, online("b", time.Minute)}, []Slot{streakSlot("a", t0.Add(-30*time.Minute))})
	if !slices.Equal(d.TimedOut, []string{"a"}) || len(d.Assumed) != 0 {
		t.Fatalf("timed out %v, assumed %v; want a timed out, nothing assumed", d.TimedOut, d.Assumed)
	}
	if got := summary(d.Slots); !slices.Equal(got, []string{"b:streak"}) {
		t.Fatalf("slots = %v, want the next channel to get its attempt", got)
	}

	// Продление никогда не уходит за потолок.
	c.StreakSeconds = 60
	d = Decide(testPolicy, t0, []ChannelState{c}, []Slot{streakSlot("a", t0.Add(-20*time.Minute))})
	if len(d.Slots) != 1 || !d.Slots[0].Deadline.Equal(t0.Add(10*time.Minute)) {
		t.Fatalf("extension must stop at the 30 min limit, got %+v", d.Slots)
	}

	// Канал «на отдыхе» в очередь не берётся.
	cool := online("cool", time.Hour)
	cool.StreakCooldown = true
	if d := Decide(testPolicy, t0, []ChannelState{cool}, nil); len(d.Slots) != 0 {
		t.Fatalf("channel in cooldown was taken: %v", summary(d.Slots))
	}
}
