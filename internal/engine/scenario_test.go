package engine

// Сценарные тесты: реальный движок против фейкового Twitch (faketwitch_test.go)
// с управляемыми часами. Тест играет роль горутины Run движка: он двигает
// время минута за минутой, отправляет реальные события minute-watched через
// реальный HTTP-клиент, воспроизводит записанные сообщения PubSub и применяет
// результаты фоновых запросов (pump). После каждого шага проверяются жёсткие
// лимиты: не больше двух просмотров, чаты ровно просматриваемых каналов.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	minerv1 "github.com/xjl0/smart-channel-points-miner-twitch/internal/gen/miner/v1"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/live"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/store"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/store/db"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/twitch"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/twitch/chat"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/twitch/pubsub"
)

type sim struct {
	t     *testing.T
	e     *Engine
	tw    *fakeTwitch
	bus   *live.Bus
	ctx   context.Context
	clock time.Time
	// pubsubDown имитирует отвалившийся PubSub: Twitch тогда не засчитывает просмотр.
	pubsubDown bool
}

func newSim(t *testing.T) *sim {
	t.Helper()
	s := &sim{t: t, clock: time.Date(2026, 11, 22, 20, 0, 0, 0, time.UTC)}
	s.tw = newFakeTwitch(t, func() time.Time { return s.clock })

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	st, err := store.Open(context.Background(), ":memory:", log)
	if err != nil {
		t.Fatal(err)
	}
	if s.bus, err = live.New(context.Background(), st, log); err != nil {
		t.Fatal(err)
	}
	tw := twitch.New(log, nil, twitch.WithEndpoints(s.tw.srv.URL+"/gql", s.tw.srv.URL+"/spade"))
	tw.SetAuth(twitch.Auth{Token: "test-token", UserID: "1000", Login: "me", DeviceID: "dev"})

	p := DefaultPolicy()
	p.Jitter = func(d time.Duration) time.Duration { return d } // досмотр ровно 3m, удержание 30m
	s.e = New(log, p, st, tw, pubsub.New(log, nil), chat.New(log, nil), s.bus)
	s.e.now = func() time.Time { return s.clock }
	s.e.presence = func() bool { return !s.pubsubDown }
	s.e.timing = DefaultTimings()
	s.e.timing.FirstBeacon = [2]time.Duration{24 * time.Hour, 24 * time.Hour} // события minute-watched отправляет sim.minute
	s.e.timing.BeaconEvery = time.Hour
	s.e.timing.ChestDelay = [2]time.Duration{0, 0}
	s.e.timing.MomentDelay = [2]time.Duration{0, 0}
	s.e.timing.StreamUpRetries = nil
	s.e.settings = settingsFrom(s.e.policy, s.e.timing)
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx, s.e.ctx, s.e.authed = ctx, ctx, true
	t.Cleanup(func() {
		cancel()
		close(s.e.stopped)
		_ = st.Close()
	})
	return s
}

// pump применяет результаты фоновых запросов, пока не останется ни одного незавершённого.
func (s *sim) pump() { s.pumpFor(0) }

// pumpFor также ждёт не меньше min (для отложенных действий вроде нажатия на сундук).
func (s *sim) pumpFor(min time.Duration) {
	deadline := time.Now().Add(min)
	timeout := time.After(10 * time.Second)
	for {
		select {
		case op := <-s.e.ops:
			op()
			continue
		case <-timeout:
			s.t.Fatal("engine did not settle: background requests still pending")
		default:
		}
		if s.e.pending.Load() == 0 && time.Now().After(deadline) {
			s.checkLimits()
			return
		}
		time.Sleep(time.Millisecond)
	}
}

// api вызывает публичный метод движка (который идёт через e.call), параллельно выполняя pump.
func (s *sim) api(fn func() error) error {
	done := make(chan error, 1)
	go func() { done <- fn() }()
	for {
		select {
		case err := <-done:
			s.pump()
			return err
		case op := <-s.e.ops:
			op()
		}
	}
}

func (s *sim) add(logins ...string) {
	for _, l := range logins {
		s.tw.channel(l)
		if err := s.api(func() error { _, err := s.e.AddChannel(s.ctx, l); return err }); err != nil {
			s.t.Fatalf("add %s: %v", l, err)
		}
	}
}

// poll выполняет периодическую пакетную проверку онлайна.
func (s *sim) poll() {
	s.e.refreshAll("test")
	s.pump()
}

// minute пропускает одну минуту: каждый просмотр отправляет реальное событие minute-watched
// в фейковый spade, результат учитывается, часы идут, запускается планировщик.
func (s *sim) minute() {
	for _, v := range s.viewings() {
		err := s.e.tw.SendMinuteWatched(s.ctx, *v.beacon.Load())
		s.e.onWatched(v, err)
	}
	s.clock = s.clock.Add(time.Minute)
	s.e.reconcile()
	s.pump()
}

func (s *sim) minutes(n int) {
	for range n {
		s.minute()
	}
}

func (s *sim) viewings() []*viewing {
	out := make([]*viewing, 0, len(s.e.viewing))
	for _, v := range s.e.viewing {
		out = append(out, v)
	}
	return out
}

// pubsubEvent воспроизводит записанное сообщение PubSub для заданного канала и
// ждёт, пока движок полностью отреагирует.
func (s *sim) pubsubEvent(name, login string) {
	s.t.Helper()
	s.inject(name, login)
	s.pump()
}

// inject воспроизводит записанное сообщение PubSub, не дожидаясь реакции.
func (s *sim) inject(name, login string) {
	s.t.Helper()
	var f struct {
		Topic   string          `json:"topic"`
		Message json.RawMessage `json:"message"`
	}
	if err := json.Unmarshal(fixtureBytes(s.t, name), &f); err != nil {
		s.t.Fatal(err)
	}
	id := s.tw.channel(login).id
	topic, msg := f.Topic, string(f.Message)
	if orig := capturedChannelID(f.Topic, f.Message); orig != "" {
		topic, msg = strings.ReplaceAll(topic, orig, id), strings.ReplaceAll(msg, orig, id)
	}
	if ev := pubsub.ParseMessage(topic, msg); ev != nil {
		s.e.onPubSub(ev)
	}
}

// capturedChannelID находит id канала, к которому относится записанное сообщение.
func capturedChannelID(topic string, msg json.RawMessage) string {
	if strings.HasPrefix(topic, "video-playback-by-id.") || strings.HasPrefix(topic, "community-moments-channel-v1.") {
		return topic[strings.Index(topic, ".")+1:]
	}
	var m struct {
		Data struct {
			ChannelID string `json:"channel_id"`
			Claim     struct {
				ChannelID string `json:"channel_id"`
			} `json:"claim"`
		} `json:"data"`
	}
	_ = json.Unmarshal(msg, &m)
	if m.Data.ChannelID != "" {
		return m.Data.ChannelID
	}
	return m.Data.Claim.ChannelID
}

func (s *sim) ch(login string) *channel { return s.e.channels[s.tw.channel(login).id] }

// watching возвращает "login:reason" текущих слотов в отсортированном виде.
func (s *sim) watching() []string {
	var out []string
	for id, v := range s.e.viewing {
		out = append(out, s.e.channels[id].Login+":"+string(v.slot.Reason))
	}
	sort.Strings(out)
	return out
}

func (s *sim) expectWatching(want ...string) {
	s.t.Helper()
	sort.Strings(want)
	if got := s.watching(); !slices.Equal(got, want) {
		s.t.Fatalf("at %s watching %v, want %v (queue %v)", s.clock.Format("15:04"), got, want, s.e.queue)
	}
}

// checkLimits — жёсткий инвариант, проверяемый после каждого шага.
func (s *sim) checkLimits() {
	s.t.Helper()
	if len(s.e.viewing) > 2 {
		s.t.Fatalf("more than 2 viewings: %v", s.watching())
	}
	var logins []string
	for _, v := range s.e.viewing {
		logins = append(logins, v.login)
	}
	sort.Strings(logins)
	if chats := s.e.chat.Joined(); !slices.Equal(chats, logins) {
		s.t.Fatalf("chats %v, but watching %v", chats, logins)
	}
}

// lastBeacon возвращает трансляцию последнего события minute-watched, отправленного для login.
func (s *sim) lastBeacon(login string) string {
	_, _, beacons, _ := s.tw.snapshot()
	for i := len(beacons) - 1; i >= 0; i-- {
		if l, b, _ := strings.Cut(beacons[i], "/"); l == login {
			return b
		}
	}
	return ""
}

// ---------------------------------------------------------------- сценарии

func TestScenarioStartupTakesTwoOldestLive(t *testing.T) {
	s := newSim(t)
	s.tw.goLive("old", "b-old", 3*time.Hour)
	s.tw.goLive("mid", "b-mid", time.Hour)
	s.tw.goLive("new", "b-new", 5*time.Minute)
	s.tw.channel("off") // офлайн
	s.add("old", "mid", "new", "off")

	s.expectWatching("old:streak", "mid:streak")
	if !slices.Equal(s.e.queue, []string{"id-new"}) {
		t.Fatalf("queue = %v, want [id-new]", s.e.queue)
	}
	if s.ch("off").online {
		t.Fatal("offline channel marked online")
	}
}

func TestScenarioPollIsBatched(t *testing.T) {
	s := newSim(t)
	var logins []string
	for i := range 12 {
		l := "c" + itoa(i)
		s.tw.channel(l)
		logins = append(logins, l)
	}
	s.add(logins...)
	_, before, _, _ := s.tw.snapshot()
	s.poll()
	ops, after, _, _ := s.tw.snapshot()
	if got := after["VideoPlayerStreamInfoOverlayChannel"] - before["VideoPlayerStreamInfoOverlayChannel"]; got != 2 {
		t.Fatalf("12 channels polled in %d HTTP requests, want 2 (10 per batch)", got)
	}
	if ops["WithIsStreamLiveQuery"] != ops["VideoPlayerStreamInfoOverlayChannel"] {
		t.Fatalf("start time not fetched together with stream info: %v", ops)
	}
}

func TestScenarioStreakArrivesThenSlotMovesOn(t *testing.T) {
	s := newSim(t)
	s.tw.goLive("a", "b-a", 2*time.Hour)
	s.tw.goLive("b", "b-b", time.Hour)
	s.tw.goLive("c", "b-c", time.Minute)
	s.tw.set("a", func(c *fakeChannel) { c.streakAfterMinute = 6 }) // Twitch выдаёт стрик a через 6 минут
	s.add("a", "b", "c")
	s.expectWatching("a:streak", "b:streak") // c ждёт: сначала доводятся до конца текущие попытки

	s.minutes(6) // стрик выдан на 6-м событии minute-watched, найден проверкой RewardList
	if !s.ch("a").streakClaimed {
		t.Fatal("streak of a not detected")
	}
	s.expectWatching("a:streak", "b:streak") // a немного досматривает после стрика

	s.minutes(3) // досмотр окончен: a полностью освобождается, c занимает слот
	s.expectWatching("b:streak", "c:streak")
}

func TestScenarioStreakTakenElsewhereIsNeverChased(t *testing.T) {
	s := newSim(t)
	c := s.tw.goLive("phone", "b-phone", time.Hour)
	s.tw.set("phone", func(f *fakeChannel) { f.streakLength, f.achievedAt = 5, c.startedAt.Add(20*time.Minute) })
	s.add("phone")

	if !s.ch("phone").streakClaimed {
		t.Fatal("streak received during this broadcast (on another device) not recognised")
	}
	s.expectWatching("phone:fill") // только как обычный просмотр (fill), никогда как попытка стрика
}

func TestScenarioNoStreakAssumedAfter15Minutes(t *testing.T) {
	s := newSim(t)
	s.tw.goLive("stingy", "b-s", 2*time.Hour) // никогда не выдаёт стрик
	s.tw.goLive("next", "b-n", time.Hour)
	s.tw.goLive("third", "b-t", time.Minute)
	s.add("stingy", "next", "third")
	s.expectWatching("stingy:streak", "next:streak")

	s.minutes(14)
	s.expectWatching("stingy:streak", "next:streak")
	s.minutes(1) // 15 минут просмотрено: оба считаются полученными; третий получает попытку,
	// наименее просмотренный из двух остаётся обычным просмотром (ничья → более старая трансляция)
	s.expectWatching("third:streak", "stingy:fill")
	if !s.ch("stingy").streakClaimed {
		t.Fatal("streak not assumed after 15 minutes")
	}
}

func TestScenarioStreamUpAndDownViaPubSub(t *testing.T) {
	s := newSim(t)
	s.tw.goLive("a", "b-a", time.Hour)
	s.tw.channel("late")
	s.add("a", "late")
	s.expectWatching("a:streak")

	s.tw.goLive("late", "b-late", 0)
	s.pubsubEvent("pubsub_stream_up", "late")
	s.expectWatching("a:streak", "late:streak")

	s.tw.goOffline("a")
	s.pubsubEvent("pubsub_stream_down", "a")
	s.expectWatching("late:streak")
	if s.ch("a").online {
		t.Fatal("stream-down did not take the channel offline")
	}
}

func TestScenarioStreamRestartMovesBeaconsToNewBroadcast(t *testing.T) {
	s := newSim(t)
	s.tw.goLive("a", "first", time.Hour)
	s.add("a")
	s.minutes(2)
	if got := s.lastBeacon("a"); got != "first" {
		t.Fatalf("beacon broadcast %q, want first", got)
	}

	s.tw.goLive("a", "second", 0) // стример перезапускает стрим
	s.poll()
	s.minutes(1)
	if got := s.lastBeacon("a"); got != "second" {
		t.Fatalf("after restart beacons go to %q, want second", got)
	}
	s.expectWatching("a:streak")
}

func TestScenarioBannedChannelAndUnban(t *testing.T) {
	s := newSim(t)
	s.tw.goLive("a", "b-a", time.Hour)
	s.add("a")
	s.tw.set("a", func(c *fakeChannel) { c.banned = true })
	s.poll()
	if !s.ch("a").missing {
		t.Fatal("banned channel not marked missing")
	}
	s.expectWatching()

	s.tw.set("a", func(c *fakeChannel) { c.banned = false })
	s.poll()
	if s.ch("a").missing {
		t.Fatal("unbanned channel still missing")
	}
	s.expectWatching("a:streak")
}

func TestScenarioRetryStreakGoesFirst(t *testing.T) {
	s := newSim(t)
	s.tw.goLive("a", "b-a", time.Hour)
	s.add("a")
	s.minutes(15) // считается полученным, продолжает смотреться как обычный просмотр
	s.expectWatching("a:fill")

	if err := s.api(func() error { return s.e.RetryStreak(s.ch("a").ID) }); err != nil {
		t.Fatal(err)
	}
	s.expectWatching("a:streak")
	if v := s.e.viewing[s.ch("a").ID]; !v.slot.Deadline.Equal(s.clock.Add(15 * time.Minute)) {
		t.Fatalf("retry deadline %v, want a fresh 15 minutes", v.slot.Deadline)
	}
	s.tw.set("a", func(c *fakeChannel) { c.streakAfterMinute = c.minutes + 4 })
	s.minutes(6)
	if !s.ch("a").streakClaimed || s.ch("a").streakRetry {
		t.Fatal("retried streak not received")
	}
}

func TestScenarioBonusChest(t *testing.T) {
	s := newSim(t)
	s.tw.goLive("a", "b-a", time.Hour)
	s.add("a")

	s.pubsubEvent("pubsub_claim_available", "a")
	_, _, _, claims := s.tw.snapshot()
	if len(claims) != 1 {
		t.Fatalf("chest clicks = %v, want one", claims)
	}

	// Сначала сундук забрал другой клиент (claim-claimed): нажимать нельзя.
	s.e.timing.ChestDelay = [2]time.Duration{150 * time.Millisecond, 150 * time.Millisecond}
	s.ch("a").lastClaimID = ""              // записанная фикстура повторно использует тот же id сундука
	s.inject("pubsub_claim_available", "a") // начинаем выжидать своё "время реакции"…
	s.inject("pubsub_claim_claimed", "a")   // …пока другой клиент его забирает
	s.pumpFor(300 * time.Millisecond)
	if _, _, _, claims = s.tw.snapshot(); len(claims) != 1 {
		t.Fatalf("clicked a chest that was already taken: %v", claims)
	}
}

func TestScenarioPointsViewersAndIgnoredMessages(t *testing.T) {
	s := newSim(t)
	s.tw.goLive("a", "b-a", time.Hour)
	s.add("a")
	sub := s.bus.Subscribe(0)
	defer sub.Close()

	s.pubsubEvent("pubsub_points_earned", "a")
	var earned struct {
		Data struct {
			Balance struct {
				Balance int64 `json:"balance"`
			} `json:"balance"`
		} `json:"data"`
	}
	var f struct {
		Message json.RawMessage `json:"message"`
	}
	_ = json.Unmarshal(fixtureBytes(t, "pubsub_points_earned"), &f)
	_ = json.Unmarshal(f.Message, &earned)
	if got := s.ch("a").Points; got != earned.Data.Balance.Balance {
		t.Fatalf("balance %d, want %d from the captured event", got, earned.Data.Balance.Balance)
	}

	s.pubsubEvent("pubsub_viewcount", "a")
	if s.ch("a").viewers == 0 {
		t.Fatal("viewer count not applied")
	}
	var sawViewers bool
	for len(sub.C) > 0 {
		if m := <-sub.C; m.GetViewersChanged() != nil {
			sawViewers = true
		}
	}
	if !sawViewers {
		t.Fatal("viewer count not pushed to the panel")
	}

	s.pubsubEvent("pubsub_commercial", "a") // реклама: записывается, но ни на что не влияет
	s.expectWatching("a:streak")
	_ = minerv1.SlotReason_SLOT_REASON_STREAK
}

func TestScenarioNewStreamerPreemptsFiller(t *testing.T) {
	s := newSim(t)
	// Два канала, чьи стрики на этой трансляции уже получены: оба — обычный просмотр.
	for _, l := range []string{"x", "y"} {
		c := s.tw.goLive(l, "b-"+l, time.Hour)
		s.tw.set(l, func(f *fakeChannel) { f.streakLength, f.achievedAt = 3, c.startedAt.Add(time.Minute) })
	}
	s.tw.channel("fresh")
	s.add("x", "y", "fresh")
	s.expectWatching("x:fill", "y:fill")

	s.tw.goLive("fresh", "b-fresh", 0)
	s.pubsubEvent("pubsub_stream_up", "fresh")
	// Стрик к получению важнее обычного просмотра: один fill-канал полностью освобождается
	// (выход из чата), и fresh сразу занимает его слот.
	w := s.watching()
	if len(w) != 2 || !slices.Contains(w, "fresh:streak") {
		t.Fatalf("watching %v, want fresh:streak plus one filler", w)
	}
}

func TestScenarioExpiringStreakIsServedFirst(t *testing.T) {
	s := newSim(t)
	s.tw.goLive("busy1", "b1", 3*time.Hour)
	s.tw.goLive("busy2", "b2", 3*time.Hour)
	s.add("busy1", "busy2") // оба слота заняты попытками
	s.tw.goLive("early", "b-early", 2*time.Hour)
	s.tw.goLive("expiring", "b-exp", time.Minute)
	s.add("early", "expiring")
	// Стрик "expiring" вот-вот сгорит: RewardList сообщает срок истечения.
	s.e.channels[s.ch("expiring").ID].StreakExpiresAt = ptr(s.clock.Add(time.Hour))
	s.e.reconcile()
	s.pump()
	if !slices.Equal(s.e.queue, []string{s.ch("expiring").ID, s.ch("early").ID}) {
		t.Fatalf("queue = %v, want expiring before early", s.e.queue)
	}
}

func TestScenarioMinutesWithoutPubSubDoNotCount(t *testing.T) {
	s := newSim(t)
	s.tw.goLive("a", "b-a", time.Hour) // стрик так и не придёт
	s.add("a")

	s.minutes(5) // 5 засчитанных минут
	s.pubsubDown = true
	s.minutes(10) // 10 минут без PubSub: маяки уходят, но в отсчёт не идут
	s.expectWatching("a:streak")
	if s.ch("a").streakClaimed {
		t.Fatal("streak assumed although only 5 minutes were credited")
	}
	if !s.ch("a").streakPaused {
		t.Fatal("streak timer not paused while PubSub is down")
	}

	s.pubsubDown = false
	s.minutes(9) // итого 14 засчитанных
	s.expectWatching("a:streak")
	s.minutes(1) // 15 засчитанных: теперь честно считаем полученным
	if !s.ch("a").streakClaimed {
		t.Fatal("streak not assumed after 15 credited minutes")
	}
	if s.ch("a").streakPaused {
		t.Fatal("streak timer still paused after PubSub came back")
	}
	if got := s.ch("a").watched; got != 25*60 {
		t.Fatalf("total watch time %ds, want 1500 (uncredited minutes still count as watched)", got)
	}
}

func TestScenarioMissedStreamUpIsCaughtAfterReconnect(t *testing.T) {
	s := newSim(t)
	s.tw.goLive("a", "b-a", time.Hour)
	s.tw.channel("late")
	s.add("a", "late")
	s.expectWatching("a:streak")

	// PubSub лежал: "late" вышла в эфир, а событие stream-up до нас не дошло.
	s.tw.goLive("late", "b-late", 10*time.Minute)
	s.minute()
	s.expectWatching("a:streak") // без события мы о ней не знаем

	// Соединение восстановилось — движок сам перепроверяет каналы.
	s.e.onPubSub(pubsub.Reconnected{})
	s.pump()
	s.expectWatching("a:streak", "late:streak")
}

func TestScenarioUncreditedAttemptIsReleasedAndVisible(t *testing.T) {
	s := newSim(t)
	s.tw.goLive("stuck", "b-s", 2*time.Hour)
	s.tw.goLive("next", "b-n", time.Hour)
	s.tw.goLive("third", "b-t", time.Minute)
	s.add("stuck", "next", "third")
	s.expectWatching("next:streak", "stuck:streak")

	// Twitch перестал принимать минуты: слоты показывают ошибки, отсчёт не идёт.
	s.tw.mu.Lock()
	s.tw.spadeErr = true
	s.tw.mu.Unlock()
	s.minutes(3)
	if got := s.e.viewing[s.ch("stuck").ID].failures; got != 3 {
		t.Fatalf("failures = %d, want 3", got)
	}
	sub := s.bus.Subscribe(s.bus.Seq())
	defer sub.Close()
	s.minute()
	var shown int32
	for len(sub.C) > 0 {
		if sc := (<-sub.C).GetSlotsChanged(); sc != nil {
			for _, sl := range sc.Slots {
				shown = max(shown, sl.BeaconFailures)
			}
		}
	}
	if shown != 4 {
		t.Fatalf("panel shows %d failures, want 4", shown)
	}

	// 30 минут по часам без засчитанных минут: оба отпущены без зачёта, third получает попытку.
	s.minutes(26)
	if w := s.watching(); !slices.Contains(w, "third:streak") {
		t.Fatalf("watching %v: the waiting channel never got a slot", w)
	}
	if s.ch("stuck").streakClaimed || s.ch("next").streakClaimed {
		t.Fatal("streak assumed although no minute was credited")
	}
	if !s.e.now().Before(s.ch("stuck").streakCooldownUntil) {
		t.Fatal("released channel is not resting")
	}
}

func TestScenarioChestOnlyOnOurSlots(t *testing.T) {
	s := newSim(t)
	s.tw.goLive("ours", "b-o", time.Hour)
	c := s.tw.goLive("browser", "b-b", time.Hour) // пользователь смотрит его сам
	s.tw.set("browser", func(f *fakeChannel) { f.streakLength, f.achievedAt = 1, c.startedAt.Add(time.Minute) })
	s.add("ours", "browser")
	s.e.policy.MaxSlots = 1 // в слоте только "ours"
	s.e.reconcile()
	s.pump()
	s.expectWatchingSlots(1)

	s.pubsubEvent("pubsub_claim_available", "browser") // сундук на канале, который смотрят в браузере
	if _, _, _, claims := s.tw.snapshot(); len(claims) != 0 {
		t.Fatalf("clicked a chest on a channel we do not watch: %v", claims)
	}
	s.pubsubEvent("pubsub_claim_available", "ours")
	if _, _, _, claims := s.tw.snapshot(); len(claims) != 1 {
		t.Fatalf("chest on our own slot not clicked: %v", claims)
	}
}

// expectWatchingSlots проверяет число занятых слотов.
func (s *sim) expectWatchingSlots(n int) {
	s.t.Helper()
	if got := len(s.e.viewing); got != n {
		s.t.Fatalf("watching %v, want %d slot(s)", s.watching(), n)
	}
}

func TestScenarioSettingsApplyAtOnce(t *testing.T) {
	s := newSim(t)
	for _, l := range []string{"x", "y"} {
		c := s.tw.goLive(l, "b-"+l, time.Hour)
		s.tw.set(l, func(f *fakeChannel) { f.streakLength, f.achievedAt = 3, c.startedAt.Add(time.Minute) })
	}
	s.add("x", "y")
	s.expectWatching("x:fill", "y:fill")
	sub := s.bus.Subscribe(s.bus.Seq())
	defer sub.Close()

	// Неверные значения отклоняются понятной ошибкой.
	bad := DefaultSettings()
	bad.StreakAttemptMaxSec = 60 // меньше «считать полученным после» (15 мин)
	if err := s.api(func() error { _, err := s.e.UpdateSettings(s.ctx, bad); return err }); !errors.Is(err, ErrInvalidSettings) {
		t.Fatalf("invalid settings accepted: %v", err)
	}

	// «Минимум на канале» 5 минут — срок текущих слотов пересчитан сразу.
	set := DefaultSettings()
	set.FillHoldSec = 300
	if err := s.api(func() error { _, err := s.e.UpdateSettings(s.ctx, set); return err }); err != nil {
		t.Fatal(err)
	}
	for _, v := range s.e.viewing {
		if want := v.slot.StartedAt.Add(5 * time.Minute); !v.slot.Deadline.Equal(want) {
			t.Fatalf("slot deadline %v, want %v (new fill hold applied immediately)", v.slot.Deadline, want)
		}
	}
	if s.e.policy.FillHold != 5*time.Minute {
		t.Fatalf("policy fill hold = %v", s.e.policy.FillHold)
	}

	// Админка получила изменение, а в БД оно сохранено.
	var pushed bool
	for len(sub.C) > 0 {
		if sc := (<-sub.C).GetSettingsChanged(); sc != nil && sc.Settings.FillHoldSec == 300 && sc.Defaults.FillHoldSec == 1800 {
			pushed = true
		}
	}
	if !pushed {
		t.Fatal("settings change not pushed to the panel")
	}
	raw, err := s.e.store.GetSettings(dbctx())
	if err != nil {
		t.Fatal(err)
	}
	stored := &minerv1.Settings{}
	if err := proto.Unmarshal(raw, stored); err != nil || stored.FillHoldSec != 300 {
		t.Fatalf("stored settings %v (err %v)", stored, err)
	}

	// После перезапуска загружается сохранённое; пустые новые поля — по умолчанию.
	stored.SchedulerTickMinSec, stored.SchedulerTickMaxSec = 0, 0 // «старая запись без новых полей»
	raw, _ = proto.Marshal(stored)
	_ = s.e.store.SaveSettings(dbctx(), db.SaveSettingsParams{Data: raw, UpdatedAt: time.Now()})
	// Перезагрузка — через очередь движка, как и любые изменения его состояния.
	var hold time.Duration
	var tick [2]time.Duration
	err = s.api(func() error {
		return s.e.call(func() error {
			s.e.policy, s.e.timing = DefaultPolicy(), DefaultTimings()
			if err := s.e.loadSettings(); err != nil {
				return err
			}
			hold, tick = s.e.policy.FillHold, s.e.timing.SchedulerTick
			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if hold != 5*time.Minute || tick != [2]time.Duration{20 * time.Second, 40 * time.Second} {
		t.Fatalf("reloaded: fill hold %v, tick %v", hold, tick)
	}
}
