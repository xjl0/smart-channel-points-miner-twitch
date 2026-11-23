// Package engine решает, что смотреть, и управляет клиентами Twitch.
//
// Модель конкурентности (актор): всё состояние движка принадлежит горутине Run.
// Всё, что требует сетевого I/O, выполняется в фоновой горутине и передаёт
// результат обратно замыканием, исполняемым в горутине Run (см. async/do).
// Поэтому вокруг состояния нет блокировок, а гонок данных нет по построению.
package engine

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"slices"
	"sync/atomic"
	"time"

	minerv1 "github.com/xjl0/smart-channel-points-miner-twitch/internal/gen/miner/v1"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/jitter"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/live"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/store"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/twitch"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/twitch/chat"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/twitch/pubsub"
)

// Ошибки, возвращаемые в admin API.
var (
	ErrNotFound    = errors.New("channel not found")
	ErrExists      = errors.New("channel already tracked")
	ErrNotLoggedIn = errors.New("twitch account is not logged in")
	ErrOffline     = errors.New("channel is offline")
	ErrStopped     = errors.New("engine stopped")
)

// Интервалы фоновых циклов.
const (
	maxWatchFailures   = 3                    // после этого перепроверить канал
	validateEvery      = time.Hour            // проверка токена, как требует Twitch
	housekeepEvery     = 24 * time.Hour       // очистка старой истории
	eventRetention     = 90 * 24 * time.Hour  // срок хранения истории в таблице events
	broadcastRetention = 180 * 24 * time.Hour // завершённые трансляции (и их сессии просмотра)
)

// DefaultPolicy — поведение, согласованное в PLAN.md §3.2.
func DefaultPolicy() Policy {
	return Policy{
		MaxSlots:          2,
		StreakAssumeAfter: 15 * time.Minute,
		StreakAttemptMax:  30 * time.Minute,
		StreakLinger:      3 * time.Minute,  // ±1/3 → 2–4 мин
		FillHold:          30 * time.Minute, // ±1/3 → 20–40 мин
		Jitter:            func(d time.Duration) time.Duration { return jitter.Around(d, d/3) },
	}
}

// Timings — «человеческие» задержки движка (тесты их сокращают).
type Timings struct {
	FirstBeacon      [2]time.Duration // «загрузка плеера» перед первым minute-watched
	BeaconEvery      time.Duration    // период minute-watched (±4 с)
	ChestDelay       [2]time.Duration // время реакции перед кликом по сундуку
	MomentDelay      [2]time.Duration // время реакции перед получением момента
	StreamUpRetries  []time.Duration  // перепроверки после PubSub stream-up, пока GQL отстаёт
	StreakCheckEvery time.Duration    // RewardList во время попытки стрика
	StreakCooldown   time.Duration    // отдых канала после потолка попытки
	OnlinePoll       [2]time.Duration // плановая проверка онлайна всех каналов
	SchedulerTick    [2]time.Duration // шаг планировщика
}

// DefaultTimings — задержки для продакшена.
func DefaultTimings() Timings {
	return Timings{
		FirstBeacon:      [2]time.Duration{3 * time.Second, 10 * time.Second},
		BeaconEvery:      time.Minute,
		ChestDelay:       [2]time.Duration{5 * time.Second, 40 * time.Second},
		MomentDelay:      [2]time.Duration{5 * time.Second, 30 * time.Second},
		StreamUpRetries:  []time.Duration{10 * time.Second, 30 * time.Second, time.Minute},
		StreakCheckEvery: 3 * time.Minute,
		StreakCooldown:   10 * time.Minute,
		OnlinePoll:       [2]time.Duration{10 * time.Minute, 15 * time.Minute},
		SchedulerTick:    [2]time.Duration{20 * time.Second, 40 * time.Second},
	}
}

// Engine — «мозг» просмотра. Создаётся через New, запускается через Run.
type Engine struct {
	log       *slog.Logger
	store     *store.Store
	tw        *twitch.Client
	ps        *pubsub.Client
	chat      *chat.Client
	bus       *live.Bus
	policy    Policy
	timing    Timings
	now       func() time.Time  // часы планировщика (тесты ими управляют)
	settings  *minerv1.Settings // текущие таймеры из админки (см. settings.go)
	tickTimer *time.Timer       // шаг планировщика; перезапускается при смене настроек
	pollTimer *time.Timer       // плановая проверка онлайна
	// presence сообщает, подключён ли PubSub: без него Twitch не засчитывает
	// просмотр. По умолчанию — e.ps.Connected; в тестах подменяется.
	presence func() bool

	ctx     context.Context // задаётся в Run; отменяется при завершении
	ops     chan func()     // замыкания, исполняемые в горутине Run
	stopped chan struct{}   // закрывается при выходе из Run
	pending atomic.Int64    // фоновые запросы, результат которых ещё не применён

	// --- состояние, принадлежащее горутине Run ---
	account    *minerv1.Account
	authed     bool
	validating bool               // идёт проверка токена
	netStarted bool               // горутины pubsub/chat запущены
	cancelPoll context.CancelFunc // активный опрос device-login
	channels   map[string]*channel
	slots      []Slot
	viewing    map[string]*viewing // по id канала
	queue      []string
}

// New собирает движок; до вызова Run он ничего не делает.
func New(log *slog.Logger, policy Policy, st *store.Store, tw *twitch.Client, ps *pubsub.Client, ch *chat.Client, bus *live.Bus) *Engine {
	return &Engine{
		log:      log.With("component", "engine"),
		store:    st,
		tw:       tw,
		ps:       ps,
		chat:     ch,
		bus:      bus,
		policy:   policy,
		timing:   DefaultTimings(),
		now:      time.Now,
		presence: ps.Connected,
		ops:      make(chan func()),
		stopped:  make(chan struct{}),
		account:  &minerv1.Account{},
		channels: map[string]*channel{},
		viewing:  map[string]*viewing{},
	}
}

// Run выполняет цикл движка, пока ctx не отменён.
func (e *Engine) Run(ctx context.Context) error {
	e.ctx = ctx
	defer close(e.stopped)

	if err := e.loadSettings(); err != nil {
		return err
	}
	e.publishSettings()
	if err := e.loadChannels(); err != nil {
		return err
	}
	e.restoreAccount()

	e.tickTimer = time.NewTimer(jitter.Between(e.timing.SchedulerTick[0], e.timing.SchedulerTick[1]))
	// Round(0) убирает монотонную часть: монотонные часы не идут во время сна,
	// и паузу было бы не заметить. Сравниваем по настенным часам.
	lastTick := time.Now().Round(0)
	e.pollTimer = time.NewTimer(jitter.Between(e.timing.OnlinePoll[0], e.timing.OnlinePoll[1]))
	validate := time.NewTimer(jitter.Around(validateEvery, 5*time.Minute))
	housekeep := time.NewTimer(time.Minute)
	defer e.tickTimer.Stop()
	defer e.pollTimer.Stop()
	defer validate.Stop()
	defer housekeep.Stop()

	e.log.Info("engine started", "channels", len(e.channels), "streak_assume_after", e.policy.StreakAssumeAfter,
		"fill_hold", e.policy.FillHold, "attempt_max", e.policy.StreakAttemptMax)
	for {
		select {
		case <-ctx.Done():
			e.shutdown()
			return nil
		case op := <-e.ops:
			op()
		case ev := <-e.ps.Events():
			e.onPubSub(ev)
		case rs := <-e.chat.RoomStates():
			e.onRoomState(rs)
		case <-e.tickTimer.C:
			// Таймер срабатывает раз в шаг планировщика (по умолчанию 20–40 с). Если
			// с прошлого раза по часам прошло намного больше, процесс стоял (сон
			// компьютера, заморозка VM, зависание): события могли пройти мимо.
			if gap := time.Since(lastTick); gap > 3*e.timing.SchedulerTick[1] {
				e.log.Info("long pause detected, rechecking channels", "pause", gap.Round(time.Second))
				e.refreshAll("after a pause")
			}
			lastTick = time.Now().Round(0)
			e.reconcile()
			e.tickTimer.Reset(jitter.Between(e.timing.SchedulerTick[0], e.timing.SchedulerTick[1]))
		case <-e.pollTimer.C:
			e.refreshAll("periodic check")
			e.pollTimer.Reset(jitter.Between(e.timing.OnlinePoll[0], e.timing.OnlinePoll[1]))
		case <-validate.C:
			e.revalidate()
			validate.Reset(jitter.Around(validateEvery, 5*time.Minute))
		case <-housekeep.C:
			e.pruneHistory()
			housekeep.Reset(housekeepEvery)
		}
	}
}

// do выполняет fn в горутине Run (без ожидания результата).
func (e *Engine) do(fn func()) {
	select {
	case e.ops <- fn:
	case <-e.stopped:
	}
}

// call выполняет fn в горутине Run и ждёт результата.
func (e *Engine) call(fn func() error) error {
	res := make(chan error, 1)
	select {
	case e.ops <- func() { res <- fn() }:
	case <-e.stopped:
		return ErrStopped
	}
	select {
	case err := <-res:
		return err
	case <-e.stopped:
		return ErrStopped
	}
}

// async выполняет io в фоне; возвращённое им замыкание (если есть) затем
// применяется в горутине Run. Только так состояние соприкасается с сетью.
func (e *Engine) async(io func(ctx context.Context) (apply func())) {
	e.pending.Add(1)
	go func() {
		apply := io(e.ctx)
		if apply == nil || e.ctx.Err() != nil {
			e.pending.Add(-1)
			return
		}
		e.do(func() {
			defer e.pending.Add(-1)
			apply()
		})
	}()
}

// reconcile запускает планировщик и применяет его решение. Вызывается по таймеру
// и после каждого изменения состояния, которое может повлиять на то, что мы смотрим.
func (e *Engine) reconcile() {
	if !e.authed {
		e.applySlots(nil)
		return
	}
	d := Decide(e.policy, e.now(), e.channelStates(), e.slots)
	for _, id := range d.TimedOut {
		if ch := e.channels[id]; ch != nil {
			ch.streakCooldownUntil = e.now().Add(e.timing.StreakCooldown)
			e.logger(ch).Warn("streak attempt hit the time limit: watch minutes are not being credited, letting other channels go first",
				"limit", e.policy.StreakAttemptMax, "credited", time.Duration(ch.watched-ch.streakBase)*time.Second, "retry_after", e.timing.StreakCooldown)
		}
	}
	for _, id := range d.Assumed {
		if ch := e.channels[id]; ch != nil {
			e.recordStreak(ch, 0, true, "no streak after watching, assuming it was received earlier")
		}
	}
	e.applySlots(d.Slots)
	e.scheduleStreakChecks()

	if !slices.Equal(d.Queue, e.queue) {
		e.queue = d.Queue
		e.publish(&minerv1.LiveMessage{Body: &minerv1.LiveMessage_QueueChanged{
			QueueChanged: &minerv1.QueueChanged{ChannelIds: slices.Clone(d.Queue)},
		}})
	}
}

// pruneHistory годами держит базу маленькой: старые события и старые
// завершённые трансляции (с их сессиями просмотра) удаляются, затем SQLite сливает WAL
// обратно в основной файл и обновляет статистику запросов. Освобождённые страницы
// переиспользуются, поэтому файл перестаёт расти, как только достигнут срок хранения.
func (e *Engine) pruneHistory() {
	now := time.Now().UTC() // время хранится в UTC
	events, err := e.store.DeleteEventsBefore(dbctx(), now.Add(-eventRetention))
	if err != nil {
		e.log.Error("prune events", "err", err)
	}
	broadcasts, err := e.store.DeleteBroadcastsEndedBefore(dbctx(), ptr(now.Add(-broadcastRetention)))
	if err != nil {
		e.log.Error("prune broadcasts", "err", err)
	}
	if err := e.store.Maintain(dbctx()); err != nil {
		e.log.Error("database maintenance", "err", err)
	}
	// Показатели ресурсов раз в день: медленная утечка проявится здесь за недели.
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	e.log.Info("housekeeping done", "events_deleted", events, "broadcasts_deleted", broadcasts,
		"heap_mb", mem.HeapAlloc>>20, "goroutines", runtime.NumGoroutine(), "channels", len(e.channels))
}

// shutdown закрывает открытые сессии просмотра, чтобы время просмотра учлось корректно.
func (e *Engine) shutdown() {
	e.log.Info("engine stopping", "active_slots", len(e.viewing))
	for id, v := range e.viewing {
		e.stopViewing(id, v)
	}
}

// publish отправляет сообщение в live-шину.
func (e *Engine) publish(msg *minerv1.LiveMessage) { e.bus.Publish(msg) }

// dbctx — контекст для локальных вызовов SQLite: они должны выполняться и во время
// завершения (закрытие сессий), поэтому игнорируют контекст движка.
func dbctx() context.Context { return context.Background() }
