package engine

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	minerv1 "github.com/xjl0/smart-channel-points-miner-twitch/internal/gen/miner/v1"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/jitter"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/store/db"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/twitch"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/twitch/pubsub"
)

// viewing — работающий слот: цикл minute-watched плюс присутствие в чате.
type viewing struct {
	slot      Slot
	login     string                               // логин канала, хранится, чтобы всегда можно было выйти из его чата
	beacon    atomic.Pointer[twitch.MinuteWatched] // что отправляет watchLoop; подменяется при смене игры
	sessionID int64                                // строка watch_sessions, куда идут секунды
	failures  int                                  // ошибки minute-watched подряд
	cancel    context.CancelFunc
}

// applySlots запускает/останавливает просмотры, чтобы они совпадали с желаемыми слотами.
// Всё, что больше не нужно, останавливается первым, поэтому никогда, даже на миг,
// не бывает больше MaxSlots просмотров (и чатов).
func (e *Engine) applySlots(next []Slot) {
	for id, v := range e.viewing {
		if !hasSlot(next, id) {
			e.stopViewing(id, v)
		}
	}
	applied := make([]Slot, 0, len(next))
	for _, s := range next {
		v := e.viewing[s.ChannelID]
		switch {
		case v != nil && v.slot.BroadcastID != s.BroadcastID:
			// Стример перезапустил стрим: события minute-watched и время просмотра должны
			// идти в новую трансляцию, поэтому перезапускаем просмотр с нуля.
			e.stopViewing(s.ChannelID, v)
			if !e.startViewing(s) {
				continue
			}
		case v == nil:
			if !e.startViewing(s) {
				continue
			}
		case v.slot.Reason != s.Reason:
			// например, досмотр стрика окончен и канал остаётся как fill:
			// продолжаем смотреть и сидеть в чате, но время учитываем в новой сессии.
			e.endSession(v)
			v.sessionID = e.startSession(s)
			v.slot = s
			e.logger(e.channels[s.ChannelID]).Info("slot reason changed", "reason", s.Reason)
		default:
			v.slot = s // обновление дедлайна / флага досмотра
		}
		applied = append(applied, s)
	}
	e.checkInvariant()

	if slotsEqual(e.slots, applied) {
		return
	}
	e.slots = applied
	e.publishSlots()
}

// publishSlots отправляет в админку текущие слоты (вместе со счётчиком
// неудачных minute-watched, чтобы проблема была видна сразу).
func (e *Engine) publishSlots() {
	msg := &minerv1.SlotsChanged{}
	for _, s := range e.slots {
		p := &minerv1.Slot{
			ChannelId: s.ChannelID,
			Reason:    reasonProto(s.Reason),
			StartedAt: timestamppb.New(s.StartedAt),
			Deadline:  timestamppb.New(s.Deadline),
			Lingering: s.Lingering,
		}
		if v := e.viewing[s.ChannelID]; v != nil {
			p.BeaconFailures = int32(v.failures)
		}
		msg.Slots = append(msg.Slots, p)
	}
	e.publish(&minerv1.LiveMessage{Body: &minerv1.LiveMessage_SlotsChanged{SlotsChanged: msg}})
}

// startViewing начинает просмотр канала: строка сессии, цикл minute-watched, JOIN
// в чат и проверка баллов канала (баланс + ожидающий сундук).
// Возвращает false, если канал нельзя смотреть (пропал или офлайн).
func (e *Engine) startViewing(s Slot) bool {
	ch := e.channels[s.ChannelID]
	if ch == nil || ch.stream == nil {
		return false
	}
	ctx, cancel := context.WithCancel(e.ctx)
	v := &viewing{slot: s, login: ch.Login, sessionID: e.startSession(s), cancel: cancel}
	v.beacon.Store(beaconFor(ch))
	e.viewing[s.ChannelID] = v

	go e.watchLoop(ctx, v, e.timing.FirstBeacon, e.timing.BeaconEvery) // задержки — копией: горутина не читает e.timing
	e.chat.Join(ch.Login)
	for _, t := range pubsub.WatchTopics(ch.ID) { // то, что слушает веб-плеер во время просмотра
		e.ps.Listen(t)
	}
	e.checkPoints(ch)

	e.logger(ch).Info("started watching", "reason", s.Reason, "until", s.Deadline.Format(time.TimeOnly))
	return true
}

// stopViewing полностью завершает просмотр: цикл minute-watched, чат, топики PubSub
// и сессию просмотра. Не зависит от того, существует ли ещё канал.
func (e *Engine) stopViewing(id string, v *viewing) {
	v.cancel() // также прерывает выполняющийся запрос minute-watched
	e.endSession(v)
	delete(e.viewing, id)
	e.chat.Part(v.login)
	for _, t := range pubsub.WatchTopics(id) {
		e.ps.Unlisten(t)
	}
	var watched time.Duration
	if ch := e.channels[id]; ch != nil {
		watched = time.Duration(ch.watched) * time.Second
	}
	e.log.Info("stopped watching", "channel", v.login, "channel_id", id, "reason", v.slot.Reason, "watched_this_broadcast", watched)
}

// checkInvariant проверяет жёсткие ограничения после каждого изменения слотов: не более
// MaxSlots просмотров и чаты только смотрящихся каналов. Если инвариант
// когда-нибудь нарушится, это логируется как ошибка и тут же исправляется.
func (e *Engine) checkInvariant() {
	if len(e.viewing) > e.policy.MaxSlots {
		e.log.Error("invariant broken: too many viewings", "viewing", len(e.viewing), "max", e.policy.MaxSlots)
	}
	watching := make(map[string]bool, len(e.viewing))
	for _, v := range e.viewing {
		watching[v.login] = true
	}
	for _, login := range e.chat.Joined() {
		if !watching[login] {
			e.log.Error("invariant broken: in a chat we do not watch, leaving", "channel", login)
			e.chat.Part(login)
		}
	}
}

func (e *Engine) startSession(s Slot) int64 {
	id, err := e.store.StartWatchSession(dbctx(), db.StartWatchSessionParams{
		ChannelID: s.ChannelID, BroadcastID: s.BroadcastID, Reason: string(s.Reason), StartedAt: time.Now().UTC(),
	})
	if err != nil {
		e.log.Error("start watch session", "channel_id", s.ChannelID, "err", err)
	}
	return id
}

func (e *Engine) endSession(v *viewing) {
	if err := e.store.EndWatchSession(dbctx(), db.EndWatchSessionParams{EndedAt: ptr(time.Now().UTC()), ID: v.sessionID}); err != nil {
		e.log.Error("end watch session", "session", v.sessionID, "err", err)
	}
}

// beaconFor собирает payload minute-watched для трансляции канала.
func beaconFor(ch *channel) *twitch.MinuteWatched {
	return &twitch.MinuteWatched{
		ChannelID: ch.ID, ChannelLogin: ch.Login, BroadcastID: ch.stream.BroadcastID,
		GameID: ch.stream.GameID, GameName: ch.stream.GameName,
	}
}

// watchLoop отправляет событие minute-watched примерно раз в минуту, пока ctx
// не завершится. Первое событие задерживается на несколько секунд, как при загрузке плеера.
// Payload каждый раз читается из просмотра, поэтому смена игры применяется.
func (e *Engine) watchLoop(ctx context.Context, v *viewing, first [2]time.Duration, every time.Duration) {
	delay := jitter.Between(first[0], first[1])
	for jitter.Sleep(ctx, delay) {
		err := e.tw.SendMinuteWatched(ctx, *v.beacon.Load())
		if ctx.Err() != nil {
			return
		}
		e.do(func() { e.onWatched(v, err) })
		delay = jitter.Around(every, every/15)
	}
}

// onWatched учитывает результат одного события minute-watched просмотра v.
func (e *Engine) onWatched(v *viewing, err error) {
	channelID := v.slot.ChannelID
	ch := e.channels[channelID]
	if e.viewing[channelID] != v || ch == nil {
		return // тот просмотр завершён (возможно, заменён новым): не нам его учитывать
	}
	if err != nil {
		v.failures++
		e.publishSlots() // админка показывает «минуты не проходят»
		level := slog.LevelDebug
		if v.failures == 1 || v.failures%10 == 0 {
			level = slog.LevelWarn // первая ошибка и каждая 10-я подряд: видно, что проблема не прошла
		}
		e.logger(ch).Log(e.ctx, level, "minute-watched failed", "err", err, "failures_in_a_row", v.failures)
		if v.failures%maxWatchFailures == 0 {
			e.refresh(channelID) // возможно, стрим закончился
		}
		return
	}
	if v.failures > 0 {
		e.logger(ch).Info("minute-watched recovered", "after_failures", v.failures)
		v.failures = 0
		e.publishSlots()
	}
	const minute = 60
	if err := e.store.AddWatchSeconds(dbctx(), db.AddWatchSecondsParams{Seconds: minute, ID: v.sessionID}); err != nil {
		e.logger(ch).Error("save watch time", "err", err)
	}
	ch.watched += minute
	e.creditStreakMinute(ch, minute)
	e.logger(ch).Debug("minute watched", "total", time.Duration(ch.watched)*time.Second)
	e.publish(&minerv1.LiveMessage{Body: &minerv1.LiveMessage_WatchTick{WatchTick: &minerv1.WatchTick{
		ChannelId: channelID, WatchedSeconds: ch.watched,
	}}})
}

// creditStreakMinute решает, идёт ли минута в отсчёт «N минут без стрика».
// Без живого PubSub Twitch просмотр не засчитывает, поэтому такая минута
// добавляется в streakBase (сохраняется в БД) и отсчёт стоит, пока связь не
// вернётся. Общее время просмотра (для выбора fill) считается всегда.
func (e *Engine) creditStreakMinute(ch *channel, seconds int64) {
	if e.presence() {
		if ch.streakPaused {
			ch.streakPaused = false
			e.logger(ch).Info("streak timer resumed: pubsub is back")
		}
		return
	}
	ch.streakBase += seconds
	if ch.stream != nil {
		if err := e.store.AddStreakBase(dbctx(), db.AddStreakBaseParams{StreakBaseSeconds: seconds, ID: ch.stream.BroadcastID}); err != nil {
			e.logger(ch).Error("save uncredited minute", "err", err)
		}
	}
	if !ch.streakPaused {
		ch.streakPaused = true
		e.logger(ch).Warn("streak timer paused: pubsub not connected, twitch does not credit watching")
	}
}

// scheduleStreakChecks перепроверяет RewardList каждые несколько минут во время активных
// попыток получить стрик — запасной вариант на случай пропущенного события PubSub WATCH_STREAK.
func (e *Engine) scheduleStreakChecks() {
	for id, v := range e.viewing {
		ch := e.channels[id]
		if v.slot.Reason == ReasonStreak && ch != nil && !ch.streakClaimed && ch.stream != nil &&
			e.now().Sub(ch.lastStreakCheck) >= e.timing.StreakCheckEvery {
			e.checkStreak(ch)
		}
	}
}

// checkStreak запрашивает у RewardList стрик канала: сохраняет его длину и
// срок истечения и записывает его, если он уже был получен за эту трансляцию.
// Первая проверка за трансляцию также пускает канал в очередь стриков
// (streakKnown), чтобы за стриком, полученным в другом месте, больше не гнаться.
func (e *Engine) checkStreak(ch *channel) {
	if ch.stream == nil {
		return
	}
	ch.lastStreakCheck = e.now()
	chID, broadcast, started := ch.ID, ch.stream.BroadcastID, ch.stream.StartedAt
	e.async(func(ctx context.Context) func() {
		ws, err := e.tw.WatchStreak(ctx, chID)
		return func() {
			ch := e.channels[chID]
			if ch == nil || ch.stream == nil || ch.stream.BroadcastID != broadcast {
				return
			}
			first := !ch.streakKnown
			ch.streakKnown = true // даже при ошибке: никогда не блокируем очередь из-за RewardList
			switch {
			case err != nil:
				e.logger(ch).Warn("watch streak check failed", "err", err)
			case ws.ClaimedFor(started):
				e.saveStreak(ch, ws)
				e.markStreakClaimed(ch, ws.Length, "reward list", "achieved_at", ws.AchievedAt, "stream_started", started)
				return
			default:
				e.saveStreak(ch, ws)
				e.logger(ch).Debug("watch streak checked", "length", ws.Length, "expires", ws.ExpiresAt)
			}
			e.publishChannel(ch)
			if first {
				e.reconcile()
			}
		}
	})
}

// saveStreak сохраняет длину и срок истечения стрика, если они изменились.
func (e *Engine) saveStreak(ch *channel, ws twitch.WatchStreak) {
	var expires *time.Time
	if !ws.ExpiresAt.IsZero() {
		expires = &ws.ExpiresAt
	}
	same := int64(ws.Length) == ch.StreakLength &&
		((expires == nil && ch.StreakExpiresAt == nil) || (expires != nil && ch.StreakExpiresAt != nil && expires.Equal(*ch.StreakExpiresAt)))
	if same {
		return
	}
	err := e.store.SetChannelStreak(dbctx(), db.SetChannelStreakParams{StreakLength: int64(ws.Length), StreakExpiresAt: expires, ID: ch.ID})
	if err != nil {
		e.logger(ch).Error("save streak", "err", err)
	}
	ch.StreakLength, ch.StreakExpiresAt = int64(ws.Length), expires
}

// markStreakClaimed записывает полученный стрик и перезапускает планировщик.
// attrs — дополнительные поля лога (например, когда Twitch его выдал).
func (e *Engine) markStreakClaimed(ch *channel, length int, source string, attrs ...any) {
	if e.recordStreak(ch, length, false, source, attrs...) {
		e.reconcile()
	}
}

// recordStreak сохраняет стрик для текущей трансляции (один раз). assumed
// означает, что он не пришёл, но мы смотрели достаточно долго, чтобы считать его полученным.
// Сообщает, изменилось ли что-нибудь. Не вызывает reconcile (безопасно внутри reconcile).
func (e *Engine) recordStreak(ch *channel, length int, assumed bool, source string, attrs ...any) bool {
	if ch.streakClaimed || ch.stream == nil {
		return false
	}
	log := e.logger(ch)
	if err := e.store.MarkStreakClaimed(dbctx(), db.MarkStreakClaimedParams{StreakClaimedAt: ptr(time.Now().UTC()), ID: ch.stream.BroadcastID}); err != nil {
		log.Error("save streak", "err", err)
	}
	ch.streakClaimed, ch.streakRetry = true, false
	if length > 0 {
		e.saveStreak(ch, twitch.WatchStreak{Length: length, ExpiresAt: deref(ch.StreakExpiresAt)})
	}
	msg := "watch streak received"
	if assumed {
		msg = "watch streak assumed"
	}
	log.Info(msg, append([]any{"length", ch.StreakLength, "source", source,
		"watched", time.Duration(ch.watched) * time.Second}, attrs...)...)
	e.publish(&minerv1.LiveMessage{Body: &minerv1.LiveMessage_StreakResult{StreakResult: &minerv1.StreakResult{
		ChannelId: ch.ID, StreakLength: int32(ch.StreakLength), Assumed: assumed,
	}}})
	e.publishChannel(ch)
	return true
}

// checkPoints загружает баланс и кликает по ожидающему сундуку.
func (e *Engine) checkPoints(ch *channel) {
	id, login := ch.ID, ch.Login
	e.async(func(ctx context.Context) func() {
		p, err := e.tw.ChannelPoints(ctx, login)
		return func() {
			ch := e.channels[id]
			if ch == nil {
				return
			}
			if err != nil {
				e.logger(ch).Warn("channel points check failed", "err", err)
				return
			}
			e.setPoints(ch, int64(p.Balance))
			ch.pointsBonus = p.Bonus
			ch.streakBonus = nil // новый срез: опубликованные сообщения никогда не должны его разделять
			for _, b := range p.StreakBonus {
				ch.streakBonus = append(ch.streakBonus, int32(b))
			}
			e.publishChannel(ch)
			if p.ClaimID != "" {
				e.claimBonus(ch, p.ClaimID)
			}
		}
	})
}

func (e *Engine) setPoints(ch *channel, balance int64) {
	if ch.Points == balance {
		return
	}
	if err := e.store.SetChannelPoints(dbctx(), db.SetChannelPointsParams{Points: balance, ID: ch.ID}); err != nil {
		e.logger(ch).Error("save points", "err", err)
	}
	ch.Points = balance
	e.publishChannel(ch)
}

// claimBonus кликает по сундуку после «человеческой» задержки реакции, если его
// тем временем не забрали (PubSub claim-claimed, например другим клиентом
// аккаунта). Баллы приходят событием PubSub points-earned (reason CLAIM).
//
// Только на каналах в наших слотах: события о сундуках приходят по всему
// аккаунту, и сундук канала, который пользователь смотрит сам (в браузере,
// на телефоне), остаётся ему.
func (e *Engine) claimBonus(ch *channel, claimID string) {
	if claimID == ch.lastClaimID {
		return
	}
	if e.viewing[ch.ID] == nil {
		e.logger(ch).Debug("bonus chest on a channel we do not watch (watched elsewhere), leaving it", "claim_id", claimID)
		return
	}
	ch.lastClaimID = claimID
	id := ch.ID
	e.logger(ch).Info("bonus chest available", "claim_id", claimID)
	chestDelay := jitter.Between(e.timing.ChestDelay[0], e.timing.ChestDelay[1]) // на цикле движка: e.timing меняет админка
	e.async(func(ctx context.Context) func() {
		if !jitter.Sleep(ctx, chestDelay) {
			return nil
		}
		return func() { e.clickChest(id, claimID) }
	})
}

// clickChest отправляет claim, если сундук ещё на месте.
func (e *Engine) clickChest(id, claimID string) {
	ch := e.channels[id]
	if ch == nil {
		return
	}
	if ch.takenClaimID == claimID {
		e.logger(ch).Info("bonus chest already taken by another client, skipping")
		return
	}
	if e.viewing[id] == nil { // пока ждали «реакцию», канал ушёл из слота
		e.logger(ch).Debug("bonus chest left: the channel is no longer in our slots")
		return
	}
	e.async(func(ctx context.Context) func() {
		err := e.tw.ClaimBonus(ctx, id, claimID)
		return func() {
			if ch := e.channels[id]; ch != nil {
				if err != nil {
					e.logger(ch).Warn("bonus chest claim failed", "err", err)
				} else {
					e.logger(ch).Info("bonus chest claimed")
				}
			}
		}
	})
}

// claimMoment получает момент сообщества на канале, который мы смотрим, после
// «человеческой» задержки.
func (e *Engine) claimMoment(ch *channel, momentID string) {
	if e.viewing[ch.ID] == nil || momentID == ch.lastMomentID {
		return // зритель видит моменты только на том стриме, который смотрит
	}
	ch.lastMomentID = momentID
	id := ch.ID
	e.logger(ch).Info("moment available", "moment_id", momentID)
	momentDelay := jitter.Between(e.timing.MomentDelay[0], e.timing.MomentDelay[1]) // на цикле движка
	e.async(func(ctx context.Context) func() {
		if !jitter.Sleep(ctx, momentDelay) {
			return nil
		}
		err := e.tw.ClaimMoment(ctx, momentID)
		return func() {
			if ch := e.channels[id]; ch != nil {
				if err != nil {
					e.logger(ch).Warn("moment claim failed", "err", err)
				} else {
					e.logger(ch).Info("moment claimed")
				}
			}
		}
	})
}

func slotsEqual(a, b []Slot) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ChannelID != b[i].ChannelID || a[i].Reason != b[i].Reason ||
			!a[i].Deadline.Equal(b[i].Deadline) || a[i].Lingering != b[i].Lingering {
			return false
		}
	}
	return true
}

func reasonProto(r Reason) minerv1.SlotReason {
	if r == ReasonStreak {
		return minerv1.SlotReason_SLOT_REASON_STREAK
	}
	return minerv1.SlotReason_SLOT_REASON_FILL
}

func deref[T any](p *T) T {
	var zero T
	if p == nil {
		return zero
	}
	return *p
}
