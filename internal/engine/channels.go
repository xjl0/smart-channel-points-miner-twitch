package engine

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	minerv1 "github.com/xjl0/smart-channel-points-miner-twitch/internal/gen/miner/v1"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/jitter"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/store/db"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/twitch"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/twitch/pubsub"
)

// channel — состояние отслеживаемого канала в памяти.
type channel struct {
	db.Channel // сохраняемая часть: id, логин, включён ли, баллы, длина стрика

	online          bool
	stream          *twitch.Stream // текущая трансляция, nil когда офлайн
	streakClaimed   bool           // получен или считается полученным, для текущей трансляции
	streakKnown     bool           // RewardList проверен для текущей трансляции
	watched         int64          // наши секунды просмотра за текущую трансляцию
	lastStreakCheck time.Time
	refreshing      bool   // выполняется запрос StreamInfo
	lastClaimID     string // последний сундук, по которому мы кликнули (дедупликация PubSub + проверки контекста)
	takenClaimID    string // последний сундук, о котором сообщили как о забранном (claim-claimed)
	missing         bool   // Twitch больше не знает этот логин (бан или переименование)
	pointsBonus     float64
	lastMomentID    string // дедупликация получения моментов
	streakBase      int64  // секунды просмотра, не идущие в отсчёт стрика (до ручного повтора и без PubSub)
	streakPaused    bool   // отсчёт стрика стоит: PubSub не подключён

	streakCooldownUntil time.Time // после потолка попытки канал до этого времени пропускает других
	streakRetry         bool      // ожидается ручной повтор (первый в очереди)
	viewers             int
	streakBonus         []int32
	chatModes           *minerv1.ChatModes // nil, пока мы не в чате
}

// pendingPrefix помечает канал, добавленный, когда Twitch не знал его логин
// (бан или переименование). Его настоящий id неизвестен, поэтому он хранится под
// "pending:<login>" и заменяется настоящим id, как только логин разрешится.
const pendingPrefix = "pending:"

// ref идентифицирует канал для поиска стрима (пока канал в ожидании, id нет).
func (ch *channel) ref() twitch.ChannelRef {
	if ch.pending() {
		return twitch.ChannelRef{Login: ch.Login}
	}
	return twitch.ChannelRef{ID: ch.ID, Login: ch.Login}
}

// pending сообщает, ждёт ли канал всё ещё свой настоящий Twitch id.
func (ch *channel) pending() bool { return strings.HasPrefix(ch.ID, pendingPrefix) }

// logger возвращает логгер с меткой канала (channel_id связывает записи
// лога с каналом в истории админки).
func (e *Engine) logger(ch *channel) *slog.Logger {
	return e.log.With("channel", ch.Login, "channel_id", ch.ID)
}

// loadChannels читает отслеживаемые каналы из базы при запуске.
func (e *Engine) loadChannels() error {
	rows, err := e.store.ListChannels(dbctx())
	if err != nil {
		return fmt.Errorf("load channels: %w", err)
	}
	// Иначе сессии, оставшиеся открытыми после падения, никогда бы не завершились.
	if err := e.store.EndAllWatchSessions(dbctx(), ptr(time.Now().UTC())); err != nil {
		return fmt.Errorf("close stale sessions: %w", err)
	}
	for _, r := range rows {
		ch := &channel{Channel: r}
		e.channels[r.ID] = ch
		e.track(ch)
	}
	if err := e.restoreBroadcasts(); err != nil {
		return err
	}
	for _, ch := range e.channels {
		e.publishChannel(ch)
	}
	return nil
}

// restoreBroadcasts восстанавливает трансляции, которые шли в момент остановки
// майнера, чтобы после перезапуска все каналы не мигали на минуту как офлайн.
// Затем стартовая проверка онлайна подтверждает или исправляет каждую из них.
func (e *Engine) restoreBroadcasts() error {
	open, err := e.store.OpenBroadcasts(dbctx())
	if err != nil {
		return fmt.Errorf("load open broadcasts: %w", err)
	}
	for _, b := range open {
		ch := e.channels[b.ChannelID]
		if ch == nil {
			continue
		}
		watched, err := e.store.BroadcastWatchSeconds(dbctx(), b.ID)
		if err != nil {
			return fmt.Errorf("load watch time: %w", err)
		}
		ch.online = true
		ch.stream = &twitch.Stream{BroadcastID: b.ID, StartedAt: b.StartedAt}
		ch.streakClaimed = b.StreakClaimedAt != nil
		ch.streakKnown = true // проверен при выходе в эфир; срок истечения хранится в канале
		ch.watched = watched
		ch.streakBase = b.StreakBaseSeconds
	}
	e.log.Info("restored live broadcasts", "count", len(open))
	return nil
}

// channelStates преобразует состояние во входные данные планировщика.
func (e *Engine) channelStates() []ChannelState {
	out := make([]ChannelState, 0, len(e.channels))
	for _, ch := range e.channels {
		s := ChannelState{
			ID:             ch.ID,
			Enabled:        ch.Enabled,
			Online:         ch.online,
			StreakClaimed:  ch.streakClaimed,
			StreakKnown:    ch.streakKnown,
			StreakRetry:    ch.streakRetry,
			StreakCooldown: e.now().Before(ch.streakCooldownUntil),
			WatchedSeconds: ch.watched,
			StreakSeconds:  ch.watched - ch.streakBase,
		}
		if ch.StreakExpiresAt != nil {
			s.StreakExpires = *ch.StreakExpiresAt
		}
		if ch.stream != nil {
			s.BroadcastID, s.OnlineSince = ch.stream.BroadcastID, ch.stream.StartedAt
		}
		out = append(out, s)
	}
	return out
}

// publishChannel отправляет полное состояние канала в админ-панель.
func (e *Engine) publishChannel(ch *channel) {
	p := &minerv1.Channel{
		Id:             ch.ID,
		Login:          ch.Login,
		DisplayName:    ch.DisplayName,
		Enabled:        ch.Enabled,
		Online:         ch.online,
		StreakClaimed:  ch.streakClaimed,
		StreakLength:   int32(ch.StreakLength),
		WatchedSeconds: ch.watched,
		Points:         ch.Points,
		Missing:        ch.missing,
		PointsBonus:    ch.pointsBonus,
		Viewers:        int32(ch.viewers),
		ChatModes:      ch.chatModes,
		StreakBonus:    ch.streakBonus,
		StreakRetry:    ch.streakRetry,
	}
	if ch.StreakExpiresAt != nil {
		p.StreakExpiresAt = timestamppb.New(*ch.StreakExpiresAt)
	}
	if s := ch.stream; s != nil {
		p.BroadcastId, p.Title, p.Game = s.BroadcastID, s.Title, s.GameName
		p.OnlineSince = timestamppb.New(s.StartedAt)
	}
	e.publish(&minerv1.LiveMessage{Body: &minerv1.LiveMessage_ChannelUpdated{ChannelUpdated: &minerv1.ChannelUpdated{Channel: p}}})
}

// refreshAll проверяет каждый включённый канал пакетными запросами GQL (один
// HTTP-запрос на 20 каналов, как делает веб-клиент Twitch).
func (e *Engine) refreshAll(why string) {
	if !e.authed {
		return
	}
	logins := map[string]string{} // логин → id канала
	var list []twitch.ChannelRef
	for _, ch := range e.channels {
		if ch.Enabled && !ch.refreshing {
			ch.refreshing = true
			logins[ch.Login] = ch.ID
			list = append(list, ch.ref())
		}
	}
	if len(list) == 0 {
		return
	}
	e.async(func(ctx context.Context) func() {
		results := e.tw.StreamInfos(ctx, list)
		return func() {
			live, failed := 0, 0
			var firstErr error
			for _, r := range results {
				e.applyStream(logins[r.Login], r)
				switch {
				case r.Err != nil && !errors.Is(r.Err, twitch.ErrChannelNotFound):
					failed++
					firstErr = cmp.Or(firstErr, r.Err)
				case r.Stream != nil:
					live++
				}
			}
			if failed > 0 {
				e.log.Warn("online check failed", "reason", why, "failed", failed, "of", len(results), "err", firstErr)
			}
			e.log.Debug("online check done", "reason", why, "channels", len(results), "live", live)
			e.reconcile()
		}
	})
}

// refresh проверяет один канал. С retries он продолжает перепроверять (после каждой
// задержки), пока Twitch всё ещё сообщает, что канал офлайн, — используется после
// PubSub stream-up, когда информация о стриме в GQL может немного отставать.
func (e *Engine) refresh(id string, retries ...time.Duration) {
	ch := e.channels[id]
	if ch == nil || ch.refreshing || !e.authed {
		return
	}
	ch.refreshing = true
	ref := ch.ref()
	delays := append([]time.Duration{0}, retries...)
	e.async(func(ctx context.Context) func() {
		var r twitch.StreamResult
		for _, d := range delays {
			if !jitter.Sleep(ctx, d) {
				return nil
			}
			if r = e.tw.StreamInfo(ctx, ref); r.Stream != nil || r.Err != nil {
				break
			}
		}
		return func() {
			if r.Err != nil && !errors.Is(r.Err, twitch.ErrChannelNotFound) && !errors.Is(r.Err, twitch.ErrUnauthorized) {
				if ch := e.channels[id]; ch != nil {
					e.logger(ch).Warn("stream info failed", "err", r.Err)
				}
			}
			e.applyStream(id, r)
			e.reconcile()
		}
	})
}

// applyStream записывает результат запроса StreamInfo. Он не запускает
// планировщик: вызывающие делают reconcile один раз после применения всех результатов,
// чтобы пакет оценивался целиком (порядок FIFO остаётся верным).
func (e *Engine) applyStream(id string, r twitch.StreamResult) {
	ch := e.channels[id]
	if ch == nil {
		return // тем временем удалён
	}
	ch.refreshing = false
	s, err := r.Stream, r.Err
	if r.DisplayName != "" && r.DisplayName != ch.DisplayName && !ch.pending() {
		if err := e.store.SetChannelDisplayName(dbctx(), db.SetChannelDisplayNameParams{DisplayName: r.DisplayName, ID: ch.ID}); err != nil {
			e.logger(ch).Error("save display name", "err", err)
		}
		ch.DisplayName = r.DisplayName
		e.publishChannel(ch)
	}
	if ch.pending() && err == nil {
		e.resolvePending(ch) // логин снова существует: получаем настоящий id
		return
	}
	if e.setMissing(ch, errors.Is(err, twitch.ErrChannelNotFound)) {
		if ch.online {
			e.goOffline(ch, "channel not found")
		}
		return
	}
	switch {
	case errors.Is(err, twitch.ErrUnauthorized):
		e.revalidate()
	case err != nil:
		// логирует вызывающий (один раз на пакет, а не на каждый канал)
	case s == nil && ch.online:
		e.goOffline(ch, "stream info")
	case s == nil:
		// всё ещё офлайн, ничего не изменилось
	case !ch.online || ch.stream.BroadcastID != s.BroadcastID:
		e.goOnline(ch, s)
	default:
		e.updateStream(ch, s) // та же трансляция; название/игра могли измениться
	}
}

// setMissing отслеживает, знает ли Twitch ещё этот канал, и сообщает
// новое значение. Пропавший канал так и отображается и никогда не смотрится; он
// возвращается сам, если бан снимут.
func (e *Engine) setMissing(ch *channel, missing bool) bool {
	if ch.missing == missing {
		return missing
	}
	ch.missing = missing
	if missing {
		e.logger(ch).Warn("channel not found on twitch (banned or renamed)")
	} else {
		e.logger(ch).Info("channel is available on twitch again")
	}
	e.publishChannel(ch)
	return missing
}

// updateStream обновляет название/игру текущей трансляции; идущий
// просмотр подхватит смену игры в следующем событии minute-watched.
func (e *Engine) updateStream(ch *channel, s *twitch.Stream) {
	old := ch.stream.GameName
	// Храним самое раннее известное начало. Восстановленное или впервые увиденное время
	// позже реального; исправляем его (и в БД), когда Twitch его сообщит.
	if !s.StartedAt.IsZero() && (ch.stream.StartedAt.IsZero() || s.StartedAt.Before(ch.stream.StartedAt)) {
		if _, err := e.store.UpsertBroadcast(dbctx(), db.UpsertBroadcastParams{ID: s.BroadcastID, ChannelID: ch.ID, StartedAt: s.StartedAt.UTC()}); err != nil {
			e.logger(ch).Error("save broadcast start", "err", err)
		}
		e.logger(ch).Debug("broadcast start time corrected", "started", s.StartedAt)
	} else {
		s.StartedAt = ch.stream.StartedAt
	}
	ch.stream = s
	if old != s.GameName {
		if old != "" { // пусто = восстановлено из БД после перезапуска, а не реальное изменение
			e.logger(ch).Info("game changed", "from", old, "to", s.GameName)
		}
		if v := e.viewing[ch.ID]; v != nil {
			v.beacon.Store(beaconFor(ch))
		}
	}
	e.publishChannel(ch)
}

// goOnline загружает (или создаёт) строку трансляции, восстанавливая состояние стрика и
// время просмотра после перезапусков.
func (e *Engine) goOnline(ch *channel, s *twitch.Stream) {
	log := e.logger(ch)
	if ch.stream != nil && ch.stream.BroadcastID != s.BroadcastID {
		if err := e.store.EndBroadcast(dbctx(), db.EndBroadcastParams{EndedAt: ptr(time.Now().UTC()), ID: ch.stream.BroadcastID}); err != nil {
			log.Error("end previous broadcast", "err", err)
		}
	}
	started := s.StartedAt
	if started.IsZero() {
		started = time.Now().UTC() // время начала недоступно: время первого обнаружения, исправится позже
	}
	b, err := e.store.UpsertBroadcast(dbctx(), db.UpsertBroadcastParams{ID: s.BroadcastID, ChannelID: ch.ID, StartedAt: started.UTC()})
	if err != nil {
		log.Error("save broadcast", "err", err)
		return
	}
	s.StartedAt = b.StartedAt // самое раннее известное начало (проверки стрика сравнивают с ним)
	watched, err := e.store.BroadcastWatchSeconds(dbctx(), s.BroadcastID)
	if err != nil {
		log.Error("load watch time", "err", err)
	}

	ch.online, ch.stream = true, s
	ch.streakClaimed = b.StreakClaimedAt != nil
	ch.streakKnown = ch.streakClaimed // иначе ждём проверки RewardList ниже
	ch.streakBase, ch.streakRetry = b.StreakBaseSeconds, false
	ch.viewers = s.Viewers
	ch.watched = watched
	ch.lastStreakCheck = time.Time{}
	log.Info("channel is live", "broadcast", s.BroadcastID, "game", s.GameName, "title", s.Title,
		"streak_claimed", ch.streakClaimed, "watched", time.Duration(watched)*time.Second)
	e.publishChannel(ch)
	if !ch.streakKnown {
		e.checkStreak(ch)
	}
}

// goOffline закрывает трансляцию и сбрасывает состояние, относящееся к трансляции.
func (e *Engine) goOffline(ch *channel, why string) {
	if err := e.store.EndOpenBroadcasts(dbctx(), db.EndOpenBroadcastsParams{EndedAt: ptr(time.Now().UTC()), ChannelID: ch.ID}); err != nil {
		e.logger(ch).Error("end broadcast", "err", err)
	}
	ch.online, ch.stream = false, nil
	ch.streakClaimed, ch.streakKnown, ch.watched = false, false, 0
	ch.streakBase, ch.streakRetry, ch.viewers = 0, false, 0
	ch.streakCooldownUntil = time.Time{}
	e.logger(ch).Info("channel went offline", "source", why)
	e.publishChannel(ch)
}

// AddChannel начинает отслеживать канал по логину.
func (e *Engine) AddChannel(ctx context.Context, login string) (*minerv1.Channel, error) {
	login = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(login, "@")))
	if login == "" {
		return nil, errors.New("empty login")
	}
	id, err := e.tw.ChannelID(ctx, login) // сетевой I/O вне цикла
	switch {
	case errors.Is(err, twitch.ErrUnauthorized):
		return nil, ErrNotLoggedIn
	case errors.Is(err, twitch.ErrChannelNotFound):
		id = pendingPrefix + login // бан или переименование: отслеживаем, пока не вернётся
	case err != nil:
		return nil, err
	}

	var out *minerv1.Channel
	err = e.call(func() error {
		for _, c := range e.channels {
			if c.ID == id || c.Login == login {
				return ErrExists
			}
		}
		row, err := e.store.InsertChannel(dbctx(), db.InsertChannelParams{ID: id, Login: login, DisplayName: login, AddedAt: time.Now().UTC()})
		if err != nil {
			return err
		}
		ch := &channel{Channel: row}
		e.channels[id] = ch
		e.track(ch)
		e.logger(ch).Info("channel added", "pending", ch.pending())
		e.publishChannel(ch)
		e.refresh(id)
		out = &minerv1.Channel{Id: id, Login: login, DisplayName: login, Enabled: true, Missing: ch.missing}
		return nil
	})
	return out, err
}

// track подписывается на live-события канала; у канала в ожидании нет id
// для подписки, поэтому он только помечается пропавшим и находится опросом.
func (e *Engine) track(ch *channel) {
	if ch.pending() {
		ch.missing = true
		return
	}
	e.ps.Listen(pubsub.PlaybackTopic(ch.ID))
}

// resolvePending ищет настоящий id канала в ожидании и, как только Twitch
// снова знает логин, пересоздаёт его под этим id (переносить нечего:
// канал в ожидании ни разу не смотрели).
func (e *Engine) resolvePending(ch *channel) {
	oldID, login, enabled := ch.ID, ch.Login, ch.Enabled
	e.async(func(ctx context.Context) func() {
		id, err := e.tw.ChannelID(ctx, login)
		return func() {
			if err != nil || e.channels[oldID] == nil {
				return // всё ещё неизвестен (или тем временем удалён); следующий опрос повторит
			}
			if err := e.store.DeleteChannel(dbctx(), oldID); err != nil {
				e.logger(ch).Error("drop pending channel", "err", err)
				return
			}
			row, err := e.store.InsertChannel(dbctx(), db.InsertChannelParams{ID: id, Login: login, DisplayName: login, AddedAt: ch.AddedAt})
			if err != nil {
				e.logger(ch).Error("save resolved channel", "err", err)
				return
			}
			if !enabled {
				_ = e.store.SetChannelEnabled(dbctx(), db.SetChannelEnabledParams{Enabled: false, ID: id})
				row.Enabled = false
			}
			delete(e.channels, oldID)
			e.publish(&minerv1.LiveMessage{Body: &minerv1.LiveMessage_ChannelRemoved{ChannelRemoved: &minerv1.ChannelRemoved{ChannelId: oldID}}})
			resolved := &channel{Channel: row}
			e.channels[id] = resolved
			e.track(resolved)
			e.logger(resolved).Info("channel is available on twitch again")
			e.publishChannel(resolved)
			e.refresh(id)
		}
	})
}

// RemoveChannel прекращает отслеживать канал и удаляет его историю.
func (e *Engine) RemoveChannel(id string) error {
	return e.call(func() error {
		ch := e.channels[id]
		if ch == nil {
			return ErrNotFound
		}
		if v := e.viewing[id]; v != nil {
			e.stopViewing(id, v)
		}
		if err := e.store.DeleteChannel(dbctx(), id); err != nil {
			return err
		}
		delete(e.channels, id)
		e.ps.Unlisten(pubsub.PlaybackTopic(id))
		e.logger(ch).Info("channel removed")
		e.publish(&minerv1.LiveMessage{Body: &minerv1.LiveMessage_ChannelRemoved{ChannelRemoved: &minerv1.ChannelRemoved{ChannelId: id}}})
		e.reconcile()
		return nil
	})
}

// RetryStreak забывает стрик текущей трансляции канала и ставит
// его первым в очередь стриков с новым таймером «считать полученным». Для стриков,
// которые считались полученными, но на деле не были получены. Если RewardList покажет,
// что стрик всё же получен за эту трансляцию, он просто записывается заново.
func (e *Engine) RetryStreak(id string) error {
	return e.call(func() error {
		ch := e.channels[id]
		if ch == nil {
			return ErrNotFound
		}
		if !ch.online || ch.stream == nil {
			return ErrOffline
		}
		if err := e.store.RetryBroadcastStreak(dbctx(), db.RetryBroadcastStreakParams{StreakBaseSeconds: ch.watched, ID: ch.stream.BroadcastID}); err != nil {
			return err
		}
		ch.streakClaimed, ch.streakKnown = false, true
		ch.streakBase, ch.streakRetry = ch.watched, true
		ch.lastStreakCheck = time.Time{}
		e.logger(ch).Info("streak retry requested", "watched_before", time.Duration(ch.watched)*time.Second)
		e.publishChannel(ch)
		e.reconcile()
		return nil
	})
}

// SetChannelEnabled приостанавливает или возобновляет канал, не теряя его историю.
func (e *Engine) SetChannelEnabled(id string, enabled bool) error {
	return e.call(func() error {
		ch := e.channels[id]
		if ch == nil {
			return ErrNotFound
		}
		if err := e.store.SetChannelEnabled(dbctx(), db.SetChannelEnabledParams{Enabled: enabled, ID: id}); err != nil {
			return err
		}
		ch.Enabled = enabled
		e.logger(ch).Info("channel enabled changed", "enabled", enabled)
		e.publishChannel(ch)
		if enabled {
			e.refresh(id)
		}
		e.reconcile()
		return nil
	})
}

func ptr[T any](v T) *T { return &v }

// isNoRows сообщает об отсутствии строки.
func isNoRows(err error) bool { return errors.Is(err, sql.ErrNoRows) }
