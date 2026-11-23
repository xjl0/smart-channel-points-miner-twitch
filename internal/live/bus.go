// Package live — шина событий между движком и админ-панелью.
//
// Каждое изменение публикуется как LiveMessage со следующим порядковым номером.
// Затем шина
//   - сворачивает сообщения состояния в собственный Snapshot (серверный редьюсер,
//     та же логика, что выполняет админ-панель), так что согласованный снапшот всегда
//     доступен без обращения к движку;
//   - хранит последние ringSize сообщений, чтобы переподключающиеся клиенты могли продолжить
//     со своего последнего seq без полного снапшота;
//   - сохраняет события ленты (баллы, стрики, смены слотов, предупреждения) в SQLite;
//   - рассылает сообщения подписчикам.
package live

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	minerv1 "github.com/xjl0/smart-channel-points-miner-twitch/internal/gen/miner/v1"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/store"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/store/db"
)

const (
	ringSize   = 1000 // сообщения, хранимые для возобновления
	subBufSize = 512  // очередь на подписчика; отставший клиент отключается и возобновляет поток
)

// Bus безопасен для конкурентного использования.
type Bus struct {
	store *store.Store
	log   *slog.Logger // НЕ должен быть tee-логгером (см. LogHandler), чтобы избежать петель

	mu    sync.Mutex
	seq   uint64
	ring  []*minerv1.LiveMessage // по возрастанию seq, не более ringSize
	state *minerv1.Snapshot
	subs  map[chan *minerv1.LiveMessage]struct{}
}

// New создаёт шину, продолжающую последовательность, сохранённую в базе.
func New(ctx context.Context, st *store.Store, log *slog.Logger) (*Bus, error) {
	last, err := st.LastEventSeq(ctx)
	if err != nil {
		return nil, fmt.Errorf("read last event seq: %w", err)
	}
	return &Bus{
		store: st,
		log:   log.With("component", "live"),
		seq:   uint64(last),
		state: &minerv1.Snapshot{Account: &minerv1.Account{}},
		subs:  map[chan *minerv1.LiveMessage]struct{}{},
	}, nil
}

// Publish присваивает msg (у которого должно быть задано Body) seq и метку времени и
// доставляет его. После этого msg нельзя изменять.
func (b *Bus) Publish(msg *minerv1.LiveMessage) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.seq++
	msg.Seq = b.seq
	msg.Ts = timestamppb.Now()

	store := persisted(msg) && !b.sameLineup(msg) // решается до изменения состояния
	apply(b.state, msg)

	b.ring = append(b.ring, msg)
	if len(b.ring) > ringSize {
		b.ring = b.ring[len(b.ring)-ringSize:]
	}
	if store {
		b.persist(msg)
	}
	for ch := range b.subs {
		select {
		case ch <- msg:
		default:
			// Слишком медленный: отключаем подписчика; он возобновит поток со своего последнего seq.
			delete(b.subs, ch)
			close(ch)
			b.log.Warn("live subscriber dropped: too slow")
		}
	}
}

// Broadcast доставляет эфемерное сообщение (напр. строку чата) только текущим
// подписчикам: оно не получает seq, не сохраняется, не сворачивается в
// снапшот и не хранится для возобновления, поэтому никогда не вытеснит дельты состояния.
func (b *Bus) Broadcast(msg *minerv1.LiveMessage) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.subs) == 0 {
		return // панель никто не смотрит: отбрасываем
	}
	msg.Seq = b.seq
	msg.Ts = timestamppb.Now()
	for ch := range b.subs {
		select {
		case ch <- msg:
		default: // занятый клиент пропускает строку чата, а не отключается
		}
	}
}

// Subscription — live-лента для одного клиента.
type Subscription struct {
	// Backlog нужно отправить раньше всего из C: либо пропущенные дельты,
	// либо один Snapshot.
	Backlog []*minerv1.LiveMessage
	// C доставляет новые сообщения; закрывается, если клиент отстаёт.
	C <-chan *minerv1.LiveMessage

	bus *Bus
	ch  chan *minerv1.LiveMessage
}

// Close останавливает подписку.
func (s *Subscription) Close() {
	s.bus.mu.Lock()
	defer s.bus.mu.Unlock()
	if _, ok := s.bus.subs[s.ch]; ok {
		delete(s.bus.subs, s.ch)
		close(s.ch)
	}
}

// Subscribe запускает ленту. При sinceSeq > 0 шина пытается воспроизвести из кольца только
// пропущенные сообщения; иначе (или если их уже нет) backlog —
// один Snapshot текущего состояния.
func (b *Bus) Subscribe(sinceSeq uint64) *Subscription {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch := make(chan *minerv1.LiveMessage, subBufSize)
	b.subs[ch] = struct{}{}
	sub := &Subscription{C: ch, bus: b, ch: ch}

	if missed, ok := b.missedSince(sinceSeq); ok {
		sub.Backlog = missed
		b.log.Debug("live subscriber resumed", "since", sinceSeq, "replayed", len(missed))
	} else {
		sub.Backlog = []*minerv1.LiveMessage{b.snapshotLocked()}
		b.log.Debug("live subscriber got snapshot", "since", sinceSeq, "seq", b.seq)
	}
	return sub
}

// Seq возвращает порядковый номер последнего опубликованного сообщения.
func (b *Bus) Seq() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.seq
}

// missedSince возвращает сообщения кольца с seq > since, если кольцо ещё покрывает
// этот диапазон. Вызывающий держит b.mu.
func (b *Bus) missedSince(since uint64) ([]*minerv1.LiveMessage, bool) {
	if since == 0 || since > b.seq {
		return nil, false // новый клиент или seq из времени до перезапуска сервера
	}
	if since == b.seq {
		return nil, true // полностью актуален
	}
	if len(b.ring) == 0 || b.ring[0].Seq > since+1 {
		return nil, false // разрыв: самое старое нужное сообщение уже вытеснено
	}
	start := int(since + 1 - b.ring[0].Seq)
	return append([]*minerv1.LiveMessage(nil), b.ring[start:]...), true
}

// snapshotLocked оборачивает глубокую копию свёрнутого состояния. Вызывающий держит b.mu.
func (b *Bus) snapshotLocked() *minerv1.LiveMessage {
	return &minerv1.LiveMessage{
		Seq:  b.seq,
		Ts:   timestamppb.Now(),
		Body: &minerv1.LiveMessage_Snapshot{Snapshot: proto.Clone(b.state).(*minerv1.Snapshot)},
	}
}

// sameLineup сообщает о SlotsChanged, который лишь сдвигает дедлайны (те же каналы
// и причины): live-клиентам он нужен, истории — нет. Вызывающий держит b.mu.
func (b *Bus) sameLineup(msg *minerv1.LiveMessage) bool {
	sc := msg.GetSlotsChanged()
	if sc == nil || len(sc.Slots) != len(b.state.Slots) {
		return false
	}
	for i, s := range sc.Slots {
		if s.ChannelId != b.state.Slots[i].ChannelId || s.Reason != b.state.Slots[i].Reason {
			return false
		}
	}
	return true
}

// persisted сообщает, относится ли сообщение к сохраняемой истории.
// Высокочастотное состояние (тики, обновления каналов, очередь) — только live.
func persisted(msg *minerv1.LiveMessage) bool {
	switch m := msg.Body.(type) {
	case *minerv1.LiveMessage_PointsGained, *minerv1.LiveMessage_StreakResult,
		*minerv1.LiveMessage_SlotsChanged, *minerv1.LiveMessage_ChannelRemoved, *minerv1.LiveMessage_SettingsChanged:
		return true
	case *minerv1.LiveMessage_LogEntry:
		return m.LogEntry.Level == slog.LevelWarn.String() || m.LogEntry.Level == slog.LevelError.String()
	}
	return false
}

// persist записывает одно сообщение в таблицу events. Вызывающий держит b.mu.
func (b *Bus) persist(msg *minerv1.LiveMessage) {
	payload, err := proto.Marshal(msg)
	if err != nil {
		b.log.Error("marshal event", "err", err)
		return
	}
	var channelID *string
	if id := channelOf(msg); id != "" {
		channelID = &id
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = b.store.InsertEvent(ctx, db.InsertEventParams{
		Seq:       int64(msg.Seq),
		Ts:        msg.Ts.AsTime(),
		Type:      typeName(msg),
		ChannelID: channelID,
		Payload:   payload,
	})
	if err != nil {
		b.log.Error("persist event", "seq", msg.Seq, "err", err)
	}
}

// History возвращает сохранённые события старше beforeSeq (0 = самые новые), сначала новые.
func (b *Bus) History(ctx context.Context, beforeSeq uint64, channelID string, limit int) ([]*minerv1.LiveMessage, error) {
	if beforeSeq == 0 {
		beforeSeq = 1 << 62
	}
	rows, err := b.store.ListEvents(ctx, db.ListEventsParams{BeforeSeq: int64(beforeSeq), ChannelID: channelID, Limit: int64(limit)})
	if err != nil {
		return nil, err
	}
	out := make([]*minerv1.LiveMessage, 0, len(rows))
	for _, r := range rows {
		msg := &minerv1.LiveMessage{}
		if err := proto.Unmarshal(r.Payload, msg); err != nil {
			b.log.Warn("skip undecodable event", "seq", r.Seq, "err", err)
			continue
		}
		out = append(out, msg)
	}
	return out, nil
}

// typeName возвращает имя поля oneof, напр. "points_gained".
func typeName(msg *minerv1.LiveMessage) string {
	if f := msg.ProtoReflect().WhichOneof(msg.ProtoReflect().Descriptor().Oneofs().ByName("body")); f != nil {
		return string(f.Name())
	}
	return "unknown"
}

// channelOf извлекает id канала, к которому относится сообщение ("", если такого нет).
func channelOf(msg *minerv1.LiveMessage) string {
	switch m := msg.Body.(type) {
	case *minerv1.LiveMessage_PointsGained:
		return m.PointsGained.ChannelId
	case *minerv1.LiveMessage_StreakResult:
		return m.StreakResult.ChannelId
	case *minerv1.LiveMessage_ChannelRemoved:
		return m.ChannelRemoved.ChannelId
	case *minerv1.LiveMessage_LogEntry:
		return m.LogEntry.Attrs["channel_id"]
	}
	return ""
}
