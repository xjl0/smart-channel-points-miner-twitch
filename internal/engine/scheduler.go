package engine

import (
	"cmp"
	"slices"
	"time"
)

// Этот файл — вся политика «кого смотрим» (PLAN.md §3.2) в виде чистой
// функции: без I/O, без часов, без собственной случайности. Движок передаёт ей
// снимок и применяет результат; тесты передают ей таблицы.

// Reason говорит, почему слот смотрит канал.
type Reason string

const (
	ReasonStreak Reason = "streak" // пытаемся получить стрик просмотра
	ReasonFill   Reason = "fill"   // стрики готовы, просто смотрим наименее просмотренный канал
)

// Policy содержит настраиваемые параметры планировщика.
type Policy struct {
	MaxSlots int // жёсткий предел одновременных стримов (2)
	// StreakAssumeAfter: если столько трансляции просмотрено, а стрик
	// не пришёл, скорее всего, он был получен раньше (например, смотрели в другом месте),
	// поэтому считаем его полученным (15м).
	StreakAssumeAfter time.Duration
	// StreakAttemptMax — потолок попытки по часам. Если минуты не засчитываются
	// (PubSub лежит, minute-watched падают), попытка иначе продлевалась бы
	// бесконечно и держала слот. По потолку канал отпускается без зачёта (30m).
	StreakAttemptMax time.Duration
	StreakLinger     time.Duration // досмотр после получения стрика (~2–4м)
	FillHold         time.Duration // минимальное время, которое слот fill остаётся на одном канале (~30м)
	// Jitter рандомизирует StreakLinger / FillHold. В тестах — тождественная функция.
	Jitter func(time.Duration) time.Duration
}

// ChannelState — то, что планировщику нужно знать об одном канале.
type ChannelState struct {
	ID             string
	Enabled        bool
	Online         bool
	BroadcastID    string
	OnlineSince    time.Time // начало трансляции, задаёт порядок FIFO очереди стриков
	StreakClaimed  bool      // стрик получен (или считается полученным) за текущую трансляцию
	StreakKnown    bool      // состояние стрика проверено для этой трансляции (до этого в очередь не ставится)
	StreakExpires  time.Time // когда стрик будет потерян; ноль, если неизвестно / стрика нет
	StreakRetry    bool      // запрошен ручной повтор: первый в очереди стриков
	StreakCooldown bool      // недавно упёрся в потолок попытки: пока пропускает других
	WatchedSeconds int64     // наше время просмотра за текущую трансляцию
	// StreakSeconds — время просмотра, засчитываемое в StreakAssumeAfter
	// (WatchedSeconds минус то, что уже было просмотрено до ручного повтора).
	StreakSeconds int64
}

// Slot — один активный просмотр.
type Slot struct {
	ChannelID   string
	BroadcastID string
	Reason      Reason
	StartedAt   time.Time
	// Смысл Deadline зависит от слота:
	//   streak, не получен → когда стрик будет считаться полученным
	//   streak, получен    → конец периода досмотра
	//   fill               → конец минимального удержания
	Deadline  time.Time
	Lingering bool // слот стрика, чей стрик уже пришёл
}

// Decision — результат работы планировщика.
type Decision struct {
	Slots    []Slot   // желаемые слоты, не более MaxSlots
	Assumed  []string // каналы, чей стрик теперь считается полученным (движок это записывает)
	TimedOut []string // попытка упёрлась в StreakAttemptMax: отпущены без зачёта стрика
	Queue    []string // кандидаты на стрик, ещё ждущие свободного слота, по порядку
}

// Decide вычисляет желаемые слоты по текущим слотам и состояниям каналов.
func Decide(p Policy, now time.Time, channels []ChannelState, current []Slot) Decision {
	byID := make(map[string]ChannelState, len(channels))
	for _, c := range channels {
		byID[c.ID] = c
	}
	watchable := func(id string) (ChannelState, bool) {
		c, ok := byID[id]
		return c, ok && c.Enabled && c.Online && c.BroadcastID != ""
	}

	var d Decision
	var fills []Slot // существующие слоты fill, решаются после стриков

	// 1. Существующие слоты стриков: доводим начатое до конца (никогда не вытесняются).
	for _, s := range current {
		c, ok := watchable(s.ChannelID)
		if !ok || c.BroadcastID != s.BroadcastID {
			continue // офлайн, выключен, удалён или новая трансляция: оцениваем заново
		}
		if s.Reason == ReasonFill {
			fills = append(fills, s)
			continue
		}
		switch {
		case c.StreakClaimed && !s.Lingering:
			s.Lingering, s.Deadline = true, now.Add(p.Jitter(p.StreakLinger))
			d.Slots = append(d.Slots, s)
		case c.StreakClaimed && now.Before(s.Deadline):
			d.Slots = append(d.Slots, s) // ещё досматриваем
		case c.StreakClaimed:
			// досмотр окончен, освобождаем
		case now.Before(s.Deadline):
			d.Slots = append(d.Slots, s) // попытка в процессе
		case remaining(p, c) <= 0:
			d.Assumed = append(d.Assumed, c.ID) // смотрели достаточно долго, освобождаем
		case !now.Before(s.StartedAt.Add(p.StreakAttemptMax)):
			d.TimedOut = append(d.TimedOut, c.ID) // минуты не засчитываются: не держим слот вечно
		default:
			// Время вышло, но засчитанных минут ещё мало (часть прошла без
			// PubSub или с ошибками): продлеваем на недостающее, не дальше потолка.
			s.Deadline = minTime(now.Add(remaining(p, c)), s.StartedAt.Add(p.StreakAttemptMax))
			d.Slots = append(d.Slots, s)
		}
	}

	// 2. Очередь стриков. Канал попадает в неё, как только известно состояние стрика (одна
	// проверка RewardList при выходе в эфир). Порядок: ручные повторы, затем стрики, истекающие раньше всех,
	// за ними каналы без известного срока истечения, FIFO по началу трансляции
	// внутри каждой группы. Канал, который уже достаточно смотрели за
	// эту трансляцию (например, до перезапуска), считается готовым.
	var queue []ChannelState
	for _, c := range channels {
		if _, ok := watchable(c.ID); !ok || !c.StreakKnown || c.StreakClaimed || c.StreakCooldown ||
			hasSlot(d.Slots, c.ID) || slices.Contains(d.Assumed, c.ID) || slices.Contains(d.TimedOut, c.ID) {
			continue
		}
		if remaining(p, c) <= 0 {
			d.Assumed = append(d.Assumed, c.ID)
			continue
		}
		queue = append(queue, c)
	}
	slices.SortFunc(queue, func(a, b ChannelState) int {
		if a.StreakRetry != b.StreakRetry {
			return cmp.Compare(boolInt(b.StreakRetry), boolInt(a.StreakRetry)) // сначала ручные повторы
		}
		if a.StreakExpires.IsZero() != b.StreakExpires.IsZero() {
			return cmp.Compare(boolInt(a.StreakExpires.IsZero()), boolInt(b.StreakExpires.IsZero()))
		}
		if c := a.StreakExpires.Compare(b.StreakExpires); c != 0 {
			return c
		}
		if c := a.OnlineSince.Compare(b.OnlineSince); c != 0 {
			return c
		}
		return cmp.Compare(a.ID, b.ID)
	})

	// 3. Кандидаты на стрик занимают свободные слоты, вытесняя слоты fill.
	for i, c := range queue {
		if len(d.Slots) >= p.MaxSlots {
			// Стриков в ожидании больше, чем слотов: они ждут, fill нет вовсе.
			for _, w := range queue[i:] {
				d.Queue = append(d.Queue, w.ID)
			}
			return d
		}
		d.Slots = append(d.Slots, Slot{
			ChannelID: c.ID, BroadcastID: c.BroadcastID, Reason: ReasonStreak,
			StartedAt: now, Deadline: now.Add(remaining(p, c)),
		})
	}

	// 4. Fill: все стрики обработаны, свободные мощности отдаём каналу, который мы
	// смотрели меньше всего за его текущую трансляцию. Подходят каналы, закончившие
	// со стриком, включая считающиеся полученными в этом же решении: канал,
	// который мы только что смотрели, остаётся как fill, а не уходит и возвращается.
	leastWatched := func() (ChannelState, bool) {
		var best ChannelState
		found := false
		for _, c := range channels {
			done := c.StreakClaimed || slices.Contains(d.Assumed, c.ID)
			if _, ok := watchable(c.ID); !ok || !done || hasSlot(d.Slots, c.ID) {
				continue
			}
			if !found || lessWatched(c, best) {
				best, found = c, true
			}
		}
		return best, found
	}

	for _, s := range fills {
		if len(d.Slots) >= p.MaxSlots || hasSlot(d.Slots, s.ChannelID) {
			continue // вытеснен стриком
		}
		if now.Before(s.Deadline) {
			d.Slots = append(d.Slots, s) // минимальное удержание ещё не истекло
			continue
		}
		// Удержание окончено: остаёмся, только если этот канал всё ещё наименее просмотренный.
		if best, ok := leastWatched(); ok && best.ID != s.ChannelID && lessWatched(best, byID[s.ChannelID]) {
			continue
		}
		s.Deadline = now.Add(p.Jitter(p.FillHold))
		d.Slots = append(d.Slots, s)
	}

	for len(d.Slots) < p.MaxSlots {
		c, ok := leastWatched()
		if !ok {
			break
		}
		d.Slots = append(d.Slots, Slot{
			ChannelID: c.ID, BroadcastID: c.BroadcastID, Reason: ReasonFill,
			StartedAt: now, Deadline: now.Add(p.Jitter(p.FillHold)),
		})
	}
	return d
}

// remaining — сколько ещё трансляции нужно просмотреть, прежде чем
// стрик будет считаться полученным.
func remaining(p Policy, c ChannelState) time.Duration {
	return p.StreakAssumeAfter - time.Duration(c.StreakSeconds)*time.Second
}

// lessWatched упорядочивает кандидатов fill: меньшее время просмотра, затем старейшая трансляция, затем id.
func lessWatched(a, b ChannelState) bool {
	if a.WatchedSeconds != b.WatchedSeconds {
		return a.WatchedSeconds < b.WatchedSeconds
	}
	if c := a.OnlineSince.Compare(b.OnlineSince); c != 0 {
		return c < 0
	}
	return a.ID < b.ID
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func hasSlot(slots []Slot, channelID string) bool {
	return slices.ContainsFunc(slots, func(s Slot) bool { return s.ChannelID == channelID })
}
