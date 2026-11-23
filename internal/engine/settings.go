package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	minerv1 "github.com/xjl0/smart-channel-points-miner-twitch/internal/gen/miner/v1"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/jitter"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/store/db"
)

// Настройки — таймеры движка, которые меняются из админки (страница
// «Настройки»). Хранятся в БД как protobuf miner.v1.Settings, применяются
// сразу: текущие слоты пересчитываются, таймеры проверок перезапускаются.
// Жёсткие правила (не больше 2 каналов, minute-watched раз в минуту) сюда
// не входят и не меняются.

// ErrInvalidSettings — настройки вне допустимых границ (текст — для админки).
var ErrInvalidSettings = errors.New("недопустимые настройки")

// DefaultSettings — значения по умолчанию (из DefaultPolicy и DefaultTimings).
func DefaultSettings() *minerv1.Settings {
	return settingsFrom(DefaultPolicy(), DefaultTimings())
}

// settingsFrom переводит политику и задержки движка в сообщение для админки.
func settingsFrom(p Policy, t Timings) *minerv1.Settings {
	sec := func(d time.Duration) int32 { return int32(d / time.Second) }
	return &minerv1.Settings{
		StreakAssumeAfterSec: sec(p.StreakAssumeAfter),
		StreakAttemptMaxSec:  sec(p.StreakAttemptMax),
		StreakLingerSec:      sec(p.StreakLinger),
		FillHoldSec:          sec(p.FillHold),
		StreakCooldownSec:    sec(t.StreakCooldown),
		StreakCheckEverySec:  sec(t.StreakCheckEvery),
		ChestDelayMinSec:     sec(t.ChestDelay[0]),
		ChestDelayMaxSec:     sec(t.ChestDelay[1]),
		MomentDelayMinSec:    sec(t.MomentDelay[0]),
		MomentDelayMaxSec:    sec(t.MomentDelay[1]),
		OnlinePollMinSec:     sec(t.OnlinePoll[0]),
		OnlinePollMaxSec:     sec(t.OnlinePoll[1]),
		SchedulerTickMinSec:  sec(t.SchedulerTick[0]),
		SchedulerTickMaxSec:  sec(t.SchedulerTick[1]),
	}
}

// applyTo переносит настройки в политику и задержки движка.
func applyTo(s *minerv1.Settings, p *Policy, t *Timings) {
	d := func(v int32) time.Duration { return time.Duration(v) * time.Second }
	p.StreakAssumeAfter = d(s.StreakAssumeAfterSec)
	p.StreakAttemptMax = d(s.StreakAttemptMaxSec)
	p.StreakLinger = d(s.StreakLingerSec)
	p.FillHold = d(s.FillHoldSec)
	t.StreakCooldown = d(s.StreakCooldownSec)
	t.StreakCheckEvery = d(s.StreakCheckEverySec)
	t.ChestDelay = [2]time.Duration{d(s.ChestDelayMinSec), d(s.ChestDelayMaxSec)}
	t.MomentDelay = [2]time.Duration{d(s.MomentDelayMinSec), d(s.MomentDelayMaxSec)}
	t.OnlinePoll = [2]time.Duration{d(s.OnlinePollMinSec), d(s.OnlinePollMaxSec)}
	t.SchedulerTick = [2]time.Duration{d(s.SchedulerTickMinSec), d(s.SchedulerTickMaxSec)}
}

// ValidateSettings проверяет границы. Сообщение ошибки показывается в админке.
func ValidateSettings(s *minerv1.Settings) error {
	type rng struct {
		name     string
		v        int32
		min, max int32
	}
	const m, h = 60, 3600
	checks := []rng{
		{"«Считать стрик полученным после»", s.StreakAssumeAfterSec, 1 * m, 2 * h},
		{"«Потолок попытки стрика»", s.StreakAttemptMaxSec, 1 * m, 4 * h},
		{"«Досмотр после стрика»", s.StreakLingerSec, 0, 30 * m},
		{"«Минимум на канале»", s.FillHoldSec, 1 * m, 4 * h},
		{"«Отдых канала после потолка»", s.StreakCooldownSec, 0, 2 * h},
		{"«Проверка стрика раз в»", s.StreakCheckEverySec, 1 * m, 30 * m},
		{"«Задержка клика сундука» (от)", s.ChestDelayMinSec, 0, 10 * m},
		{"«Задержка клика сундука» (до)", s.ChestDelayMaxSec, 0, 10 * m},
		{"«Задержка получения момента» (от)", s.MomentDelayMinSec, 0, 10 * m},
		{"«Задержка получения момента» (до)", s.MomentDelayMaxSec, 0, 10 * m},
		{"«Проверка онлайна каналов» (от)", s.OnlinePollMinSec, 1 * m, 1 * h},
		{"«Проверка онлайна каналов» (до)", s.OnlinePollMaxSec, 1 * m, 2 * h},
		{"«Шаг планировщика» (от)", s.SchedulerTickMinSec, 5, 10 * m},
		{"«Шаг планировщика» (до)", s.SchedulerTickMaxSec, 5, 15 * m},
	}
	for _, c := range checks {
		if c.v < c.min || c.v > c.max {
			return fmt.Errorf("%w: %s должно быть от %s до %s", ErrInvalidSettings, c.name, human(c.min), human(c.max))
		}
	}
	pairs := []struct {
		name     string
		min, max int32
	}{
		{"«Задержка клика сундука»", s.ChestDelayMinSec, s.ChestDelayMaxSec},
		{"«Задержка получения момента»", s.MomentDelayMinSec, s.MomentDelayMaxSec},
		{"«Проверка онлайна каналов»", s.OnlinePollMinSec, s.OnlinePollMaxSec},
		{"«Шаг планировщика»", s.SchedulerTickMinSec, s.SchedulerTickMaxSec},
	}
	for _, pr := range pairs {
		if pr.min > pr.max {
			return fmt.Errorf("%w: в %s «от» больше «до»", ErrInvalidSettings, pr.name)
		}
	}
	if s.StreakAttemptMaxSec < s.StreakAssumeAfterSec {
		return fmt.Errorf("%w: «Потолок попытки стрика» не может быть меньше «Считать стрик полученным после»", ErrInvalidSettings)
	}
	return nil
}

// human форматирует секунды для сообщений об ошибке: «90 с», «15 мин», «2 ч».
func human(sec int32) string {
	switch {
	case sec >= 3600 && sec%3600 == 0:
		return fmt.Sprintf("%d ч", sec/3600)
	case sec >= 60 && sec%60 == 0:
		return fmt.Sprintf("%d мин", sec/60)
	}
	return fmt.Sprintf("%d с", sec)
}

// withDefaults заполняет нулевые поля значениями по умолчанию: так старые
// сохранённые настройки переживают появление новых полей.
func withDefaults(s *minerv1.Settings) *minerv1.Settings {
	out := proto.Clone(s).(*minerv1.Settings)
	def := DefaultSettings().ProtoReflect()
	m := out.ProtoReflect()
	fields := m.Descriptor().Fields()
	for i := range fields.Len() {
		f := fields.Get(i)
		if m.Get(f).Int() == 0 && def.Get(f).Int() != 0 && !zeroAllowed(f) {
			m.Set(f, def.Get(f))
		}
	}
	return out
}

// zeroAllowed — поля, для которых 0 — осмысленное значение (без задержки,
// без досмотра, без отдыха), а не «не задано».
func zeroAllowed(f protoreflect.FieldDescriptor) bool {
	switch f.Name() {
	case "streak_linger_sec", "streak_cooldown_sec", "chest_delay_min_sec", "chest_delay_max_sec",
		"moment_delay_min_sec", "moment_delay_max_sec":
		return true
	}
	return false
}

// loadSettings читает настройки из БД при старте (нет строки — по умолчанию).
func (e *Engine) loadSettings() error {
	raw, err := e.store.GetSettings(dbctx())
	if isNoRows(err) {
		e.settings = DefaultSettings()
		return nil
	}
	if err != nil {
		return fmt.Errorf("load settings: %w", err)
	}
	s := &minerv1.Settings{}
	if err := proto.Unmarshal(raw, s); err != nil {
		e.log.Error("stored settings are unreadable, using defaults", "err", err)
		e.settings = DefaultSettings()
		return nil
	}
	s = withDefaults(s)
	if err := ValidateSettings(s); err != nil {
		e.log.Error("stored settings are invalid, using defaults", "err", err)
		s = DefaultSettings()
	}
	e.settings = s
	applyTo(s, &e.policy, &e.timing)
	return nil
}

// UpdateSettings проверяет, сохраняет и сразу применяет новые настройки.
func (e *Engine) UpdateSettings(_ context.Context, s *minerv1.Settings) (*minerv1.Settings, error) {
	if s == nil {
		return nil, fmt.Errorf("%w: пустые настройки", ErrInvalidSettings)
	}
	if err := ValidateSettings(s); err != nil {
		return nil, err
	}
	s = proto.Clone(s).(*minerv1.Settings)
	err := e.call(func() error {
		raw, err := proto.Marshal(s)
		if err != nil {
			return err
		}
		if err := e.store.SaveSettings(dbctx(), db.SaveSettingsParams{Data: raw, UpdatedAt: time.Now().UTC()}); err != nil {
			return err
		}
		old := e.settings
		e.settings = s
		applyTo(s, &e.policy, &e.timing)
		e.log.Info("settings changed", changedFields(old, s)...)
		e.publishSettings()
		e.rebaseSlots()
		e.resetTimers()
		e.reconcile()
		return nil
	})
	if err != nil {
		return nil, err
	}
	return proto.Clone(s).(*minerv1.Settings), nil
}

// changedFields — пары «поле, старое → новое» для лога.
func changedFields(old, cur *minerv1.Settings) []any {
	var out []any
	o, c := old.ProtoReflect(), cur.ProtoReflect()
	fields := c.Descriptor().Fields()
	for i := range fields.Len() {
		f := fields.Get(i)
		if a, b := o.Get(f).Int(), c.Get(f).Int(); a != b {
			out = append(out, string(f.Name()), fmt.Sprintf("%d → %d", a, b))
		}
	}
	return out
}

// publishSettings отправляет текущие настройки (и значения по умолчанию) в админку.
func (e *Engine) publishSettings() {
	e.publish(&minerv1.LiveMessage{Body: &minerv1.LiveMessage_SettingsChanged{SettingsChanged: &minerv1.SettingsChanged{
		Settings: proto.Clone(e.settings).(*minerv1.Settings),
		Defaults: DefaultSettings(),
	}}})
}

// rebaseSlots пересчитывает сроки текущих слотов под новые настройки, чтобы
// изменение подействовало сразу, а не со следующего слота.
func (e *Engine) rebaseSlots() {
	now := e.now()
	for i := range e.slots {
		s := &e.slots[i]
		ch := e.channels[s.ChannelID]
		switch {
		case s.Reason == ReasonFill:
			s.Deadline = s.StartedAt.Add(e.policy.FillHold)
		case s.Lingering:
			s.Deadline = minTime(s.Deadline, now.Add(e.policy.StreakLinger))
		case ch != nil:
			state := ChannelState{StreakSeconds: ch.watched - ch.streakBase}
			s.Deadline = minTime(now.Add(remaining(e.policy, state)), s.StartedAt.Add(e.policy.StreakAttemptMax))
		}
		if v := e.viewing[s.ChannelID]; v != nil {
			v.slot = *s
		}
	}
	e.publishSlots()
}

// resetTimers перезапускает таймеры шага планировщика и проверки онлайна с
// новыми интервалами (иначе старый интервал дождался бы своего срабатывания).
func (e *Engine) resetTimers() {
	if e.tickTimer != nil {
		e.tickTimer.Reset(jitter.Between(e.timing.SchedulerTick[0], e.timing.SchedulerTick[1]))
	}
	if e.pollTimer != nil {
		e.pollTimer.Reset(jitter.Between(e.timing.OnlinePoll[0], e.timing.OnlinePoll[1]))
	}
}
