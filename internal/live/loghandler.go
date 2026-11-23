package live

import (
	"context"
	"fmt"
	"log/slog"

	minerv1 "github.com/xjl0/smart-channel-points-miner-twitch/internal/gen/miner/v1"
)

// LogHandler раздваивает записи slog: всё идёт в обёрнутый обработчик
// (stdout), а записи уровня Info и выше также публикуются как LogEntry, чтобы
// админ-панель показывала лог вживую. Записи Warn/Error сохраняются.
type LogHandler struct {
	next   slog.Handler
	bus    *Bus
	attrs  []slog.Attr
	prefix string // накопленный префикс групп, напр. "req."
}

// NewLogHandler оборачивает next. Шина должна использовать логгер, построенный на самом next,
// а не на этом обработчике, иначе ошибка шины снова логировалась бы в шину.
func NewLogHandler(next slog.Handler, bus *Bus) *LogHandler {
	return &LogHandler{next: next, bus: bus}
}

func (h *LogHandler) Enabled(ctx context.Context, l slog.Level) bool { return h.next.Enabled(ctx, l) }

func (h *LogHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Level >= slog.LevelInfo {
		attrs := make(map[string]string, len(h.attrs)+r.NumAttrs())
		for _, a := range h.attrs {
			attrs[a.Key] = a.Value.String()
		}
		r.Attrs(func(a slog.Attr) bool {
			attrs[h.prefix+a.Key] = fmt.Sprint(a.Value.Resolve().Any())
			return true
		})
		h.bus.Publish(&minerv1.LiveMessage{Body: &minerv1.LiveMessage_LogEntry{LogEntry: &minerv1.LogEntry{
			Level: r.Level.String(), Message: r.Message, Attrs: attrs,
		}}})
	}
	return h.next.Handle(ctx, r)
}

func (h *LogHandler) WithAttrs(as []slog.Attr) slog.Handler {
	c := *h
	c.next = h.next.WithAttrs(as)
	c.attrs = append([]slog.Attr(nil), h.attrs...)
	for _, a := range as {
		c.attrs = append(c.attrs, slog.Attr{Key: h.prefix + a.Key, Value: a.Value.Resolve()})
	}
	return &c
}

func (h *LogHandler) WithGroup(name string) slog.Handler {
	c := *h
	c.next = h.next.WithGroup(name)
	c.prefix = h.prefix + name + "."
	return &c
}
