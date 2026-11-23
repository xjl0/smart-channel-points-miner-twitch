// Package trace записывает сырой трафик обмена с Twitch (фреймы PubSub,
// строки IRC, запросы/ответы GQL, события spade) в виде JSON-строк для последующего
// анализа. Секреты (OAuth-токены) никогда не записываются.
//
// Файлы почасовые (<dir>/YYYY-MM-DDTHH.jsonl). Объём ограничен: в начале каждого
// нового часа удаляются файлы старше MaxAge, затем самые старые, пока
// каталог не станет меньше MaxBytes, так что трассировку можно держать включённой годами.
//
// Nil *Tracer допустим и ничего не записывает, поэтому компоненты всегда могут его
// вызывать; трассировка включается установкой MINER_TRACE_DIR.
package trace

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Limits ограничивают место на диске, занимаемое трассировкой.
type Limits struct {
	MaxAge   time.Duration // удалять файлы старше этого срока
	MaxBytes int64         // и держать каталог меньше этого размера
}

// DefaultLimits хранят три дня, не более 200 МБ.
var DefaultLimits = Limits{MaxAge: 72 * time.Hour, MaxBytes: 200 << 20}

const fileLayout = "2006-01-02T15" // один файл на час

// Tracer дописывает события в почасовые файлы. Безопасен для конкурентного использования.
type Tracer struct {
	dir    string
	limits Limits
	log    *slog.Logger

	mu   sync.Mutex
	hour string
	file *os.File
}

// New возвращает трассировщик, пишущий в dir, или nil (выключен), если dir пуст.
func New(dir string, limits Limits, log *slog.Logger) *Tracer {
	if dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Error("trace disabled: cannot create dir", "dir", dir, "err", err)
		return nil
	}
	log.Info("raw twitch traffic is traced", "dir", dir, "max_age", limits.MaxAge, "max_mb", limits.MaxBytes>>20)
	return &Tracer{dir: dir, limits: limits, log: log.With("component", "trace")}
}

// Write записывает одно событие. src — канал ("gql", "pubsub", "irc",
// "spade"), ev — что произошло ("in", "out", "exchange", …); fields —
// произвольные дополнительные данные. Значения в виде JSON-текста (json.RawMessage)
// встраиваются как JSON, чтобы файлы было легко запрашивать через jq.
func (t *Tracer) Write(src, ev string, fields map[string]any) {
	if t == nil {
		return
	}
	now := time.Now().UTC()
	rec := make(map[string]any, len(fields)+3)
	for k, v := range fields {
		rec[k] = v
	}
	rec["ts"], rec["src"], rec["ev"] = now.Format(time.RFC3339Nano), src, ev
	line, err := json.Marshal(rec)
	if err != nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.rotate(now); err != nil {
		return
	}
	_, _ = t.file.Write(append(line, '\n'))
}

// rotate открывает файл текущего часа и применяет лимиты, когда
// начинается новый файл. Вызывающий держит t.mu.
func (t *Tracer) rotate(now time.Time) error {
	hour := now.Format(fileLayout)
	if t.file != nil && t.hour == hour {
		return nil
	}
	if t.file != nil {
		_ = t.file.Close()
	}
	f, err := os.OpenFile(filepath.Join(t.dir, hour+".jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.log.Error("open trace file", "err", err)
		t.file = nil
		return err
	}
	t.file, t.hour = f, hour
	t.prune(now)
	return nil
}

// prune удаляет файлы старше MaxAge, затем самые старые, пока не уложится в MaxBytes.
// Файл, в который идёт запись, никогда не удаляется. Вызывающий держит t.mu.
func (t *Tracer) prune(now time.Time) {
	paths, _ := filepath.Glob(filepath.Join(t.dir, "*.jsonl"))
	sort.Strings(paths) // имена сортируются хронологически
	type file struct {
		path string
		size int64
	}
	var files []file
	var total int64
	cutoff := now.Add(-t.limits.MaxAge).Format(fileLayout)
	for _, p := range paths {
		name := strings.TrimSuffix(filepath.Base(p), ".jsonl")
		if name == t.hour {
			continue
		}
		if name < cutoff {
			t.remove(p, "older than max age")
			continue
		}
		if st, err := os.Stat(p); err == nil {
			files = append(files, file{p, st.Size()})
			total += st.Size()
		}
	}
	for _, f := range files {
		if total <= t.limits.MaxBytes {
			break
		}
		t.remove(f.path, "trace over size limit")
		total -= f.size
	}
}

func (t *Tracer) remove(path, why string) {
	if err := os.Remove(path); err == nil {
		t.log.Debug("trace file removed", "file", filepath.Base(path), "reason", why)
	}
}

// JSON возвращает raw как встраиваемый JSON, если это валидный JSON, иначе как строку.
func JSON(raw []byte) any {
	if json.Valid(raw) {
		return json.RawMessage(raw)
	}
	return string(raw)
}
