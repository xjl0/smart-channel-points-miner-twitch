package trace

import (
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestWriteAndNil(t *testing.T) {
	var none *Tracer
	none.Write("x", "y", nil) // выключенный трассировщик ничего не делает

	dir := t.TempDir()
	tr := New(dir, DefaultLimits, quiet)
	tr.Write("pubsub", "in", map[string]any{"raw": JSON([]byte(`{"type":"PONG"}`))})

	raw, err := os.ReadFile(filepath.Join(dir, time.Now().UTC().Format(fileLayout)+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var rec struct {
		Src string         `json:"src"`
		Raw map[string]any `json:"raw"`
	}
	if err := json.Unmarshal(raw, &rec); err != nil || rec.Src != "pubsub" || rec.Raw["type"] != "PONG" {
		t.Fatalf("record = %s (err %v)", raw, err)
	}
}

func TestPruneByAgeAndSize(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	write := func(at time.Time, size int) string {
		p := filepath.Join(dir, at.Format(fileLayout)+".jsonl")
		if err := os.WriteFile(p, make([]byte, size), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	tooOld := write(now.Add(-100*time.Hour), 10)
	oldest := write(now.Add(-3*time.Hour), 600)
	newer := write(now.Add(-2*time.Hour), 600)

	tr := &Tracer{dir: dir, limits: Limits{MaxAge: 72 * time.Hour, MaxBytes: 1000}, log: quiet}
	tr.mu.Lock()
	if err := tr.rotate(now); err != nil { // начинается новый час → лимиты применяются
		t.Fatal(err)
	}
	tr.mu.Unlock()

	exists := func(p string) bool { _, err := os.Stat(p); return err == nil }
	if exists(tooOld) || exists(oldest) || !exists(newer) {
		t.Fatalf("after prune: tooOld=%v oldest=%v newer=%v (want false false true)", exists(tooOld), exists(oldest), exists(newer))
	}
}
