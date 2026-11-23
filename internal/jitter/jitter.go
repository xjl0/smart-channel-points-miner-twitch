// Package jitter предоставляет случайные задержки. Всё, что майнер делает по
// таймеру, проходит через него, чтобы у трафика никогда не было машинно-идеального ритма.
package jitter

import (
	"context"
	"math/rand/v2"
	"time"
)

// Between возвращает равномерно распределённую случайную длительность в [lo, hi].
func Between(lo, hi time.Duration) time.Duration {
	if hi <= lo {
		return lo
	}
	return lo + rand.N(hi-lo+1)
}

// Around возвращает d ± spread (никогда не отрицательное).
func Around(d, spread time.Duration) time.Duration {
	return max(Between(d-spread, d+spread), 0)
}

// Sleep ждёт d или завершения ctx. Возвращает false, если ctx завершился раньше.
func Sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
