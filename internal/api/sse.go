package api

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	minerv1 "github.com/xjl0/smart-channel-points-miner-twitch/internal/gen/miner/v1"
)

// heartbeatEvery поддерживает простаивающие потоки живыми через прокси и позволяет панели
// показывать индикатор соединения.
const heartbeatEvery = 15 * time.Second

// live обслуживает GET /live как поток Server-Sent Events.
//
// Каждое событие — один LiveMessage в JSON (protojson, та же форма, что выдаёт
// TS-генератор кода). Дельты и снапшоты несут "id: <seq>", поэтому
// EventSource браузера отправляет его обратно как Last-Event-ID при переподключении,
// и мы воспроизводим ровно пропущенные дельты: возобновление достаётся даром.
// ?since=<seq> делает то же для нового EventSource (после разрыва).
//
//	id: 42
//	data: {"seq":"42","ts":"…","channelUpdated":{"channel":{…}}}
func (s *Server) live(w http.ResponseWriter, r *http.Request) {
	rc := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no") // просим прокси вроде nginx не буферизовать

	since := sinceSeq(r)
	sub := s.bus.Subscribe(since)
	defer sub.Close()
	s.log.Debug("admin connected (sse)", "since_seq", since, "backlog", len(sub.Backlog), "peer", r.RemoteAddr)
	defer s.log.Debug("admin disconnected (sse)", "peer", r.RemoteAddr)

	// Быстро переподключаться после обрыва (по умолчанию у EventSource ~3 с).
	if _, err := fmt.Fprint(w, "retry: 2000\n\n"); err != nil {
		return
	}
	for _, msg := range sub.Backlog {
		if writeEvent(w, msg) != nil {
			return
		}
	}
	if rc.Flush() != nil {
		return
	}

	hb := time.NewTicker(heartbeatEvery)
	defer hb.Stop()
	for {
		var msg *minerv1.LiveMessage
		select {
		case <-r.Context().Done():
			return
		case m, ok := <-sub.C:
			if !ok {
				return // слишком медленно: браузер переподключится с Last-Event-ID
			}
			msg = m
		case <-hb.C:
			msg = &minerv1.LiveMessage{Seq: s.bus.Seq(), Ts: timestamppb.Now(),
				Body: &minerv1.LiveMessage_Heartbeat{Heartbeat: &minerv1.Heartbeat{}}}
		}
		if writeEvent(w, msg) != nil || rc.Flush() != nil {
			return
		}
	}
}

// writeEvent пишет одно SSE-событие. id получают только дельты и снапшоты:
// heartbeat'ы и строки чата не входят в воспроизводимую последовательность.
func writeEvent(w http.ResponseWriter, msg *minerv1.LiveMessage) error {
	data, err := protojson.Marshal(msg) // одна строка: protojson по умолчанию не выдаёт переводов строк
	if err != nil {
		return err
	}
	switch msg.Body.(type) {
	case *minerv1.LiveMessage_Heartbeat, *minerv1.LiveMessage_ChatMessage:
		_, err = fmt.Fprintf(w, "data: %s\n\n", data)
	default:
		_, err = fmt.Fprintf(w, "id: %d\ndata: %s\n\n", msg.Seq, data)
	}
	return err
}

// sinceSeq выбирает точку возобновления: Last-Event-ID (отправляется EventSource при
// автоматическом переподключении, всегда самый новый) важнее ?since=.
func sinceSeq(r *http.Request) uint64 {
	for _, v := range []string{r.Header.Get("Last-Event-ID"), r.URL.Query().Get("since")} {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			return n
		}
	}
	return 0
}
