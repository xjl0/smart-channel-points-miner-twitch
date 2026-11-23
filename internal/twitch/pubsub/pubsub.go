// Package pubsub держит одно WebSocket-соединение с Twitch PubSub и превращает
// сырые сообщения топиков в типизированные события для движка:
//
//   - video-playback-by-id.<channel>          → StreamUp / StreamDown
//   - community-moments-channel-v1.<channel>  → MomentAvailable
//   - community-points-user-v1.<user>         → PointsEarned / ClaimAvailable
//
// Топики каналов могут меняться во время работы (Listen / Unlisten); после каждого
// переподключения подписка на все топики выполняется заново.
package pubsub

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/xjl0/smart-channel-points-miner-twitch/internal/jitter"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/trace"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/twitch/ws"
)

// endpoint — адрес сервера Twitch (переменная, чтобы тест мог подставить свой сервер).
var endpoint = "wss://pubsub-edge.twitch.tv/v1"

const (
	// Twitch требует PING не реже раза в 5 минут; мы пингуем каждые 2–4.
	pingMin, pingMax = 2 * time.Minute, 4 * time.Minute
	pongTimeout      = 15 * time.Second
	maxTopics        = 50 // лимит Twitch на одно соединение
)

// Event — одно из StreamUp, StreamDown, PointsEarned, ClaimAvailable.
type Event interface{ isEvent() }

// StreamUp: канал вышел в эфир.
type StreamUp struct{ ChannelID string }

// StreamDown: канал ушёл из эфира.
type StreamDown struct{ ChannelID string }

// PointsEarned: аккаунт получил баллы канала. Reason — это reason_code
// от Twitch, например WATCH, WATCH_STREAK, CLAIM.
type PointsEarned struct {
	ChannelID string
	Points    int
	Reason    string
	Balance   int
}

// ClaimAvailable: на канале появился сундук.
type ClaimAvailable struct {
	ChannelID string
	ClaimID   string
}

// ClaimClaimed: сундук забран (нами или другим клиентом того же
// аккаунта, например вкладкой браузера с автокликером).
type ClaimClaimed struct {
	ChannelID string
	ClaimID   string
}

// ViewCount: текущее число зрителей канала в эфире (~каждые 30 с).
type ViewCount struct {
	ChannelID string
	Viewers   int
}

// MomentAvailable: на канале можно забрать community moment.
type MomentAvailable struct {
	ChannelID string
	MomentID  string
}

func (StreamUp) isEvent()        {}
func (StreamDown) isEvent()      {}
func (PointsEarned) isEvent()    {}
func (ClaimAvailable) isEvent()  {}
func (MomentAvailable) isEvent() {}
func (ViewCount) isEvent()       {}
func (ClaimClaimed) isEvent()    {}
func (Reconnected) isEvent()     {}

// Reconnected: соединение восстановлено после обрыва. События за время обрыва
// (начало и конец стримов) Twitch не досылает — их нужно перепроверить.
type Reconnected struct{}

// errReconnect — просьба Twitch переподключиться: штатная ситуация, не проблема.
var errReconnect = errors.New("server requested reconnect")

// Client — самопереподключающееся соединение PubSub.
type Client struct {
	log    *slog.Logger
	trace  *trace.Tracer
	events chan Event

	mu     sync.Mutex
	conn   *websocket.Conn
	token  string
	userID string
	topics map[string]struct{} // топики каналов (приватный топик пользователя подразумевается)

	connected atomic.Bool  // подписки отправлены, соединение живо
	sessions  atomic.Int64 // сколько раз подключались (первое — не восстановление)
}

// Connected сообщает, живо ли сейчас соединение с подписками. Без него Twitch
// не засчитывает просмотр (проверено по трассировке), поэтому движок не
// учитывает такие минуты в отсчёте стрика.
func (c *Client) Connected() bool { return c.connected.Load() }

// New создаёт клиент; ничего не подключается до вызова Run.
// tr (может быть nil) получает каждый фрейм в обоих направлениях.
func New(log *slog.Logger, tr *trace.Tracer) *Client {
	return &Client{
		log:    log.With("component", "pubsub"),
		trace:  tr,
		events: make(chan Event, 64),
		topics: map[string]struct{}{},
	}
}

// Events доставляет разобранные события. Читайте его непрерывно.
func (c *Client) Events() <-chan Event { return c.events }

// SetAuth задаёт аккаунт для приватного топика баллов. Открытое
// соединение разрывается, чтобы Run переподключился с новыми учётными данными.
func (c *Client) SetAuth(token, userID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.token, c.userID = token, userID
	if c.conn != nil {
		_ = c.conn.CloseNow() // Run переподключится с новыми данными
	}
}

// PlaybackTopic передаёт stream-up / stream-down канала.
func PlaybackTopic(channelID string) string { return "video-playback-by-id." + channelID }

// MomentsTopic передаёт community moments канала.
func MomentsTopic(channelID string) string { return "community-moments-channel-v1." + channelID }

// WatchTopics — топики канала, на которые подписывается плеер twitch.tv, пока
// открыта трансляция. Мы делаем то же для каналов в наших слотах: это похоже
// на настоящего зрителя, и каждое сообщение попадает в трассировку сырого трафика.
func WatchTopics(channelID string) []string {
	return []string{
		MomentsTopic(channelID),
		"community-points-channel-v1." + channelID,
		"raid." + channelID,
		"predictions-channel-v1." + channelID,
		"hype-train-events-v1." + channelID,
		"broadcast-settings-update." + channelID,
	}
}

// userTopics — приватные топики аккаунта (им нужен OAuth-токен).
func userTopics(userID string) []string {
	return []string{
		"community-points-user-v1." + userID, // баллы, сундуки (разбираются)
		"onsite-notifications." + userID,     // "стример вышел в эфир" и т. п. (трассируются)
		"user-drop-events." + userID,         // прогресс дропов (трассируются)
		"predictions-user-v1." + userID,      // (трассируются)
	}
}

// Listen подписывается на топик канала (см. PlaybackTopic, MomentsTopic).
func (c *Client) Listen(topic string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.topics[topic]; ok {
		return
	}
	if len(c.topics)+len(userTopics("")) >= maxTopics {
		c.log.Error("too many topics for one pubsub connection, ignoring", "topic", topic)
		return
	}
	c.topics[topic] = struct{}{}
	c.sendLocked("LISTEN", []string{topic}, "")
}

// Unlisten удаляет подписку на топик.
func (c *Client) Unlisten(topic string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.topics[topic]; !ok {
		return
	}
	delete(c.topics, topic)
	c.sendLocked("UNLISTEN", []string{topic}, "")
}

// Run подключается и поддерживает соединение, пока ctx не отменён,
// переподключаясь с экспоненциальной задержкой (1s … 2min) при любом сбое.
func (c *Client) Run(ctx context.Context) {
	backoff := time.Second
	shortLived := 0 // сессий подряд, оборвавшихся сразу после подключения
	for ctx.Err() == nil {
		started := time.Now()
		err := c.session(ctx)
		if ctx.Err() != nil {
			return
		}
		if time.Since(started) > time.Minute {
			backoff, shortLived = time.Second, 0 // сессия была здоровой, начинаем заново
		} else if shortLived++; shortLived == 3 {
			// Это не сетевой сбой: Twitch нас отвергает. Без PubSub нет событий
			// баллов/сундуков, поэтому громко сообщаем об этом в админ-панели.
			c.log.Error("pubsub keeps dropping right after connect", "err", err)
		}
		if errors.Is(err, errReconnect) {
			c.log.Info("pubsub reconnect requested by twitch", "in", backoff)
		} else {
			c.log.Warn("pubsub disconnected, reconnecting", "err", err, "in", backoff)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 2*time.Minute)
	}
}

// session обслуживает одно соединение целиком: dial, подписка, чтение, ping.
func (c *Client) session(ctx context.Context) error {
	conn, err := ws.Dial(ctx, endpoint)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.CloseNow()
	// Контекст сессии: его отмена прерывает чтение в readLoop.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	c.mu.Lock()
	c.conn = conn
	topics := make([]string, 0, len(c.topics))
	for t := range c.topics {
		topics = append(topics, t)
	}
	// Один фрейм LISTEN на топик, как в эталонной реализации и веб-клиенте:
	// один фрейм со всеми топиками превышает лимит размера фрейма Twitch, и
	// сервер разрывает соединение (close 1009 "message too big").
	if c.userID != "" {
		for _, t := range userTopics(c.userID) {
			c.sendLocked("LISTEN", []string{t}, c.token)
		}
	}
	for _, t := range topics {
		c.sendLocked("LISTEN", []string{t}, "")
	}
	c.mu.Unlock()
	c.connected.Store(true)
	if c.sessions.Add(1) > 1 { // первое подключение — не восстановление
		select {
		case c.events <- Reconnected{}:
		default: // очередь полна: проверку подхватит плановый опрос
		}
	}
	defer func() {
		c.connected.Store(false)
		c.mu.Lock()
		c.conn = nil
		c.mu.Unlock()
	}()
	c.log.Info("pubsub connected", "topics", len(topics))

	// Горутина чтения: пересылает фреймы, сообщает о первой ошибке.
	pong := make(chan struct{}, 1)
	readErr := make(chan error, 1)
	go func() { readErr <- c.readLoop(ctx, conn, pong) }()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-readErr:
			return err
		case <-time.After(jitter.Between(pingMin, pingMax)):
		}
		c.mu.Lock()
		err := ws.WriteJSON(conn, map[string]string{"type": "PING"})
		c.trace.Write("pubsub", "out", map[string]any{"type": "PING"})
		c.mu.Unlock()
		if err != nil {
			return fmt.Errorf("ping: %w", err)
		}
		select {
		case <-pong:
		case err := <-readErr:
			return err
		case <-time.After(pongTimeout):
			return errors.New("no PONG within timeout")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// readLoop читает фреймы, пока соединение не оборвётся.
func (c *Client) readLoop(ctx context.Context, conn *websocket.Conn, pong chan<- struct{}) error {
	for {
		_, raw, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		var frame struct {
			Type  string `json:"type"`
			Error string `json:"error"`
			Data  struct {
				Topic   string `json:"topic"`
				Message string `json:"message"`
			} `json:"data"`
		}
		if err := json.Unmarshal(raw, &frame); err != nil {
			c.log.Warn("bad pubsub frame", "err", err)
			continue
		}
		c.traceFrame(raw, frame.Type, frame.Data.Topic, frame.Data.Message)
		switch frame.Type {
		case "PONG":
			select {
			case pong <- struct{}{}:
			default:
			}
		case "RECONNECT":
			return errReconnect
		case "RESPONSE":
			if frame.Error != "" {
				c.log.Error("pubsub LISTEN rejected", "error", frame.Error)
			}
		case "MESSAGE":
			if ev := ParseMessage(frame.Data.Topic, frame.Data.Message); ev != nil {
				c.log.Debug("pubsub event", "topic", frame.Data.Topic, "event", fmt.Sprintf("%T", ev))
				c.events <- ev
			}
		}
	}
}

// ParseMessage преобразует одно сообщение топика (JSON-строку внутри фрейма
// PubSub MESSAGE) в Event или возвращает nil, если майнер на него не реагирует.
func ParseMessage(topic, message string) Event {
	var msg struct {
		Type    string `json:"type"`
		Viewers int    `json:"viewers"`
		Data    struct {
			PointGain struct {
				ChannelID   string `json:"channel_id"`
				TotalPoints int    `json:"total_points"`
				ReasonCode  string `json:"reason_code"`
			} `json:"point_gain"`
			Balance struct {
				Balance int `json:"balance"`
			} `json:"balance"`
			Claim struct {
				ID        string `json:"id"`
				ChannelID string `json:"channel_id"`
			} `json:"claim"`
			MomentID string `json:"moment_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(message), &msg); err != nil {
		return nil
	}

	if channelID, ok := strings.CutPrefix(topic, "video-playback-by-id."); ok {
		switch msg.Type {
		case "stream-up":
			return StreamUp{ChannelID: channelID}
		case "stream-down":
			return StreamDown{ChannelID: channelID}
		case "viewcount":
			return ViewCount{ChannelID: channelID, Viewers: msg.Viewers}
		}
		return nil
	}

	if channelID, ok := strings.CutPrefix(topic, "community-moments-channel-v1."); ok {
		if msg.Type == "active" && msg.Data.MomentID != "" {
			return MomentAvailable{ChannelID: channelID, MomentID: msg.Data.MomentID}
		}
		return nil
	}

	if !strings.HasPrefix(topic, "community-points-user-v1.") {
		return nil // остальные топики только трассируются
	}
	switch msg.Type {
	case "points-earned":
		g := msg.Data.PointGain
		return PointsEarned{ChannelID: g.ChannelID, Points: g.TotalPoints, Reason: g.ReasonCode, Balance: msg.Data.Balance.Balance}
	case "claim-available":
		return ClaimAvailable{ChannelID: msg.Data.Claim.ChannelID, ClaimID: msg.Data.Claim.ID}
	case "claim-claimed":
		return ClaimClaimed{ChannelID: msg.Data.Claim.ChannelID, ClaimID: msg.Data.Claim.ID}
	}
	return nil
}

// sendLocked отправляет фрейм LISTEN/UNLISTEN, если соединение есть. Вызывающий держит c.mu.
func (c *Client) sendLocked(kind string, topics []string, token string) {
	if c.conn == nil {
		return // будет отправлено при (пере)подключении
	}
	data := map[string]any{"topics": topics}
	if token != "" {
		data["auth_token"] = token
	}
	frame := map[string]any{"type": kind, "nonce": nonce(), "data": data}
	c.trace.Write("pubsub", "out", map[string]any{"type": kind, "topics": topics}) // токен опущен
	if err := ws.WriteJSON(c.conn, frame); err != nil {
		c.log.Warn("pubsub write failed", "type", kind, "err", err)
		return
	}
	c.log.Debug("pubsub "+strings.ToLower(kind), "topics", topics)
}

// traceFrame записывает входящий фрейм; полезная нагрузка MESSAGE (JSON внутри
// строки) распаковывается, чтобы трассировка была читаемой.
func (c *Client) traceFrame(raw []byte, typ, topic, message string) {
	if c.trace == nil {
		return
	}
	if typ == "MESSAGE" {
		c.trace.Write("pubsub", "in", map[string]any{"type": typ, "topic": topic, "message": trace.JSON([]byte(message))})
		return
	}
	c.trace.Write("pubsub", "in", map[string]any{"frame": trace.JSON(raw)})
}

func nonce() string {
	b := make([]byte, 12)
	_, _ = crand.Read(b)
	return hex.EncodeToString(b)
}
