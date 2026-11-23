// Package chat поддерживает присутствие аккаунта в чатах просматриваемых каналов.
//
// Он открывает одно соединение IRC-over-WebSocket (тот же транспорт, что
// использует веб-плеер twitch.tv) и делает JOIN/PART каналов по требованию. Он никогда
// не отправляет сообщения в чат: одного JOIN достаточно, чтобы попасть в список зрителей.
// Сообщения из чатов, к которым выполнен JOIN, отдаются через Messages(), чтобы
// админ-панель могла показывать их вживую (ничего не сохраняется).
package chat

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/xjl0/smart-channel-points-miner-twitch/internal/trace"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/twitch/ws"
)

// endpoint — адрес сервера Twitch (переменная, чтобы тест мог подставить свой сервер).
var endpoint = "wss://irc-ws.chat.twitch.tv:443"

const (
	// Twitch пингует примерно раз в 5 минут; тишина дольше этого = мёртвое соединение.
	readTimeout = 7 * time.Minute
)

// errAuth сигнализирует об отклонённых учётных данных; быстрые повторы не помогут.
var errAuth = errors.New("chat authentication failed")

// Message — одна строка чата из канала, к которому выполнен JOIN.
type Message struct {
	Channel  string // логин канала
	User     string // отображаемое имя автора
	Color    string // цвет имени автора, "#RRGGBB" или пусто
	Text     string
	Badges   []string // названия значков: broadcaster, moderator, vip, subscriber, …
	FirstMsg bool     // первое сообщение автора в этом чате
}

// RoomState — набор ограничений чата канала (IRC ROOMSTATE).
type RoomState struct {
	Channel       string
	EmoteOnly     bool
	FollowersOnly int // -1 выкл., 0 любой фолловер, N минут
	SlowSeconds   int
	SubsOnly      bool
	UniqueChat    bool // r9k
}

// Client — самопереподключающийся хранитель присутствия в чате.
type Client struct {
	log      *slog.Logger
	trace    *trace.Tracer
	messages chan Message
	rooms    chan RoomState
	roomMu   sync.Mutex
	state    map[string]RoomState // последнее для каждого канала (обновления ROOMSTATE частичные)

	mu     sync.Mutex
	conn   *websocket.Conn
	login  string
	token  string
	joined map[string]struct{} // желаемый набор логинов каналов
}

// New создаёт клиент; ничего не подключается до вызова Run.
// tr (может быть nil) получает каждую строку IRC в обоих направлениях (кроме PASS).
func New(log *slog.Logger, tr *trace.Tracer) *Client {
	return &Client{
		log:      log.With("component", "chat"),
		trace:    tr,
		messages: make(chan Message, 256),
		rooms:    make(chan RoomState, 16),
		state:    map[string]RoomState{},
		joined:   map[string]struct{}{},
	}
}

// Messages доставляет строки чата; если их никто не успевает читать, они отбрасываются.
func (c *Client) Messages() <-chan Message { return c.messages }

// RoomStates доставляет полные ограничения чата при каждом их изменении.
func (c *Client) RoomStates() <-chan RoomState { return c.rooms }

// SetAuth задаёт учётные данные аккаунта. Открытое соединение разрывается, чтобы
// Run переподключился с новыми.
func (c *Client) SetAuth(login, token string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.login, c.token = strings.ToLower(login), token
	if c.conn != nil {
		_ = c.conn.CloseNow() // Run переподключится с новыми данными
	}
}

// Join входит в чат канала (и продолжает входить в него заново после переподключений).
func (c *Client) Join(channel string) {
	channel = strings.ToLower(channel)
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.joined[channel]; ok {
		return
	}
	c.joined[channel] = struct{}{}
	c.writeLocked("JOIN #" + channel)
	c.log.Debug("chat join", "channel", channel) // движок логирует "started watching"
}

// Part покидает чат канала.
func (c *Client) Part(channel string) {
	channel = strings.ToLower(channel)
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.joined[channel]; !ok {
		return
	}
	delete(c.joined, channel)
	c.writeLocked("PART #" + channel)
	c.roomMu.Lock()
	delete(c.state, channel) // свежий ROOMSTATE придёт со следующим JOIN
	c.roomMu.Unlock()
	c.log.Debug("chat part", "channel", channel)
}

// Joined возвращает отсортированные каналы, в которых мы есть (или будем после переподключения).
func (c *Client) Joined() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.joined))
	for ch := range c.joined {
		out = append(out, ch)
	}
	sort.Strings(out)
	return out
}

// Run поддерживает соединение, пока ctx не отменён. Экспоненциальная задержка
// при сбоях; 10 минут после ошибки аутентификации.
func (c *Client) Run(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		started := time.Now()
		err := c.session(ctx)
		if ctx.Err() != nil {
			return
		}
		wait := backoff
		switch {
		case errors.Is(err, errAuth):
			wait = 10 * time.Minute
		case time.Since(started) > time.Minute:
			wait, backoff = time.Second, time.Second
		default:
			backoff = min(backoff*2, 2*time.Minute)
		}
		logDisconnect(c.log, "chat", err, wait)
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// errReconnect — просьба Twitch переподключиться: штатная ситуация, не проблема.
var errReconnect = errors.New("server requested reconnect")

// logDisconnect логирует разрыв соединения: Info для запрошенного переподключения,
// Warn для настоящих сбоев.
func logDisconnect(log *slog.Logger, what string, err error, wait time.Duration) {
	if errors.Is(err, errReconnect) {
		log.Info(what+" reconnect requested by twitch", "in", wait)
		return
	}
	log.Warn(what+" disconnected, reconnecting", "err", err, "in", wait)
}

// session обслуживает одно соединение: аутентификация, повторный JOIN каналов, ответы на PING.
func (c *Client) session(ctx context.Context) error {
	c.mu.Lock()
	login, token := c.login, c.token
	c.mu.Unlock()
	if login == "" || token == "" {
		return errors.New("no credentials yet")
	}

	conn, err := ws.Dial(ctx, endpoint)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.CloseNow()

	c.mu.Lock()
	c.conn = conn
	c.writeLocked("CAP REQ :twitch.tv/tags twitch.tv/commands")
	c.writeLocked("PASS oauth:" + token)
	c.writeLocked("NICK " + login)
	for ch := range c.joined {
		c.writeLocked("JOIN #" + ch)
	}
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.conn = nil
		c.mu.Unlock()
	}()

	for {
		// Тишина дольше readTimeout = мёртвое соединение: Read закроет его и вернёт ошибку.
		readCtx, cancel := context.WithTimeout(ctx, readTimeout)
		_, raw, err := conn.Read(readCtx)
		cancel()
		if err != nil {
			return err
		}
		// Один фрейм может содержать несколько строк IRC.
		for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\r\n") {
			if !strings.Contains(line, " PRIVMSG #") { // чат других зрителей: проксируется вживую, никогда не сохраняется
				c.trace.Write("irc", "in", map[string]any{"line": line})
			}
			if err := c.handleLine(line); err != nil {
				return err
			}
		}
	}
}

// handleLine реагирует на те немногие строки сервера, которые нам важны.
func (c *Client) handleLine(line string) error {
	switch {
	case strings.HasPrefix(line, "PING"):
		c.mu.Lock()
		c.writeLocked("PONG" + strings.TrimPrefix(line, "PING"))
		c.mu.Unlock()
	case strings.Contains(line, " 001 "):
		c.log.Info("chat connected")
	case strings.Contains(line, "Login authentication failed"), strings.Contains(line, "Improperly formatted auth"):
		return errAuth
	case strings.Contains(line, " RECONNECT"):
		return errReconnect
	case strings.Contains(line, " ROOMSTATE #"):
		c.applyRoomState(line)
	default:
		if m, ok := parsePrivmsg(line); ok {
			select {
			case c.messages <- m:
			default: // читатель занят: чат работает по принципу best-effort
			}
		}
	}
	return nil
}

// applyRoomState сливает (возможно, частичный) ROOMSTATE с состоянием канала
// и публикует результат.
func (c *Client) applyRoomState(line string) {
	tags, rest, _ := strings.Cut(strings.TrimPrefix(line, "@"), " ")
	_, channel, _ := strings.Cut(rest, " ROOMSTATE #")
	c.roomMu.Lock()
	rs, ok := c.state[channel]
	if !ok {
		rs = RoomState{Channel: channel, FollowersOnly: -1}
	}
	for _, kv := range strings.Split(tags, ";") {
		k, v, _ := strings.Cut(kv, "=")
		n, _ := strconv.Atoi(v)
		switch k {
		case "emote-only":
			rs.EmoteOnly = n == 1
		case "followers-only":
			rs.FollowersOnly = n
		case "slow":
			rs.SlowSeconds = n
		case "subs-only":
			rs.SubsOnly = n == 1
		case "r9k":
			rs.UniqueChat = n == 1
		}
	}
	c.state[channel] = rs
	c.roomMu.Unlock()
	select {
	case c.rooms <- rs:
	default:
	}
}

// parsePrivmsg разбирает IRC-строку чата с тегами:
//
//	@color=#FF0000;display-name=Foo;... :foo!foo@foo.tmi.twitch.tv PRIVMSG #chan :hello
func parsePrivmsg(line string) (Message, bool) {
	var tags string
	if strings.HasPrefix(line, "@") {
		var ok bool
		if tags, line, ok = strings.Cut(line[1:], " "); !ok {
			return Message{}, false
		}
	}
	prefix, rest, ok := strings.Cut(line, " PRIVMSG #")
	if !ok {
		return Message{}, false
	}
	channel, text, ok := strings.Cut(rest, " :")
	if !ok {
		return Message{}, false
	}
	m := Message{Channel: channel, Text: text}
	// Сообщения "/me" приходят обёрнутыми в CTCP ACTION.
	if t, ok := strings.CutPrefix(text, "\x01ACTION "); ok {
		m.Text = strings.TrimSuffix(t, "\x01")
	}
	for _, kv := range strings.Split(tags, ";") {
		k, v, _ := strings.Cut(kv, "=")
		switch k {
		case "display-name":
			m.User = v
		case "color":
			m.Color = v
		case "first-msg":
			m.FirstMsg = v == "1"
		case "badges":
			for _, b := range strings.Split(v, ",") {
				if name, _, _ := strings.Cut(b, "/"); name != "" {
					m.Badges = append(m.Badges, name)
				}
			}
		}
	}
	if m.User == "" { // тегов нет: берём ник из ":nick!user@host"
		m.User, _, _ = strings.Cut(strings.TrimPrefix(prefix, ":"), "!")
	}
	return m, true
}

// writeLocked отправляет одну строку IRC, если соединение есть. Вызывающий держит c.mu.
func (c *Client) writeLocked(line string) {
	if c.conn == nil {
		return // применится при (пере)подключении
	}
	if !strings.HasPrefix(line, "PASS ") { // токен никогда не трассируется
		c.trace.Write("irc", "out", map[string]any{"line": line})
	}
	if err := ws.WriteText(c.conn, line+"\r\n"); err != nil {
		c.log.Warn("chat write failed", "err", err)
	}
}
