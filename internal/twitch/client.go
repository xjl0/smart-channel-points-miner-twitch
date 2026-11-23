// Package twitch — минимальный клиент для эндпоинтов Twitch, которые нужны
// майнеру: вход по device-code, несколько GQL persisted query, событие spade
// "minute-watched" и получение бонуса баллов канала.
//
// Клиент последовательно выдаёт себя за веб-приложение Android TV (один Client-ID,
// один User-Agent, один device id, сохраняемый между перезапусками) и пропускает все
// вызовы GQL через ограничитель частоты, чтобы аккаунт никогда не давал всплесков запросов.
package twitch

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/time/rate"

	"github.com/xjl0/smart-channel-points-miner-twitch/internal/trace"
)

const (
	// ClientID приложения Twitch для Android TV; к нему привязан device-code flow.
	ClientID = "ue6666qo983tsx6so1t0vnawi233wa"
	// UserAgent отправляется с каждым запросом; соответствует TV-клиенту выше.
	UserAgent = "Mozilla/5.0 (Linux; Android 7.1; Smart Box C1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"

	gqlURL     = "https://gql.twitch.tv/gql"
	twitchURL  = "https://www.twitch.tv"
	versionTTL = 10 * time.Hour
	// Запасной build id, используемый, пока настоящий не получен со страницы twitch.tv.
	defaultClientVersion = "ef928475-9403-42f2-8a34-55784bd08e16"
)

var (
	// ErrUnauthorized означает, что сохранённый OAuth-токен отсутствует или отклонён.
	ErrUnauthorized = errors.New("twitch: unauthorized")
	// ErrChannelNotFound означает, что пользователя Twitch с таким логином нет.
	ErrChannelNotFound = errors.New("twitch: channel not found")
)

// Auth — идентификационные данные вошедшего аккаунта.
type Auth struct {
	Token    string
	UserID   string
	Login    string
	DeviceID string
}

// Client общается с Twitch от имени одного аккаунта. Безопасен для конкурентного использования.
type Client struct {
	http    *http.Client
	log     *slog.Logger
	trace   *trace.Tracer
	limiter *rate.Limiter
	session string // Client-Session-Id, случайный для каждого процесса, как вкладка браузера

	mu   sync.RWMutex
	auth Auth

	gqlURL string // эндпоинт gql (переопределяется через WithEndpoints)

	versionMu sync.Mutex
	version   string
	versionAt time.Time

	spadeMu     sync.Mutex
	spadeURL    string
	spadePinned bool // адрес задан через WithEndpoints: не сбрасываем и не ищем на twitch.tv
	spadeAt     time.Time
}

// Option настраивает Client.
type Option func(*Client)

// WithEndpoints фиксирует эндпоинты GQL и spade (используется тестами с фейковым
// Twitch). Также отключает получение build id и URL spade со страницы twitch.tv
// и снимает ограничение частоты, чтобы ничего не уходило за пределы теста.
func WithEndpoints(gqlURL, spadeURL string) Option {
	return func(c *Client) {
		c.gqlURL = gqlURL
		c.spadeURL, c.spadeAt, c.spadePinned = spadeURL, time.Now().Add(100*365*24*time.Hour), true
		c.versionAt = time.Now().Add(100 * 365 * 24 * time.Hour)
		c.limiter = rate.NewLimiter(rate.Inf, 1)
	}
}

// New создаёт клиент. Перед аутентифицированными вызовами вызовите SetAuth.
// tr (может быть nil) получает каждый обмен GQL и каждое событие spade.
func New(log *slog.Logger, tr *trace.Tracer, opts ...Option) *Client {
	c := &Client{
		http:  newHTTPClient(),
		log:   log.With("component", "twitch"),
		trace: tr,
		// В среднем не более одного вызова GQL за ~700 мс, небольшие всплески допустимы.
		limiter: rate.NewLimiter(rate.Every(700*time.Millisecond), 3),
		session: randomHex(16),
		version: defaultClientVersion,
		gqlURL:  gqlURL,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// SetAuth устанавливает (или заменяет) идентификационные данные аккаунта.
func (c *Client) SetAuth(a Auth) {
	c.mu.Lock()
	c.auth = a
	c.mu.Unlock()
}

// Auth возвращает текущие идентификационные данные аккаунта.
func (c *Client) Auth() Auth {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.auth
}

// operation — GQL persisted query, известный по имени и хешу sha256.
type operation struct {
	name string
	hash string
}

type gqlRequest struct {
	OperationName string         `json:"operationName"`
	Variables     map[string]any `json:"variables,omitempty"`
	Extensions    struct {
		PersistedQuery struct {
			Version    int    `json:"version"`
			Sha256Hash string `json:"sha256Hash"`
		} `json:"persistedQuery"`
	} `json:"extensions"`
}

type gqlError struct {
	Message string `json:"message"`
}

// gqlResult — один элемент ответа GQL.
type gqlResult struct {
	Data   json.RawMessage `json:"data"`
	Errors []gqlError      `json:"errors"`
}

// call — один вызов persisted query внутри пакета.
type call struct {
	op   operation
	vars map[string]any
}

// gql выполняет один persisted query и декодирует его "data" в out (указатель
// на структуру, соответствующую форме данных; nil, чтобы проигнорировать).
func (c *Client) gql(ctx context.Context, op operation, vars map[string]any, out any) error {
	res, err := c.batch(ctx, []call{{op, vars}})
	if err != nil {
		return err
	}
	return decodeResult(op, res[0], out)
}

// batch отправляет несколько persisted query ОДНИМ HTTP-запросом, точно как это
// делает веб-клиент Twitch (JSON-массив на входе, JSON-массив на выходе). Это стоит
// одного токена ограничителя частоты, поэтому опрос многих каналов — один запрос.
func (c *Client) batch(ctx context.Context, calls []call) ([]gqlResult, error) {
	if err := c.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	auth := c.Auth()
	if auth.Token == "" {
		return nil, ErrUnauthorized
	}

	reqs := make([]gqlRequest, len(calls))
	for i, cl := range calls {
		reqs[i] = gqlRequest{OperationName: cl.op.name, Variables: cl.vars}
		reqs[i].Extensions.PersistedQuery.Version = 1
		reqs[i].Extensions.PersistedQuery.Sha256Hash = cl.op.hash
	}
	body, err := json.Marshal(reqs)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.gqlURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Authorization", "OAuth "+auth.Token)
	httpReq.Header.Set("Client-Id", ClientID)
	httpReq.Header.Set("Client-Session-Id", c.session)
	httpReq.Header.Set("Client-Version", c.clientVersion(ctx))
	httpReq.Header.Set("User-Agent", UserAgent)
	httpReq.Header.Set("X-Device-Id", auth.DeviceID)
	httpReq.Header.Set("Content-Type", "application/json")
	// Мутация получения бонуса дополнительно проверяет cookie веб-сессии.
	httpReq.Header.Set("Cookie", fmt.Sprintf("auth-token=%s; persistent=%s", auth.Token, auth.UserID))

	name := calls[0].op.name
	start := time.Now()
	resp, err := c.send(httpReq)
	if err != nil {
		return nil, fmt.Errorf("gql %s: %w", name, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("gql %s: read body: %w", name, err)
	}
	c.log.Debug("gql request", "op", name, "ops", len(calls), "status", resp.StatusCode, "took", time.Since(start).Round(time.Millisecond))
	if c.trace != nil {
		ops := make([]map[string]any, len(calls))
		for i, cl := range calls {
			ops[i] = map[string]any{"op": cl.op.name, "vars": cl.vars}
		}
		c.trace.Write("gql", "exchange", map[string]any{
			"request": ops, "status": resp.StatusCode, "took_ms": time.Since(start).Milliseconds(), "response": trace.JSON(raw),
		})
	}

	if resp.StatusCode == http.StatusUnauthorized {
		return nil, ErrUnauthorized
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("gql %s: status %d: %s", name, resp.StatusCode, truncate(raw, 200))
	}
	var results []gqlResult
	if err := json.Unmarshal(raw, &results); err != nil {
		return nil, fmt.Errorf("gql %s: decode: %w", name, err)
	}
	if len(results) != len(calls) {
		return nil, fmt.Errorf("gql %s: %d results for %d operations", name, len(results), len(calls))
	}
	return results, nil
}

// decodeResult превращает один элемент пакета в out или ошибку.
func decodeResult(op operation, r gqlResult, out any) error {
	if len(r.Errors) > 0 {
		return fmt.Errorf("gql %s: %s", op.name, r.Errors[0].Message)
	}
	if out == nil || len(r.Data) == 0 {
		return nil
	}
	return json.Unmarshal(r.Data, out)
}

var twilightBuildRe = regexp.MustCompile(`window\.__twilightBuildID\s*=\s*"([0-9a-fA-F\-]{36})"`)

// clientVersion возвращает текущий build id веб-версии Twitch, обновляя его с
// главной страницы twitch.tv не чаще одного раза за versionTTL.
func (c *Client) clientVersion(ctx context.Context) string {
	c.versionMu.Lock()
	defer c.versionMu.Unlock()
	if time.Since(c.versionAt) < versionTTL {
		return c.version
	}
	c.versionAt = time.Now() // и при неудаче тоже: не повторять при каждом вызове

	body, err := c.getPage(ctx, twitchURL)
	if err != nil {
		c.log.Warn("client version refresh failed, keeping old one", "err", err)
		return c.version
	}
	if m := twilightBuildRe.FindSubmatch(body); m != nil {
		c.version = string(m[1])
		c.log.Debug("client version refreshed", "version", c.version)
	}
	return c.version
}

// getPage выполняет обычный GET с User-Agent клиента.
func (c *Client) getPage(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", UserAgent)
	resp, err := c.send(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

// idleConnTimeout: простаивающие соединения закрываем сами раньше, чем это
// сделает Twitch (около минуты), — иначе запрос уходит в уже закрытое сервером
// соединение и падает с EOF. Особенно часто это бывало с minute-watched,
// которые идут как раз раз в минуту, и после сна/обрыва сети.
const idleConnTimeout = 30 * time.Second

func newHTTPClient() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.IdleConnTimeout = idleConnTimeout
	return &http.Client{Timeout: 30 * time.Second, Transport: t}
}

// send выполняет запрос и один раз повторяет его, если соединение оборвалось
// (EOF, сброс). Так переживается и протухшее соединение из пула после сна или
// обрыва сети. Тело запроса пересоздаётся через GetBody (его заполняет
// http.NewRequest для strings/bytes-тел).
func (c *Client) send(req *http.Request) (*http.Response, error) {
	resp, err := c.http.Do(req)
	if err == nil || req.Context().Err() != nil || !brokenConn(err) {
		return resp, err
	}
	if req.Body != nil {
		if req.GetBody == nil {
			return nil, err
		}
		body, gerr := req.GetBody()
		if gerr != nil {
			return nil, err
		}
		req.Body = body
	}
	c.log.Debug("retrying after a broken connection", "host", req.URL.Host, "err", err)
	return c.http.Do(req)
}

// brokenConn сообщает, что запрос упал из-за оборванного соединения, а не из-за ответа.
func brokenConn(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET) ||
		strings.Contains(err.Error(), "connection reset") || strings.Contains(err.Error(), "broken pipe")
}

// NewDeviceID возвращает случайный device id в формате, который использует веб-версия Twitch.
func NewDeviceID() string { return randomHex(16) }

func randomHex(nBytes int) string {
	b := make([]byte, nBytes)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n]) + "…"
	}
	return string(b)
}
