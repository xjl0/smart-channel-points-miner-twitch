package twitch

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// URL spade глобален для всего сайта (не зависит от канала), поэтому он
// определяется один раз и кешируется, а не извлекается для каждой трансляции.
const spadeTTL = 6 * time.Hour

var (
	settingsJSRe = regexp.MustCompile(`(https://static\.twitchcdn\.net/config/settings.*?\.js|https://assets\.twitch\.tv/config/settings.*?\.js)`)
	spadeURLRe   = regexp.MustCompile(`"spade_url":"(.*?)"`)
)

// MinuteWatched определяет трансляцию, для которой отправляется событие просмотра.
type MinuteWatched struct {
	ChannelID    string
	ChannelLogin string
	BroadcastID  string
	GameID       string
	GameName     string
}

// SendMinuteWatched сообщает аналитике Twitch (spade) об одной просмотренной минуте.
// Именно благодаря этому Twitch считает нас зрителем и выдаёт награды за просмотр.
func (c *Client) SendMinuteWatched(ctx context.Context, mw MinuteWatched) error {
	spade, err := c.resolveSpadeURL(ctx, mw.ChannelLogin)
	if err != nil {
		return fmt.Errorf("resolve spade url: %w", err)
	}
	auth := c.Auth()

	props := map[string]any{
		"channel_id":   mw.ChannelID,
		"channel":      mw.ChannelLogin,
		"broadcast_id": mw.BroadcastID,
		"user_id":      auth.UserID,
		"player":       "site",
		"live":         true,
	}
	if mw.GameName != "" {
		props["game"] = mw.GameName
		props["game_id"] = mw.GameID
	}
	payload, err := json.Marshal([]map[string]any{{"event": "minute-watched", "properties": props}})
	if err != nil {
		return err
	}
	form := url.Values{"data": {base64.StdEncoding.EncodeToString(payload)}}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, spade, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", UserAgent)
	resp, err := c.send(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	c.trace.Write("spade", "minute-watched", map[string]any{"properties": props, "status": resp.StatusCode})
	if resp.StatusCode != http.StatusNoContent {
		c.invalidateSpade() // возможно, эндпоинт сменился
		return fmt.Errorf("minute-watched: status %d", resp.StatusCode)
	}
	c.log.Debug("minute-watched sent", "channel", mw.ChannelLogin, "broadcast", mw.BroadcastID)
	return nil
}

// resolveSpadeURL возвращает закешированный эндпоинт spade, извлекая его со
// страницы канала и скрипта настроек, если кеш пуст или устарел.
func (c *Client) resolveSpadeURL(ctx context.Context, login string) (string, error) {
	c.spadeMu.Lock()
	defer c.spadeMu.Unlock()
	if c.spadeURL != "" && time.Since(c.spadeAt) < spadeTTL {
		return c.spadeURL, nil
	}

	page, err := c.getPage(ctx, twitchURL+"/"+login)
	if err != nil {
		return "", err
	}
	m := settingsJSRe.FindSubmatch(page)
	if m == nil {
		return "", errors.New("settings script not found on channel page")
	}
	settings, err := c.getPage(ctx, string(m[1]))
	if err != nil {
		return "", err
	}
	s := spadeURLRe.FindSubmatch(settings)
	if s == nil {
		return "", errors.New("spade_url not found in settings script")
	}
	c.spadeURL, c.spadeAt = string(s[1]), time.Now()
	c.log.Debug("spade url resolved", "url", c.spadeURL)
	return c.spadeURL, nil
}

func (c *Client) invalidateSpade() {
	c.spadeMu.Lock()
	if !c.spadePinned {
		c.spadeURL = ""
	}
	c.spadeMu.Unlock()
}
