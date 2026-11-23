package twitch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	deviceURL   = "https://id.twitch.tv/oauth2/device"
	tokenURL    = "https://id.twitch.tv/oauth2/token"
	validateURL = "https://id.twitch.tv/oauth2/validate"
	// Scopes, запрашиваемые TV-приложением; именно chat:read позволяет нам делать JOIN в чаты.
	deviceScopes = "channel_read chat:read user_blocks_edit user_blocks_read user_follows_edit user_read"
)

// DeviceCode — начатый вход по device-code: пользователь открывает VerificationURI
// и вводит UserCode, а PollDeviceToken ждёт подтверждения.
type DeviceCode struct {
	UserCode        string
	VerificationURI string
	ExpiresAt       time.Time
	interval        time.Duration
	deviceCode      string
	deviceID        string
}

// StartDeviceLogin запускает OAuth device-code flow для заданного device id.
func (c *Client) StartDeviceLogin(ctx context.Context, deviceID string) (*DeviceCode, error) {
	form := url.Values{"client_id": {ClientID}, "scopes": {deviceScopes}}
	var resp struct {
		DeviceCode      string `json:"device_code"`
		UserCode        string `json:"user_code"`
		VerificationURI string `json:"verification_uri"`
		Interval        int    `json:"interval"`
		ExpiresIn       int    `json:"expires_in"`
	}
	status, err := c.postIDForm(ctx, deviceURL, form, deviceID, &resp)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("device login start: status %d", status)
	}
	if resp.VerificationURI == "" {
		resp.VerificationURI = "https://www.twitch.tv/activate"
	}
	c.log.Info("device login started", "user_code", resp.UserCode, "expires_in", resp.ExpiresIn)
	return &DeviceCode{
		UserCode:        resp.UserCode,
		VerificationURI: resp.VerificationURI,
		ExpiresAt:       time.Now().Add(time.Duration(resp.ExpiresIn) * time.Second),
		interval:        time.Duration(max(resp.Interval, 1)) * time.Second,
		deviceCode:      resp.DeviceCode,
		deviceID:        deviceID,
	}, nil
}

// PollDeviceToken блокируется, пока пользователь не подтвердит код (возвращает полный
// Auth), срок кода не истечёт или ctx не будет отменён.
func (c *Client) PollDeviceToken(ctx context.Context, dc *DeviceCode) (Auth, error) {
	form := url.Values{
		"client_id":   {ClientID},
		"device_code": {dc.deviceCode},
		"grant_type":  {"urn:ietf:params:oauth:grant-type:device_code"},
	}
	ticker := time.NewTicker(dc.interval)
	defer ticker.Stop()

	for time.Now().Before(dc.ExpiresAt) {
		select {
		case <-ctx.Done():
			return Auth{}, ctx.Err()
		case <-ticker.C:
		}
		var tok struct {
			AccessToken string `json:"access_token"`
		}
		status, err := c.postIDForm(ctx, tokenURL, form, dc.deviceID, &tok)
		if err != nil {
			return Auth{}, err
		}
		if status != http.StatusOK || tok.AccessToken == "" {
			continue // authorization_pending: пользователь ещё не подтвердил
		}
		auth, err := c.ValidateToken(ctx, tok.AccessToken)
		if err != nil {
			return Auth{}, err
		}
		auth.DeviceID = dc.deviceID
		c.log.Info("device login confirmed", "login", auth.Login, "user_id", auth.UserID)
		return auth, nil
	}
	return Auth{}, fmt.Errorf("device code expired before confirmation")
}

// ValidateToken спрашивает у Twitch, кому принадлежит токен. Возвращает ErrUnauthorized,
// если токен больше не действителен. DeviceID остаётся пустым.
func (c *Client) ValidateToken(ctx context.Context, token string) (Auth, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, validateURL, nil)
	if err != nil {
		return Auth{}, err
	}
	req.Header.Set("Authorization", "OAuth "+token)
	resp, err := c.send(req)
	if err != nil {
		return Auth{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		return Auth{}, ErrUnauthorized
	}
	if resp.StatusCode != http.StatusOK {
		return Auth{}, fmt.Errorf("validate token: status %d", resp.StatusCode)
	}
	var v struct {
		Login  string `json:"login"`
		UserID string `json:"user_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return Auth{}, err
	}
	return Auth{Token: token, UserID: v.UserID, Login: v.Login}, nil
}

// postIDForm отправляет форму на id.twitch.tv с заголовками TV-приложения и декодирует
// JSON-ответ в out (игнорируется при статусе не 200). Возвращает HTTP-статус.
func (c *Client) postIDForm(ctx context.Context, endpoint string, form url.Values, deviceID string, out any) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Client-Id", ClientID)
	req.Header.Set("Origin", "https://android.tv.twitch.tv")
	req.Header.Set("Referer", "https://android.tv.twitch.tv/")
	req.Header.Set("User-Agent", UserAgent)
	req.Header.Set("X-Device-Id", deviceID)

	resp, err := c.send(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, err
	}
	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(raw, out); err != nil {
			return resp.StatusCode, err
		}
	}
	return resp.StatusCode, nil
}
