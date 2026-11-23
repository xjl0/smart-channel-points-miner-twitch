// Package ws — общие настройки WebSocket-соединений с Twitch (PubSub и чат)
// поверх github.com/coder/websocket.
package ws

import (
	"context"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

const (
	// readLimit — максимальный размер одного сообщения. По умолчанию в
	// coder/websocket это 32 КиБ, а события PubSub (например, hype train)
	// бывают больше — превышение рвёт соединение.
	readLimit = 1 << 20
	// writeTimeout ограничивает одну запись: зависшее соединение не должно
	// держать мьютекс вызывающего.
	writeTimeout = 10 * time.Second
)

// httpClient выполняет handshake. Транспорт стандартный, поэтому прокси
// берётся из HTTPS_PROXY/HTTP_PROXY (включая socks5://), как и у GQL-клиента.
// Без общего таймаута: соединение долгоживущее, его время жизни задаёт ctx.
var httpClient = &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}

// Dial открывает соединение. ctx ограничивает только установку соединения.
func Dial(ctx context.Context, url string) (*websocket.Conn, error) {
	conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPClient: httpClient})
	if err != nil {
		return nil, err
	}
	conn.SetReadLimit(readLimit)
	return conn, nil
}

// WriteJSON отправляет v одним текстовым сообщением в формате JSON.
func WriteJSON(conn *websocket.Conn, v any) error {
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()
	return wsjson.Write(ctx, conn, v)
}

// WriteText отправляет строку одним текстовым сообщением.
func WriteText(conn *websocket.Conn, s string) error {
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()
	return conn.Write(ctx, websocket.MessageText, []byte(s))
}
