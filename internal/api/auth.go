package api

import (
	"crypto/sha256"
	"crypto/subtle"
	"log/slog"
	"net/http"
	"time"
)

// Credentials — логин и пароль админки. Пустой пароль отключает авторизацию.
type Credentials struct {
	User     string
	Password string
}

// failedAuthDelay замедляет перебор пароля: каждая неудачная попытка ждёт
// (переменная, чтобы тесты не ждали).
var failedAuthDelay = time.Second

// basicAuth защищает next HTTP Basic авторизацией. Браузер сам показывает окно
// входа и потом прикладывает логин ко всем запросам этого адреса, включая
// EventSource (/live) и запросы Connect RPC, поэтому фронту ничего делать не нужно.
func basicAuth(next http.Handler, creds Credentials, log *slog.Logger) http.Handler {
	if creds.Password == "" {
		return next
	}
	// Сравниваем хэши в постоянном времени: длина и содержимое не утекают по таймингу.
	wantUser, wantPass := sha256.Sum256([]byte(creds.User)), sha256.Sum256([]byte(creds.Password))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		gotUser, gotPass := sha256.Sum256([]byte(user)), sha256.Sum256([]byte(pass))
		userOK := subtle.ConstantTimeCompare(gotUser[:], wantUser[:]) == 1
		passOK := subtle.ConstantTimeCompare(gotPass[:], wantPass[:]) == 1
		if ok && userOK && passOK {
			next.ServeHTTP(w, r)
			return
		}
		if ok { // логин введён, но неверный (запрос без логина — обычное первое обращение браузера)
			log.Warn("admin login failed", "user", user, "peer", r.RemoteAddr)
			time.Sleep(failedAuthDelay)
		}
		w.Header().Set("WWW-Authenticate", `Basic realm="Twitch Miner", charset="UTF-8"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	})
}
