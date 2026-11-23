package engine

import (
	"context"
	"errors"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	minerv1 "github.com/xjl0/smart-channel-points-miner-twitch/internal/gen/miner/v1"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/store/db"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/twitch"
)

// restoreAccount загружает сохранённый токен и проверяет его через Twitch.
func (e *Engine) restoreAccount() {
	acc, err := e.store.GetAccount(dbctx())
	if isNoRows(err) {
		e.log.Warn("twitch account not logged in: open the admin panel to log in")
		e.publishAccount()
		return
	}
	if err != nil {
		e.log.Error("load account", "err", err)
		return
	}
	e.validate(acc, 0)
}

// revalidate немедленно проверяет сохранённый токен (раз в час и всякий раз, когда Twitch
// отвечает 401). Twitch требует от клиентов проверять токены ежечасно, и именно так
// отозванный токен замечается, а не ломается молча месяцами.
func (e *Engine) revalidate() {
	if e.validating {
		return
	}
	acc, err := e.store.GetAccount(dbctx())
	if err != nil {
		return // вход не выполнен: проверять нечего
	}
	e.validate(acc, 0)
}

// validate проверяет сохранённый токен в фоне, повторяя попытки при сетевых
// ошибках (при запуске Twitch может быть просто недоступен).
func (e *Engine) validate(acc db.Account, delay time.Duration) {
	e.validating = true
	e.async(func(ctx context.Context) func() {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
		auth, err := e.tw.ValidateToken(ctx, acc.AccessToken)
		return func() {
			e.validating = false
			switch {
			case errors.Is(err, twitch.ErrUnauthorized):
				e.loseAuth(acc.Login)
			case err != nil:
				e.log.Warn("token validation failed, retrying in 1 min", "err", err)
				e.validate(acc, time.Minute)
			case e.authed && e.account.Login == auth.Login:
				e.log.Debug("twitch token still valid", "login", auth.Login)
			default:
				auth.DeviceID = acc.DeviceID
				e.setAuth(auth)
			}
		}
	})
}

// setAuth активирует аккаунт: клиенты получают учётные данные, каналы проверяются.
func (e *Engine) setAuth(a twitch.Auth) {
	e.tw.SetAuth(a)
	e.ps.SetAuth(a.Token, a.UserID)
	e.chat.SetAuth(a.Login, a.Token)
	if !e.netStarted {
		e.netStarted = true
		go e.ps.Run(e.ctx)
		go e.chat.Run(e.ctx)
	}
	e.authed = true
	e.account = &minerv1.Account{LoggedIn: true, Login: a.Login, UserId: a.UserID}
	e.log.Info("twitch account ready", "login", a.Login, "user_id", a.UserID)
	e.publishAccount()
	e.refreshAll("startup")
}

// publishAccount отправляет копию: опубликованные сообщения разделяются между
// подписчиками и после публикации не должны изменяться.
func (e *Engine) publishAccount() {
	e.publish(&minerv1.LiveMessage{Body: &minerv1.LiveMessage_AccountChanged{AccountChanged: &minerv1.AccountChanged{
		Account: proto.Clone(e.account).(*minerv1.Account),
	}}})
}

// StartLogin начинает вход по device-code. Возвращённый код также отправляется в
// админ-панель; подтверждение ожидается в фоне.
func (e *Engine) StartLogin(ctx context.Context) (*minerv1.DeviceLogin, error) {
	// Сохраняем device id между входами: новый при каждом входе выглядит странно.
	deviceID := twitch.NewDeviceID()
	if acc, err := e.store.GetAccount(dbctx()); err == nil && acc.DeviceID != "" {
		deviceID = acc.DeviceID
	}
	dc, err := e.tw.StartDeviceLogin(ctx, deviceID)
	if err != nil {
		return nil, err
	}
	pending := &minerv1.DeviceLogin{
		UserCode: dc.UserCode, VerificationUri: dc.VerificationURI, ExpiresAt: timestamppb.New(dc.ExpiresAt),
	}

	err = e.call(func() error {
		if e.cancelPoll != nil {
			e.cancelPoll() // более новый вход заменяет старый
		}
		pollCtx, cancel := context.WithCancel(e.ctx)
		e.cancelPoll = cancel
		e.account.PendingLogin = pending
		e.publishAccount()

		go func() {
			auth, err := e.tw.PollDeviceToken(pollCtx, dc)
			if errors.Is(err, context.Canceled) {
				return
			}
			e.do(func() { e.finishLogin(dc, auth, err) })
		}()
		return nil
	})
	return pending, err
}

// finishLogin сохраняет подтверждённый аккаунт (или сообщает об ошибке).
func (e *Engine) finishLogin(dc *twitch.DeviceCode, auth twitch.Auth, err error) {
	if e.account.PendingLogin == nil || e.account.PendingLogin.UserCode != dc.UserCode {
		return // вытеснен более новым входом
	}
	e.account.PendingLogin = nil
	if err != nil {
		e.log.Warn("device login failed", "err", err)
		e.publishAccount()
		return
	}
	if err := e.store.SaveAccount(dbctx(), db.SaveAccountParams{
		Login: auth.Login, UserID: auth.UserID, AccessToken: auth.Token, DeviceID: auth.DeviceID, UpdatedAt: time.Now().UTC(),
	}); err != nil {
		e.log.Error("save account", "err", err)
	}
	e.setAuth(auth)
}

// loseAuth реагирует на отклонённый токен: прекращает просмотр и запрашивает новый
// вход в админ-панели. Строка аккаунта (и её device id) сохраняется, чтобы
// следующий вход выглядел как с того же устройства.
func (e *Engine) loseAuth(login string) {
	e.log.Error("twitch token is no longer valid: log in again from the admin panel", "login", login)
	e.authed = false
	e.tw.SetAuth(twitch.Auth{})
	e.applySlots(nil)
	e.account = &minerv1.Account{}
	e.publishAccount()
}

// Logout забывает токен и прекращает просмотр.
func (e *Engine) Logout() error {
	return e.call(func() error {
		if err := e.store.DeleteAccount(dbctx()); err != nil {
			return err
		}
		e.authed = false
		e.tw.SetAuth(twitch.Auth{})
		e.applySlots(nil)
		e.account = &minerv1.Account{}
		e.log.Info("twitch account logged out")
		e.publishAccount()
		return nil
	})
}
