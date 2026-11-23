package engine

import (
	"log/slog"

	"github.com/xjl0/smart-channel-points-miner-twitch/internal/twitch/chat"

	minerv1 "github.com/xjl0/smart-channel-points-miner-twitch/internal/gen/miner/v1"
	"github.com/xjl0/smart-channel-points-miner-twitch/internal/twitch/pubsub"
)

// onPubSub обрабатывает одно событие PubSub в горутине Run.
func (e *Engine) onPubSub(ev pubsub.Event) {
	switch ev := ev.(type) {
	case pubsub.Reconnected:
		// Пока PubSub лежал, начала и концы стримов прошли мимо нас.
		e.refreshAll("pubsub reconnected")

	case pubsub.StreamUp:
		if ch := e.channels[ev.ChannelID]; ch != nil {
			e.logger(ch).Debug("pubsub: stream up")              // следом придёт «channel is live»
			e.refresh(ev.ChannelID, e.timing.StreamUpRetries...) // GQL может отставать от PubSub
		}

	case pubsub.StreamDown:
		if ch := e.channels[ev.ChannelID]; ch != nil && ch.online {
			e.goOffline(ch, "pubsub")
			e.reconcile()
		}

	case pubsub.PointsEarned:
		ch := e.channels[ev.ChannelID]
		if ch == nil {
			return // канал, который мы не отслеживаем (например, смотрят на другом устройстве)
		}
		level := slog.LevelInfo // сундуки, бонусы за стрик, рейды…
		if ev.Reason == "WATCH" {
			level = slog.LevelDebug // рутинные +10 каждые 5 минут; видны в ленте панели
		}
		e.logger(ch).Log(e.ctx, level, "points earned", "points", ev.Points, "reason", ev.Reason, "balance", ev.Balance)
		e.setPoints(ch, int64(ev.Balance))
		e.publish(&minerv1.LiveMessage{Body: &minerv1.LiveMessage_PointsGained{PointsGained: &minerv1.PointsGained{
			ChannelId: ch.ID, Points: int32(ev.Points), Reason: ev.Reason, Balance: int64(ev.Balance),
		}}})
		if ev.Reason == "WATCH_STREAK" {
			e.markStreakClaimed(ch, 0, "pubsub", "bonus_points", ev.Points)
			e.checkStreak(ch) // PubSub не передаёт новую длину / срок истечения
		}

	case pubsub.ViewCount:
		if ch := e.channels[ev.ChannelID]; ch != nil && ch.viewers != ev.Viewers {
			ch.viewers = ev.Viewers
			// Эфемерное: приходит каждые ~30 с на канал, seq на него тратить не стоит.
			e.bus.Broadcast(&minerv1.LiveMessage{Body: &minerv1.LiveMessage_ViewersChanged{
				ViewersChanged: &minerv1.ViewersChanged{ChannelId: ch.ID, Viewers: int32(ev.Viewers)},
			}})
		}

	case pubsub.MomentAvailable:
		if ch := e.channels[ev.ChannelID]; ch != nil {
			e.claimMoment(ch, ev.MomentID)
		}

	case pubsub.ClaimClaimed:
		if ch := e.channels[ev.ChannelID]; ch != nil {
			ch.takenClaimID = ev.ClaimID
		}

	case pubsub.ClaimAvailable:
		if ch := e.channels[ev.ChannelID]; ch != nil {
			e.claimBonus(ch, ev.ClaimID)
		}
	}
}

// onRoomState сохраняет ограничения чата канала, в чате которого мы находимся.
func (e *Engine) onRoomState(rs chat.RoomState) {
	for _, ch := range e.channels {
		if ch.Login != rs.Channel {
			continue
		}
		ch.chatModes = &minerv1.ChatModes{
			EmoteOnly:            rs.EmoteOnly,
			FollowersOnlyMinutes: int32(rs.FollowersOnly),
			SlowSeconds:          int32(rs.SlowSeconds),
			SubsOnly:             rs.SubsOnly,
			UniqueChat:           rs.UniqueChat,
		}
		e.logger(ch).Debug("chat modes", "followers_only", rs.FollowersOnly, "slow", rs.SlowSeconds, "subs_only", rs.SubsOnly)
		e.publishChannel(ch)
		return
	}
}
