package live

import (
	"slices"

	"google.golang.org/protobuf/proto"

	minerv1 "github.com/xjl0/smart-channel-points-miner-twitch/internal/gen/miner/v1"
)

// apply сворачивает одно сообщение в снапшот. Админ-панель реализует ту же
// функцию на TypeScript (web/src/live/reducer.ts); держите их синхронными.
//
// Сущности клонируются на входе: опубликованные сообщения разделяются с
// подписчиками, поэтому состояние никогда не должно ссылаться на них (и потом изменять).
func apply(s *minerv1.Snapshot, msg *minerv1.LiveMessage) {
	switch m := msg.Body.(type) {
	case *minerv1.LiveMessage_ChannelUpdated:
		ch := proto.Clone(m.ChannelUpdated.Channel).(*minerv1.Channel)
		if i := channelIndex(s, ch.Id); i >= 0 {
			s.Channels[i] = ch
		} else {
			s.Channels = append(s.Channels, ch)
		}
	case *minerv1.LiveMessage_ChannelRemoved:
		if i := channelIndex(s, m.ChannelRemoved.ChannelId); i >= 0 {
			s.Channels = slices.Delete(s.Channels, i, i+1)
		}
	case *minerv1.LiveMessage_SlotsChanged:
		s.Slots = cloneAll(m.SlotsChanged.Slots)
	case *minerv1.LiveMessage_QueueChanged:
		s.StreakQueue = slices.Clone(m.QueueChanged.ChannelIds)
	case *minerv1.LiveMessage_SettingsChanged:
		s.Settings = proto.Clone(m.SettingsChanged.Settings).(*minerv1.Settings)
		s.DefaultSettings = proto.Clone(m.SettingsChanged.Defaults).(*minerv1.Settings)
	case *minerv1.LiveMessage_AccountChanged:
		s.Account = proto.Clone(m.AccountChanged.Account).(*minerv1.Account)
	case *minerv1.LiveMessage_WatchTick:
		if i := channelIndex(s, m.WatchTick.ChannelId); i >= 0 {
			s.Channels[i].WatchedSeconds = m.WatchTick.WatchedSeconds
		}
	case *minerv1.LiveMessage_PointsGained:
		if i := channelIndex(s, m.PointsGained.ChannelId); i >= 0 {
			s.Channels[i].Points = m.PointsGained.Balance
		}
	}
}

func channelIndex(s *minerv1.Snapshot, id string) int {
	return slices.IndexFunc(s.Channels, func(c *minerv1.Channel) bool { return c.Id == id })
}

func cloneAll[T proto.Message](in []T) []T {
	out := make([]T, len(in))
	for i, v := range in {
		out[i] = proto.Clone(v).(T)
	}
	return out
}
