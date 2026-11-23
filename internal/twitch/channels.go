package twitch

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Persisted query, используемые майнером (хеши взяты из веб-клиента Twitch).
var (
	opGetIDFromLogin      = operation{"GetIDFromLogin", "94e82a7b1e3c21e186daa73ee2afc4b8f23bade1fbbff6fe8ac133f50a2f58ca"}
	opStreamInfoOverlay   = operation{"VideoPlayerStreamInfoOverlayChannel", "e785b65ff71ad7b363b34878335f27dd9372869ad0c5740a130b9268bcdbe7e7"}
	opIsStreamLive        = operation{"WithIsStreamLiveQuery", "04e46329a6786ff3a81c01c50bfa5d725902507a0deb83b0edbf7abe7a3716ea"}
	opRewardList          = operation{"RewardList", "0b1471876d7647993731b9e3c6a13bf304c67fb31d07f06a945d42286ee377c4"}
	opChannelPointsCtx    = operation{"ChannelPointsContext", "374314de591e69925fce3ddc2bcf085796f56ebb8cad67a0daa3165c03adc345"}
	opClaimCommunityPoint = operation{"ClaimCommunityPoints", "46aaeebe02c99afdf4fc97c7c0cba964124bf6b0af229395f1f6d1feed05b3d0"}
	opClaimMoment         = operation{"CommunityMomentCallout_Claim", "e2d67415aead910f7f9ceb45a77b750a1e1d9622c936d832328a0689e054db62"}
)

// ChannelID преобразует логин канала в его числовой id в Twitch.
func (c *Client) ChannelID(ctx context.Context, login string) (string, error) {
	var data struct {
		User *struct {
			ID string `json:"id"`
		} `json:"user"`
	}
	login = strings.ToLower(strings.TrimSpace(login))
	if err := c.gql(ctx, opGetIDFromLogin, map[string]any{"login": login}, &data); err != nil {
		return "", err
	}
	if data.User == nil || data.User.ID == "" {
		return "", fmt.Errorf("%w: %q", ErrChannelNotFound, login)
	}
	return data.User.ID, nil
}

// Stream описывает идущую трансляцию.
type Stream struct {
	BroadcastID string
	StartedAt   time.Time
	Title       string
	GameID      string
	GameName    string
	Viewers     int
}

// streamInfoData — форма "data" для VideoPlayerStreamInfoOverlayChannel.
// Времени начала трансляции в ней нет; оно берётся из WithIsStreamLiveQuery.
type streamInfoData struct {
	User *struct {
		DisplayName string `json:"displayName"`
		Stream      *struct {
			ID           string `json:"id"`
			ViewersCount int    `json:"viewersCount"`
		} `json:"stream"`
		BroadcastSettings *struct {
			Title string `json:"title"`
			Game  *struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			} `json:"game"`
		} `json:"broadcastSettings"`
	} `json:"user"`
}

// streamStartData — форма "data" для WithIsStreamLiveQuery.
type streamStartData struct {
	User *struct {
		Stream *struct {
			CreatedAt string `json:"createdAt"`
		} `json:"stream"`
	} `json:"user"`
}

// ChannelRef идентифицирует канал для StreamInfos. ID может быть пустым, если он
// неизвестен (забаненный канал, отслеживаемый по логину); тогда время начала пропускается.
type ChannelRef struct {
	ID    string
	Login string
}

// StreamResult — результат для одного канала в StreamInfos.
type StreamResult struct {
	Login       string
	DisplayName string  // текущее отображаемое имя ("" если неизвестно)
	Stream      *Stream // nil, если канал офлайн
	Err         error   // ErrChannelNotFound, если канал забанен или переименован
}

// streamBatchSize: каналов на один HTTP-запрос (по две операции на каждый).
const streamBatchSize = 10

// StreamInfos проверяет много каналов минимальным числом HTTP-запросов,
// так же, как веб-клиент Twitch пакетирует свои запросы. Результаты идут в порядке refs.
func (c *Client) StreamInfos(ctx context.Context, refs []ChannelRef) []StreamResult {
	out := make([]StreamResult, 0, len(refs))
	for start := 0; start < len(refs); start += streamBatchSize {
		chunk := refs[start:min(start+streamBatchSize, len(refs))]
		var calls []call
		for _, r := range chunk {
			calls = append(calls, call{opStreamInfoOverlay, map[string]any{"channel": strings.ToLower(r.Login)}})
			if r.ID != "" {
				calls = append(calls, call{opIsStreamLive, map[string]any{"id": r.ID}})
			}
		}
		results, err := c.batch(ctx, calls)
		i := 0
		for _, r := range chunk {
			res := StreamResult{Login: r.Login, Err: err}
			var info streamInfoData
			var start streamStartData
			if err == nil {
				res.Err = decodeResult(opStreamInfoOverlay, results[i], &info)
				if r.ID != "" {
					_ = decodeResult(opIsStreamLive, results[i+1], &start) // необязательно: только время начала
				}
			}
			i++
			if r.ID != "" {
				i++
			}
			if res.Err == nil {
				res.Stream, res.Err = toStream(r.Login, info, start)
				if info.User != nil {
					res.DisplayName = info.User.DisplayName
				}
			}
			out = append(out, res)
		}
	}
	return out
}

// StreamInfo проверяет один канал (см. StreamInfos).
func (c *Client) StreamInfo(ctx context.Context, ref ChannelRef) StreamResult {
	return c.StreamInfos(ctx, []ChannelRef{ref})[0]
}

// toStream объединяет оба ответа: nil-трансляция, если канал офлайн,
// ErrChannelNotFound, если канал больше не существует.
func toStream(login string, info streamInfoData, start streamStartData) (*Stream, error) {
	if info.User == nil {
		return nil, fmt.Errorf("%w: %q", ErrChannelNotFound, login)
	}
	if info.User.Stream == nil {
		return nil, nil // офлайн
	}
	s := &Stream{BroadcastID: info.User.Stream.ID, Viewers: info.User.Stream.ViewersCount}
	if start.User != nil && start.User.Stream != nil {
		s.StartedAt = parseTime(start.User.Stream.CreatedAt)
	}
	if bs := info.User.BroadcastSettings; bs != nil {
		s.Title = bs.Title
		if bs.Game != nil {
			s.GameID, s.GameName = bs.Game.ID, bs.Game.Name
		}
	}
	return s, nil
}

// WatchStreak — состояние стрика просмотров аккаунта на канале.
type WatchStreak struct {
	Length     int       // текущая длина стрика, 0 если неизвестна
	AchievedAt time.Time // когда была выдана последняя веха стрика
	ExpiresAt  time.Time // когда стрик сгорит, если не посмотреть следующую трансляцию
}

// ClaimedFor сообщает, был ли стрик выдан во время трансляции, которая
// началась в streamStart (т. е. веха новее трансляции).
func (w WatchStreak) ClaimedFor(streamStart time.Time) bool {
	return !w.AchievedAt.IsZero() && !streamStart.IsZero() && w.AchievedAt.After(streamStart)
}

// WatchStreak получает веху стрика для канала.
func (c *Client) WatchStreak(ctx context.Context, channelID string) (WatchStreak, error) {
	var data struct {
		Channel *struct {
			Self *struct {
				WatchStreakMilestone *struct {
					WatchStreakMilestone *struct {
						Value                string `json:"value"`
						AchievementTimestamp string `json:"achievementTimestamp"`
					} `json:"watchStreakMilestone"`
					ExpiresAt string `json:"expiresAt"`
				} `json:"watchStreakMilestone"`
			} `json:"self"`
		} `json:"channel"`
	}
	vars := map[string]any{"channelID": channelID, "shouldIncludeAllSuspendedStreaks": false}
	if err := c.gql(ctx, opRewardList, vars, &data); err != nil {
		return WatchStreak{}, err
	}
	if data.Channel == nil || data.Channel.Self == nil || data.Channel.Self.WatchStreakMilestone == nil ||
		data.Channel.Self.WatchStreakMilestone.WatchStreakMilestone == nil {
		return WatchStreak{}, nil
	}
	outer := data.Channel.Self.WatchStreakMilestone
	m := outer.WatchStreakMilestone
	length, _ := strconv.Atoi(m.Value)
	return WatchStreak{Length: length, AchievedAt: parseTime(m.AchievementTimestamp), ExpiresAt: parseTime(outer.ExpiresAt)}, nil
}

// Points — состояние баллов канала у аккаунта на канале.
type Points struct {
	Balance     int
	ClaimID     string  // непустой, когда сундук ждёт клика
	Bonus       float64 // сумма активных множителей (подписка и т. п.), 0.2 = +20%
	StreakBonus []int   // баллы за каждый уровень стрика просмотров (настройка канала)
}

// ChannelPoints загружает баланс и ожидающий бонус, если он есть.
func (c *Client) ChannelPoints(ctx context.Context, login string) (Points, error) {
	var data struct {
		Community struct {
			Channel *struct {
				Self struct {
					CommunityPoints struct {
						Balance           int `json:"balance"`
						ActiveMultipliers []struct {
							Factor float64 `json:"factor"`
						} `json:"activeMultipliers"`
						AvailableClaim *struct {
							ID string `json:"id"`
						} `json:"availableClaim"`
					} `json:"communityPoints"`
				} `json:"self"`
				CommunityPointsSettings struct {
					Earning *struct {
						WatchStreakPoints []struct {
							Points int `json:"points"`
						} `json:"watchStreakPoints"`
					} `json:"earning"`
				} `json:"communityPointsSettings"`
			} `json:"channel"`
		} `json:"community"`
	}
	if err := c.gql(ctx, opChannelPointsCtx, map[string]any{"channelLogin": strings.ToLower(login)}, &data); err != nil {
		return Points{}, err
	}
	ch := data.Community.Channel
	if ch == nil {
		return Points{}, fmt.Errorf("channel points: channel %q missing", login)
	}
	p := Points{Balance: ch.Self.CommunityPoints.Balance}
	for _, m := range ch.Self.CommunityPoints.ActiveMultipliers {
		p.Bonus += m.Factor
	}
	if e := ch.CommunityPointsSettings.Earning; e != nil {
		for _, w := range e.WatchStreakPoints {
			p.StreakBonus = append(p.StreakBonus, w.Points)
		}
	}
	if ac := ch.Self.CommunityPoints.AvailableClaim; ac != nil {
		p.ClaimID = ac.ID
	}
	return p, nil
}

// ClaimBonus кликает по сундуку, заданному claimID.
func (c *Client) ClaimBonus(ctx context.Context, channelID, claimID string) error {
	var data struct {
		ClaimCommunityPoints *struct {
			Error *struct {
				Code string `json:"code"`
			} `json:"error"`
		} `json:"claimCommunityPoints"`
	}
	vars := map[string]any{"input": map[string]any{"channelID": channelID, "claimID": claimID}}
	if err := c.gql(ctx, opClaimCommunityPoint, vars, &data); err != nil {
		return err
	}
	if r := data.ClaimCommunityPoints; r != nil && r.Error != nil && r.Error.Code != "" {
		return fmt.Errorf("claim bonus: %s", r.Error.Code)
	}
	return nil
}

// ClaimMoment забирает community moment (коллекционный предмет, вроде значка).
func (c *Client) ClaimMoment(ctx context.Context, momentID string) error {
	var data struct {
		ClaimCommunityMoment *struct {
			Error *struct {
				Code string `json:"code"`
			} `json:"error"`
		} `json:"claimCommunityMoment"`
	}
	if err := c.gql(ctx, opClaimMoment, map[string]any{"input": map[string]any{"momentID": momentID}}, &data); err != nil {
		return err
	}
	if r := data.ClaimCommunityMoment; r != nil && r.Error != nil && r.Error.Code != "" {
		return fmt.Errorf("claim moment: %s", r.Error.Code)
	}
	return nil
}

func parseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(s))
	if err != nil {
		return time.Time{}
	}
	return t
}
