package pubsub

import "testing"

func TestParseMessage(t *testing.T) {
	cases := []struct {
		name, topic, msg string
		want             Event
	}{
		{"stream up", "video-playback-by-id.42", `{"type":"stream-up"}`, StreamUp{ChannelID: "42"}},
		{"stream down", "video-playback-by-id.42", `{"type":"stream-down"}`, StreamDown{ChannelID: "42"}},
		{"viewcount", "video-playback-by-id.42", `{"type":"viewcount","viewers":146}`, ViewCount{ChannelID: "42", Viewers: 146}},
		{"streak points", "community-points-user-v1.1",
			`{"type":"points-earned","data":{"point_gain":{"channel_id":"42","total_points":450,"reason_code":"WATCH_STREAK"},"balance":{"balance":1000}}}`,
			PointsEarned{ChannelID: "42", Points: 450, Reason: "WATCH_STREAK", Balance: 1000}},
		{"claim", "community-points-user-v1.1",
			`{"type":"claim-available","data":{"claim":{"id":"c1","channel_id":"42"}}}`,
			ClaimAvailable{ChannelID: "42", ClaimID: "c1"}},
		{"moment", "community-moments-channel-v1.42", `{"type":"active","data":{"moment_id":"m1"}}`,
			MomentAvailable{ChannelID: "42", MomentID: "m1"}},
		{"garbage", "x", `not json`, nil},
	}
	for _, tc := range cases {
		if got := ParseMessage(tc.topic, tc.msg); got != tc.want {
			t.Errorf("%s: got %#v want %#v", tc.name, got, tc.want)
		}
	}
}
