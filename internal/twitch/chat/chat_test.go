package chat

import (
	"io"
	"log/slog"
	"reflect"
	"testing"
)

func TestParsePrivmsg(t *testing.T) {
	cases := []struct {
		line string
		want Message
		ok   bool
	}{
		{"@badge-info=;color=#1E90FF;display-name=Viewer1;mod=0 :viewer1!viewer1@viewer1.tmi.twitch.tv PRIVMSG #channel_a :привет всем",
			Message{Channel: "channel_a", User: "Viewer1", Color: "#1E90FF", Text: "привет всем"}, true},
		{":nick!nick@nick.tmi.twitch.tv PRIVMSG #chan :no tags here",
			Message{Channel: "chan", User: "nick", Text: "no tags here"}, true},
		{"@display-name=Me :me!me@me PRIVMSG #chan :\x01ACTION waves\x01",
			Message{Channel: "chan", User: "Me", Text: "waves"}, true},
		{"@badges=moderator/1,subscriber/12;display-name=Mod;first-msg=1 :mod!mod@mod PRIVMSG #chan :hi",
			Message{Channel: "chan", User: "Mod", Text: "hi", Badges: []string{"moderator", "subscriber"}, FirstMsg: true}, true},
		{":tmi.twitch.tv 001 me :Welcome, GLHF!", Message{}, false},
	}
	for _, tc := range cases {
		got, ok := parsePrivmsg(tc.line)
		if ok != tc.ok || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("parsePrivmsg(%q) = %+v, %v; want %+v, %v", tc.line, got, ok, tc.want, tc.ok)
		}
	}
}

func TestRoomStateMergesPartialUpdates(t *testing.T) {
	c := New(slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	c.applyRoomState("@emote-only=0;followers-only=10;r9k=0;room-id=1;slow=0;subs-only=0 :tmi.twitch.tv ROOMSTATE #chan")
	c.applyRoomState("@room-id=1;slow=30 :tmi.twitch.tv ROOMSTATE #chan") // частичный: изменился только slow
	<-c.RoomStates()
	got := <-c.RoomStates()
	want := RoomState{Channel: "chan", FollowersOnly: 10, SlowSeconds: 30}
	if got != want {
		t.Fatalf("room state = %+v, want %+v", got, want)
	}
}
