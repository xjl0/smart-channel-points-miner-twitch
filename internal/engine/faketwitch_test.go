package engine

// Фейковый Twitch для сценарных тестов. Отдаёт реальные payload'ы, записанные в
// testdata/twitch (вырезаны из живой трассы скриптом scripts/extract_fixtures.py),
// подменяя только поля, которыми управляет сценарий, и записывает каждый запрос
// майнера. События minute-watched (spade) управляют моделью стрика: канал может
// выдать стрик после N просмотренных минут текущей трансляции, как это делает Twitch.

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeChannel — управляемое сценарием состояние одного канала Twitch.
type fakeChannel struct {
	id, login string
	live      bool
	banned    bool
	broadcast string
	startedAt time.Time
	viewers   int
	balance   int
	claimID   string // ожидающий сундук, "" если нет

	streakLength      int
	achievedAt        time.Time // последний этап стрика, нулевое значение = никогда
	streakAfterMinute int       // выдать стрик после стольких событий minute-watched (0 = никогда)
	minutes           int       // событий minute-watched получено за текущую трансляцию
}

// fakeTwitch — httptest-сервер, обслуживающий /gql (пакетами) и /spade.
type fakeTwitch struct {
	t     *testing.T
	srv   *httptest.Server
	clock func() time.Time // часы сценария, проставляют время получения стрика

	mu       sync.Mutex
	channels map[string]*fakeChannel // по логину
	byID     map[string]*fakeChannel
	httpGQL  int            // HTTP-запросы к /gql
	ops      map[string]int // GQL-операции по имени
	requests map[string]int // HTTP-запросы, содержавшие операцию (пакетирование)
	beacons  []string       // "login/broadcast" на каждое событие minute-watched
	claims   []string       // id сундуков, которые мы пытались нажать
	spadeErr bool           // отвечать на minute-watched ошибкой 500
}

func newFakeTwitch(t *testing.T, clock func() time.Time) *fakeTwitch {
	f := &fakeTwitch{t: t, clock: clock, channels: map[string]*fakeChannel{}, byID: map[string]*fakeChannel{}, ops: map[string]int{}, requests: map[string]int{}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /gql", f.gql)
	mux.HandleFunc("POST /spade", f.spade)
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

// channel добавляет (или возвращает) канал; id имеют вид "id-<login>".
func (f *fakeTwitch) channel(login string) *fakeChannel {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c := f.channels[login]; c != nil {
		return c
	}
	c := &fakeChannel{id: "id-" + login, login: login, balance: 1000, viewers: 50}
	f.channels[login], f.byID[c.id] = c, c
	return c
}

// goLive запускает новую трансляцию, начавшуюся за `ago` до текущего момента.
func (f *fakeTwitch) goLive(login, broadcast string, ago time.Duration) *fakeChannel {
	c := f.channel(login)
	f.mu.Lock()
	defer f.mu.Unlock()
	c.live, c.broadcast, c.startedAt, c.minutes = true, broadcast, f.clock().Add(-ago).UTC().Truncate(time.Second), 0
	return c
}

func (f *fakeTwitch) goOffline(login string) {
	c := f.channel(login)
	f.mu.Lock()
	c.live = false
	f.mu.Unlock()
}

// set изменяет канал под блокировкой.
func (f *fakeTwitch) set(login string, fn func(c *fakeChannel)) {
	c := f.channel(login)
	f.mu.Lock()
	fn(c)
	f.mu.Unlock()
}

// snapshot копирует то, что майнер успел отправить.
func (f *fakeTwitch) snapshot() (ops, requests map[string]int, beacons, claims []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := func(m map[string]int) map[string]int {
		out := map[string]int{}
		for k, v := range m {
			out[k] = v
		}
		return out
	}
	return cp(f.ops), cp(f.requests), append([]string(nil), f.beacons...), append([]string(nil), f.claims...)
}

// ---- /gql ----

func (f *fakeTwitch) gql(w http.ResponseWriter, r *http.Request) {
	var reqs []struct {
		OperationName string         `json:"operationName"`
		Variables     map[string]any `json:"variables"`
	}
	if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.httpGQL++
	out := make([]map[string]any, len(reqs))
	seen := map[string]bool{}
	for i, q := range reqs {
		f.ops[q.OperationName]++
		if !seen[q.OperationName] {
			seen[q.OperationName] = true
			f.requests[q.OperationName]++
		}
		out[i] = map[string]any{"data": f.answer(q.OperationName, q.Variables)}
	}
	_ = json.NewEncoder(w).Encode(out)
}

// answer собирает "data" одной операции из записанной фикстуры. Вызывающий держит f.mu.
func (f *fakeTwitch) answer(op string, vars map[string]any) any {
	str := func(k string) string { s, _ := vars[k].(string); return s }
	switch op {
	case "GetIDFromLogin":
		if c := f.channels[str("login")]; c != nil && !c.banned {
			return map[string]any{"user": map[string]any{"id": c.id}}
		}
		return map[string]any{"user": nil}

	case "VideoPlayerStreamInfoOverlayChannel":
		c := f.channels[str("channel")]
		if c == nil || c.banned {
			return map[string]any{"user": nil}
		}
		d := fixture(f.t, "gql_stream_info")
		u := d["user"].(map[string]any)
		u["id"], u["login"], u["displayName"] = c.id, c.login, strings.ToUpper(c.login[:1])+c.login[1:]
		if !c.live {
			u["stream"] = nil
		} else {
			s := u["stream"].(map[string]any)
			s["id"], s["viewersCount"] = c.broadcast, c.viewers
		}
		return d

	case "WithIsStreamLiveQuery":
		c := f.byID[str("id")]
		if c == nil || c.banned {
			return map[string]any{"user": nil}
		}
		d := fixture(f.t, "gql_stream_live")
		u := d["user"].(map[string]any)
		u["id"] = c.id
		if !c.live {
			u["stream"] = nil
		} else {
			s := u["stream"].(map[string]any)
			s["id"], s["createdAt"] = c.broadcast, c.startedAt.Format(time.RFC3339)
		}
		return d

	case "RewardList":
		c := f.byID[str("channelID")]
		name := "gql_reward_list_none"
		if c != nil && !c.achievedAt.IsZero() {
			name = "gql_reward_list_active"
		}
		d := fixture(f.t, name)
		ch := d["channel"].(map[string]any)
		if c != nil {
			ch["id"] = c.id
			m := ch["self"].(map[string]any)["watchStreakMilestone"].(map[string]any)["watchStreakMilestone"].(map[string]any)
			m["value"] = itoa(c.streakLength)
			if !c.achievedAt.IsZero() {
				m["achievementTimestamp"] = c.achievedAt.UTC().Format(time.RFC3339)
			}
		}
		return d

	case "ChannelPointsContext":
		c := f.channels[str("channelLogin")]
		d := fixture(f.t, "gql_channel_points")
		if c != nil {
			cp := d["community"].(map[string]any)["channel"].(map[string]any)["self"].(map[string]any)["communityPoints"].(map[string]any)
			cp["balance"] = c.balance
			if c.claimID != "" {
				cp["availableClaim"] = map[string]any{"id": c.claimID}
			} else {
				cp["availableClaim"] = nil
			}
		}
		return d

	case "ClaimCommunityPoints":
		input, _ := vars["input"].(map[string]any)
		claimID, _ := input["claimID"].(string)
		f.claims = append(f.claims, claimID)
		return map[string]any{"claimCommunityPoints": map[string]any{"error": nil}}
	}
	f.t.Errorf("fake twitch: unexpected GQL operation %q", op)
	return nil
}

// ---- /spade ----

// spade записывает событие minute-watched и применяет модель стрика.
func (f *fakeTwitch) spade(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	raw, err := base64.StdEncoding.DecodeString(r.PostForm.Get("data"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var events []struct {
		Event      string         `json:"event"`
		Properties map[string]any `json:"properties"`
	}
	if err := json.Unmarshal(raw, &events); err != nil || len(events) != 1 || events[0].Event != "minute-watched" {
		f.t.Errorf("fake twitch: bad spade payload %s", raw)
		http.Error(w, "bad payload", http.StatusBadRequest)
		return
	}
	p := events[0].Properties
	login, _ := p["channel"].(string)
	broadcast, _ := p["broadcast_id"].(string)

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.spadeErr {
		http.Error(w, "unavailable", http.StatusInternalServerError)
		return
	}
	f.beacons = append(f.beacons, login+"/"+broadcast)
	if c := f.channels[login]; c != nil && c.live && c.broadcast == broadcast {
		c.minutes++
		if c.streakAfterMinute > 0 && c.minutes == c.streakAfterMinute {
			c.streakLength++
			c.achievedAt = f.clock()
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---- фикстуры ----

var (
	fixtureMu    sync.Mutex
	fixtureCache = map[string][]byte{}
)

// fixtureBytes читает testdata/twitch/<name>.json (с кэшированием).
func fixtureBytes(t *testing.T, name string) []byte {
	fixtureMu.Lock()
	defer fixtureMu.Unlock()
	if b, ok := fixtureCache[name]; ok {
		return b
	}
	b, err := os.ReadFile("testdata/twitch/" + name + ".json")
	if err != nil {
		t.Fatalf("fixture %s: %v", name, err)
	}
	fixtureCache[name] = b
	return b
}

// fixture возвращает свежую декодированную копию записанного payload'а.
func fixture(t *testing.T, name string) map[string]any {
	var v map[string]any
	if err := json.Unmarshal(fixtureBytes(t, name), &v); err != nil {
		t.Fatalf("fixture %s: %v", name, err)
	}
	return v
}

func itoa(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}
