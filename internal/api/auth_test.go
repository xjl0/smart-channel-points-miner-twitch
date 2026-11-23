package api

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBasicAuth(t *testing.T) {
	failedAuthDelay = 0
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := httptest.NewServer(Handler(nil, nil, Credentials{User: "admin", Password: "secret"}, log))
	defer srv.Close()

	get := func(path, user, pass string) *http.Response {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+path, nil)
		if user != "" || pass != "" {
			req.SetBasicAuth(user, pass)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp
	}

	if r := get("/", "", ""); r.StatusCode != http.StatusUnauthorized || r.Header.Get("WWW-Authenticate") == "" {
		t.Fatalf("no credentials: status %d, WWW-Authenticate %q; want 401 with a login prompt", r.StatusCode, r.Header.Get("WWW-Authenticate"))
	}
	if r := get("/", "admin", "wrong"); r.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong password: status %d, want 401", r.StatusCode)
	}
	if r := get("/", "root", "secret"); r.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong user: status %d, want 401", r.StatusCode)
	}
	// Статику бэкенд не раздаёт: с верным логином "/" — 404, но уже не 401.
	if r := get("/", "admin", "secret"); r.StatusCode != http.StatusNotFound {
		t.Fatalf("right credentials: status %d, want 404 (auth passed, no static files)", r.StatusCode)
	}
	for _, path := range []string{"/live", "/miner.v1.MinerService/ListEvents"} {
		if r := get(path, "", ""); r.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s without credentials: status %d, want 401", path, r.StatusCode)
		}
	}
	if r := get("/healthz", "", ""); r.StatusCode != http.StatusOK {
		t.Fatalf("/healthz must stay open for the Docker healthcheck, got %d", r.StatusCode)
	}
}

func TestNoPasswordMeansNoAuth(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := httptest.NewServer(Handler(nil, nil, Credentials{User: "admin"}, log))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		t.Fatal("got 401 although no password is configured")
	}
}
