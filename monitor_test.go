package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestMonitorChecksTargets(t *testing.T) {
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer okSrv.Close()
	badSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer badSrv.Close()

	m := NewMonitor([]string{okSrv.URL, badSrv.URL}, 20*time.Millisecond, 2, time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.Run(ctx)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		snap := m.Snapshot()
		if snap[0].Checks >= 2 && snap[1].Checks >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for checks")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}

	byURL := map[string]Status{}
	for _, s := range m.Snapshot() {
		byURL[s.URL] = s
	}
	if !byURL[okSrv.URL].Up || byURL[okSrv.URL].Failures != 0 {
		t.Errorf("ok server: %+v", byURL[okSrv.URL])
	}
	if byURL[badSrv.URL].Up || byURL[badSrv.URL].Failures == 0 {
		t.Errorf("bad server: %+v", byURL[badSrv.URL])
	}
}

func TestMonitorUnreachable(t *testing.T) {
	m := NewMonitor([]string{"http://127.0.0.1:1"}, time.Hour, 1, 200*time.Millisecond)
	res := m.check(context.Background(), "http://127.0.0.1:1")
	m.record(res)
	s := m.Snapshot()[0]
	if s.Up || s.Error == "" {
		t.Errorf("expected failure with error, got %+v", s)
	}
}

func TestHubFanOut(t *testing.T) {
	h := NewHub()
	a, cancelA := h.Subscribe()
	b, cancelB := h.Subscribe()
	defer cancelB()

	h.Publish(Status{URL: "x"})
	for _, ch := range []<-chan Status{a, b} {
		select {
		case s := <-ch:
			if s.URL != "x" {
				t.Errorf("got %q", s.URL)
			}
		case <-time.After(time.Second):
			t.Fatal("subscriber got nothing")
		}
	}

	cancelA()
	if _, ok := <-a; ok {
		t.Error("channel should be closed after cancel")
	}
	h.Publish(Status{URL: "y"}) // не должно паниковать после отписки
}

func TestStatusEndpoint(t *testing.T) {
	m := NewMonitor([]string{"http://example.test"}, time.Hour, 1, time.Second)
	rec := httptest.NewRecorder()
	NewHandler(m, nil).ServeHTTP(rec, httptest.NewRequest("GET", "/api/status", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("got %d", rec.Code)
	}
}
