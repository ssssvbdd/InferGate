package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCheckerReflectsUpstreamStatus(t *testing.T) {
	for _, test := range []struct {
		status int
		ready  bool
	}{{http.StatusOK, true}, {http.StatusServiceUnavailable, false}} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(test.status)
		}))
		checker := New(server.URL, time.Second)
		if got := checker.Ready(context.Background()); got != test.ready {
			t.Fatalf("status=%d ready=%v", test.status, got)
		}
		server.Close()
	}
}

func TestCheckerRequiresCriticalDependency(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	checker := New(server.URL, time.Second).WithDependency(func(context.Context) bool { return false })
	if checker.Ready(context.Background()) {
		t.Fatal("critical quota store failure must remove readiness")
	}
}

func TestCheckerReadyWhenOneUpstreamSurvives(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	checker := NewAny([]string{"http://127.0.0.1:1/health", server.URL}, time.Second)
	if !checker.Ready(context.Background()) {
		t.Fatal("one healthy upstream must keep gateway ready")
	}
}
