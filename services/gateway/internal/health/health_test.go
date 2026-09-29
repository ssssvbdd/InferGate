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
