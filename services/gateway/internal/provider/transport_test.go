package provider

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/gwerr"
)

func TestStreamOutlivesOrdinaryRequestTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"part\":1}\n\n")
		w.(http.Flusher).Flush()
		time.Sleep(60 * time.Millisecond)
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()
	chunks := make(chan rawChunk, 4)
	if err := doStream(context.Background(), streamHTTPClient(true), srv.URL, nil, nil, chunks, true, time.Second); err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 2 {
		t.Fatalf("expected data and DONE, got %d chunks", len(chunks))
	}
}

func TestStreamIdleTimeoutCancelsUpstream(t *testing.T) {
	cancelled := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"part\":1}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(cancelled)
	}))
	defer srv.Close()
	err := doStream(context.Background(), streamHTTPClient(true), srv.URL, nil, nil,
		make(chan rawChunk, 4), true, 20*time.Millisecond)
	var ge *gwerr.GatewayError
	if !errors.As(err, &ge) || ge.Type != gwerr.TypeTimeout {
		t.Fatalf("expected idle timeout, got %v", err)
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("upstream request was not cancelled")
	}
}
