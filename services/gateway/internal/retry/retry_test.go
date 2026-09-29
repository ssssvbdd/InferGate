package retry

import (
	"context"
	"errors"
	"testing"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/gwerr"
)

func TestZeroRetriesCallsOnce(t *testing.T) {
	calls := 0
	_, err := WithRetry(context.Background(), Option{MaxRetries: 0}, func(context.Context, int) (int, error) {
		calls++
		return 0, gwerr.ErrUpstream.WithCause(errors.New("failed"))
	})
	if err == nil || calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}
