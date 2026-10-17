package breaker

import (
	"testing"
	"time"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/gwerr"
)

func TestCircuitBreaker_StateMachine(t *testing.T) {
	cb := New(Config{
		FailureWindow:    time.Second,
		MaxFailCount:     3,
		RecoveryInterval: 50 * time.Millisecond,
	})
	defer cb.Close()

	key := "model|inst-1"
	upstreamErr := gwerr.ErrUpstream

	// 初始为健康（Closed）。
	if !cb.IsHealthy(key) {
		t.Fatal("expected healthy initially")
	}

	// 连续 3 次可触发失败 → Open。
	for i := 0; i < 3; i++ {
		cb.RecordFailure(key)
	}
	if cb.State(key) != StateOpen {
		t.Fatalf("expected open, got %v", cb.State(key))
	}
	if cb.IsHealthy(key) {
		t.Fatal("expected unhealthy when open")
	}

	// 等待恢复间隔 → 半开可探测。
	time.Sleep(60 * time.Millisecond)
	if !cb.IsHealthy(key) {
		t.Fatal("expected half-open probe allowed")
	}
	if cb.State(key) != StateHalfOpen {
		t.Fatalf("expected half-open, got %v", cb.State(key))
	}

	// 半开成功 → Closed。
	cb.RecordSuccess(key)
	if cb.State(key) != StateClosed {
		t.Fatalf("expected closed after success, got %v", cb.State(key))
	}

	// 非触发类错误不应影响状态。
	cb.Record(key, gwerr.ErrRateLimited)
	if cb.State(key) != StateClosed {
		t.Fatalf("rate-limit error should not trip breaker, got %v", cb.State(key))
	}
	_ = upstreamErr
}

func TestHalfOpenAdmitsOneProbe(t *testing.T) {
	cb := New(Config{FailureWindow: time.Second, MaxFailCount: 1, RecoveryInterval: 20 * time.Millisecond})
	defer cb.Close()
	cb.RecordFailure("ep")
	if cb.TryAcquire("ep") {
		t.Fatal("open endpoint admitted before recovery interval")
	}
	time.Sleep(25 * time.Millisecond)
	if !cb.TryAcquire("ep") || cb.TryAcquire("ep") {
		t.Fatal("half-open must admit exactly one concurrent probe")
	}
	cb.Record("ep", gwerr.ErrInvalidRequest)
	if cb.State("ep") != StateOpen {
		t.Fatal("inconclusive probe must return to open")
	}
}
