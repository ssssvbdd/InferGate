// Package retry 提供泛型智能重试。
package retry

import (
	"context"
	"time"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/gwerr"
)

// Option 是重试配置。
type Option struct {
	MaxRetries int
	// BaseDelay 线性递增基准延迟。
	BaseDelay time.Duration
}

// DefaultOption 默认最大 3 次，线性递增 150ms。
func DefaultOption() Option {
	return Option{MaxRetries: 3, BaseDelay: 150 * time.Millisecond}
}

// WithRetry 执行 fn，可重试错误时按线性递增延迟重试。
func WithRetry[T any](ctx context.Context, opt Option, fn func(ctx context.Context, attempt int) (T, error)) (T, error) {
	var zero T
	if opt.MaxRetries < 0 {
		opt.MaxRetries = 0
	}
	if opt.BaseDelay <= 0 {
		opt.BaseDelay = DefaultOption().BaseDelay
	}
	var lastErr error
	for attempt := 0; attempt <= opt.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(attempt) * opt.BaseDelay // 150ms → 300ms → 450ms
			select {
			case <-ctx.Done():
				return zero, gwerr.ErrTimeout.WithCause(ctx.Err())
			case <-time.After(delay):
			}
		}
		out, err := fn(ctx, attempt)
		if err == nil {
			return out, nil
		}
		lastErr = err
		if !gwerr.IsRetryable(err) {
			return zero, err
		}
		if ctx.Err() != nil {
			return zero, err
		}
	}
	return zero, lastErr
}
