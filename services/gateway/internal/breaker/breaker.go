// Package breaker 实现三状态熔断器（Closed → Open → HalfOpen）。
package breaker

import (
	"errors"
	"sync"
	"time"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/gwerr"
)

// State 是熔断器状态。
type State int

const (
	StateClosed   State = iota // 正常
	StateOpen                  // 熔断
	StateHalfOpen              // 探测恢复
)

func (s State) String() string {
	switch s {
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half_open"
	default:
		return "closed"
	}
}

// Config 是熔断器配置。
type Config struct {
	// FailureWindow 失败窗口期（建议 30-60s）。
	FailureWindow time.Duration
	// MaxFailCount 窗口内最大失败次数（建议 3-5）。
	MaxFailCount int
	// RecoveryInterval 恢复探测间隔（建议 60-120s，应大于 FailureWindow）。
	RecoveryInterval time.Duration
}

// DefaultConfig 返回默认配置。
func DefaultConfig() Config {
	return Config{
		FailureWindow:    45 * time.Second,
		MaxFailCount:     5,
		RecoveryInterval: 90 * time.Second,
	}
}

type instanceState struct {
	state       State
	failCount   int
	firstFailAt time.Time
	openedAt    time.Time
	lastAccess  time.Time
}

// CircuitBreaker 管理各实例的熔断状态。
type CircuitBreaker struct {
	cfg   Config
	mu    sync.Mutex
	items map[string]*instanceState
	stop  chan struct{}
}

// New 创建熔断器并启动后台清理协程。
func New(cfg Config) *CircuitBreaker {
	if cfg.MaxFailCount <= 0 {
		cfg = DefaultConfig()
	}
	cb := &CircuitBreaker{
		cfg:   cfg,
		items: make(map[string]*instanceState),
		stop:  make(chan struct{}),
	}
	go cb.cleanupLoop()
	return cb
}

// IsHealthy 判断实例当前是否可用（Open 态不可用；HalfOpen/Closed 可用）。
func (cb *CircuitBreaker) IsHealthy(key string) bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	st, ok := cb.items[key]
	if !ok {
		return true
	}
	st.lastAccess = time.Now()
	if st.state == StateOpen {
		// 到达恢复间隔则进入半开探测。
		if time.Since(st.openedAt) >= cb.cfg.RecoveryInterval {
			st.state = StateHalfOpen
			return true
		}
		return false
	}
	return true
}

// State 返回实例当前熔断状态。
func (cb *CircuitBreaker) State(key string) State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if st, ok := cb.items[key]; ok {
		return st.state
	}
	return StateClosed
}

// Record 根据请求结果更新熔断状态。
func (cb *CircuitBreaker) Record(key string, err error) {
	if err == nil {
		cb.RecordSuccess(key)
		return
	}
	if shouldTrigger(err) {
		cb.RecordFailure(key)
	}
}

// RecordSuccess 记录成功，半开态成功则关闭熔断。
func (cb *CircuitBreaker) RecordSuccess(key string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	st := cb.get(key)
	st.lastAccess = time.Now()
	if st.state == StateHalfOpen {
		st.state = StateClosed
	}
	st.failCount = 0
	st.firstFailAt = time.Time{}
}

// RecordFailure 记录失败，超过阈值则打开熔断。
func (cb *CircuitBreaker) RecordFailure(key string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	st := cb.get(key)
	now := time.Now()
	st.lastAccess = now

	if st.state == StateHalfOpen {
		// 半开探测失败，重新打开。
		st.state = StateOpen
		st.openedAt = now
		return
	}

	// 窗口过期则重置计数。
	if st.firstFailAt.IsZero() || now.Sub(st.firstFailAt) > cb.cfg.FailureWindow {
		st.firstFailAt = now
		st.failCount = 0
	}
	st.failCount++
	if st.failCount >= cb.cfg.MaxFailCount {
		st.state = StateOpen
		st.openedAt = now
	}
}

func (cb *CircuitBreaker) get(key string) *instanceState {
	st, ok := cb.items[key]
	if !ok {
		st = &instanceState{state: StateClosed, lastAccess: time.Now()}
		cb.items[key] = st
	}
	return st
}

func (cb *CircuitBreaker) cleanupLoop() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-cb.stop:
			return
		case <-t.C:
			cb.cleanup()
		}
	}
}

func (cb *CircuitBreaker) cleanup() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	now := time.Now()
	for k, st := range cb.items {
		if st.state == StateClosed && now.Sub(st.lastAccess) > 5*time.Minute {
			delete(cb.items, k)
		}
	}
}

// Close 停止后台协程。
func (cb *CircuitBreaker) Close() { close(cb.stop) }

// shouldTrigger 精细化判断错误是否应触发熔断。
// 触发：5xx、网络错误、连接失败；排除：上下文错误、过载、限流、4xx。
func shouldTrigger(err error) bool {
	if err == nil {
		return false
	}
	var ge *gwerr.GatewayError
	if errors.As(err, &ge) {
		switch ge.Type {
		case gwerr.TypeUpstream, gwerr.TypeTimeout:
			return true
		case gwerr.TypeRateLimit, gwerr.TypeOverloaded,
			gwerr.TypeInvalidRequest, gwerr.TypeAuth,
			gwerr.TypePermission, gwerr.TypeNotFound:
			return false
		}
	}
	var re *gwerr.RequestError
	if errors.As(err, &re) {
		return re.StatusCode >= 500
	}
	// 未知错误（网络类）触发。
	return true
}
