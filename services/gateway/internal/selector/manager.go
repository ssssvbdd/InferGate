package selector

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/breaker"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/discovery"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/gwerr"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
)

// Manager 执行实例选择三步流程：服务发现 → 熔断过滤 → 负载均衡。
type Manager struct {
	discoverer *discovery.Manager
	breaker    *breaker.CircuitBreaker
	rdb        *redis.Client
	timeout    time.Duration
	// selectors 缓存不同策略的选择器实例。
	selectors map[model.LoadBalanceStrategy]Selector
}

// NewManager 创建选择管理器。
func NewManager(disc *discovery.Manager, cb *breaker.CircuitBreaker, rdb *redis.Client, timeout time.Duration) *Manager {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	m := &Manager{
		discoverer: disc,
		breaker:    cb,
		rdb:        rdb,
		timeout:    timeout,
		selectors:  make(map[model.LoadBalanceStrategy]Selector),
	}
	m.selectors[model.LBWeighted] = newWeightedSelector()
	m.selectors[model.LBRoundRobin] = newRoundRobinSelector()
	m.selectors[model.LBDirect] = directSelector{}
	m.selectors[model.LBLeastConn] = newLeastConnSelector(rdb)
	m.selectors[model.LBIdleRandom] = newIdleRandomSelector(rdb)
	return m
}

// Selection 是一次选择结果，用于后续释放资源与熔断记录。
type Selection struct {
	Endpoint  model.Endpoint
	selector  Selector
	modelName string
	cbKey     string
}

// Release 释放本次选择占用的资源。
func (s *Selection) Release(ctx context.Context) {
	if s.selector != nil {
		s.selector.Release(ctx, s.modelName, s.Endpoint)
	}
}

// CBKey 返回熔断器键。
func (s *Selection) CBKey() string { return s.cbKey }

// Select 执行三步选择流程。
func (m *Manager) Select(ctx context.Context, mc *model.ModelConfig) (*Selection, error) {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()

	// 1) 服务发现。
	eps, err := m.discoverer.Discover(ctx, mc)
	if err != nil {
		return nil, gwerr.ErrUpstream.WithCause(err)
	}
	if len(eps) == 0 {
		return nil, gwerr.ErrNoInstance
	}

	// 2) 熔断健康过滤。
	healthy := make([]model.Endpoint, 0, len(eps))
	for _, ep := range eps {
		key := cbKey(mc.Name, ep)
		if m.breaker == nil || m.breaker.IsHealthy(key) {
			healthy = append(healthy, ep)
		}
	}
	if len(healthy) == 0 {
		// 全部熔断，兜底放行首个（半开探测）。
		healthy = eps[:1]
	}

	// 3) 负载均衡选择。
	strategy := mc.LoadBalance
	sel, ok := m.selectors[strategy]
	if !ok {
		sel = m.selectors[model.LBWeighted]
	}
	ep, err := sel.Select(ctx, mc.Name, healthy)
	if err != nil {
		return nil, gwerr.ErrNoInstance.WithCause(err)
	}

	return &Selection{
		Endpoint:  ep,
		selector:  sel,
		modelName: mc.Name,
		cbKey:     cbKey(mc.Name, ep),
	}, nil
}

// RecordResult 根据请求结果更新熔断状态。
func (m *Manager) RecordResult(key string, err error) {
	if m.breaker != nil {
		m.breaker.Record(key, err)
	}
}

// BreakerState 返回熔断状态字符串。
func (m *Manager) BreakerState(key string) string {
	if m.breaker == nil {
		return "closed"
	}
	return m.breaker.State(key).String()
}

func cbKey(modelName string, ep model.Endpoint) string {
	return modelName + "|" + epID(modelName, ep)
}
