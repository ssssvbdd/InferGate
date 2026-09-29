// Package selector 实现实例选择：服务发现 → 熔断过滤 → 负载均衡。
package selector

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
)

// Selector 是负载均衡选择器接口。
type Selector interface {
	// Select 从候选实例中选出一个。
	Select(ctx context.Context, modelName string, eps []model.Endpoint) (model.Endpoint, error)
	// Release 释放选中实例占用的资源（如信号量/连接计数）。
	Release(ctx context.Context, modelName string, ep model.Endpoint)
}

// weightedSelector 平滑加权轮询（Nginx SWRR 算法）。
type weightedSelector struct {
	mu      sync.Mutex
	current map[string][]int // modelName -> 每个实例当前权重
}

func newWeightedSelector() *weightedSelector {
	return &weightedSelector{current: make(map[string][]int)}
}

func (s *weightedSelector) Select(ctx context.Context, modelName string, eps []model.Endpoint) (model.Endpoint, error) {
	if len(eps) == 0 {
		return model.Endpoint{}, errNoInstance
	}
	if len(eps) == 1 {
		return eps[0], nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	cur := s.current[modelName]
	if len(cur) != len(eps) {
		cur = make([]int, len(eps))
	}

	total := 0
	best := -1
	for i, ep := range eps {
		w := ep.Weight
		if w <= 0 {
			w = 1
		}
		cur[i] += w
		total += w
		if best == -1 || cur[i] > cur[best] {
			best = i
		}
	}
	cur[best] -= total
	s.current[modelName] = cur
	return eps[best], nil
}

func (s *weightedSelector) Release(context.Context, string, model.Endpoint) {}

// roundRobinSelector 简单轮询。
type roundRobinSelector struct {
	counters sync.Map // modelName -> *uint64
}

func newRoundRobinSelector() *roundRobinSelector { return &roundRobinSelector{} }

func (s *roundRobinSelector) Select(ctx context.Context, modelName string, eps []model.Endpoint) (model.Endpoint, error) {
	if len(eps) == 0 {
		return model.Endpoint{}, errNoInstance
	}
	v, _ := s.counters.LoadOrStore(modelName, new(uint64))
	c := v.(*uint64)
	idx := int(atomic.AddUint64(c, 1)-1) % len(eps)
	return eps[idx], nil
}

func (s *roundRobinSelector) Release(context.Context, string, model.Endpoint) {}

// directSelector 直连第一个实例。
type directSelector struct{}

func (directSelector) Select(ctx context.Context, modelName string, eps []model.Endpoint) (model.Endpoint, error) {
	if len(eps) == 0 {
		return model.Endpoint{}, errNoInstance
	}
	return eps[0], nil
}

func (directSelector) Release(context.Context, string, model.Endpoint) {}
