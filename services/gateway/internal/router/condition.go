package router

import (
	"context"
	"sort"
	"sync/atomic"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
)

// ConditionStrategy 使用 Mongo 风格条件表达式匹配，按优先级排序后加权轮询选择。
type ConditionStrategy struct {
	// rrCounter 用于加权轮询。
	rrCounter uint64
}

// NewConditionStrategy 创建条件策略。
func NewConditionStrategy() *ConditionStrategy { return &ConditionStrategy{} }

// Name 返回策略名。
func (s *ConditionStrategy) Name() string { return "condition" }

// Apply 过滤命中条件的模型，排序后加权轮询选出最终目标。
func (s *ConditionStrategy) Apply(ctx context.Context, state *RoutingState) (bool, error) {
	if len(state.CurrentModels) == 0 {
		return true, nil
	}

	var matched []model.ModelConfig
	var defaults []model.ModelConfig
	for _, m := range state.CurrentModels {
		if len(m.Condition) == 0 {
			defaults = append(defaults, m)
			continue
		}
		if matchCondition(m.Condition, state.Metadata) {
			matched = append(matched, m)
		}
	}

	// applyDefaultRule 兜底：无命中条件时使用无条件模型。
	if len(matched) == 0 {
		matched = defaults
	}
	if len(matched) == 0 {
		// 保持原候选（交由后续兜底）。
		return true, nil
	}

	// 按优先级降序排序。
	sort.SliceStable(matched, func(i, j int) bool {
		return matched[i].Priority > matched[j].Priority
	})

	// 取最高优先级组，加权轮询。
	top := matched[0].Priority
	var group []model.ModelConfig
	for _, m := range matched {
		if m.Priority == top {
			group = append(group, m)
		}
	}
	selected := s.selectByWeight(group)
	state.CurrentModels = []model.ModelConfig{selected}
	return true, nil
}

// selectByWeight 按权重轮询选择。
func (s *ConditionStrategy) selectByWeight(group []model.ModelConfig) model.ModelConfig {
	if len(group) == 1 {
		return group[0]
	}
	total := 0
	for _, m := range group {
		w := m.Weight
		if w <= 0 {
			w = 1
		}
		total += w
	}
	if total <= 0 {
		return group[0]
	}
	n := int(atomic.AddUint64(&s.rrCounter, 1)-1) % total
	for _, m := range group {
		w := m.Weight
		if w <= 0 {
			w = 1
		}
		if n < w {
			return m
		}
		n -= w
	}
	return group[0]
}
