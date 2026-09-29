// Package router 实现基于策略链的路由决策。
package router

import (
	"context"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
)

// RoutingState 在策略链中传递并逐步筛选候选模型。
type RoutingState struct {
	// Request 是不可变原始请求。
	Request *model.Request
	// CurrentModels 是候选模型列表，策略逐步筛选。
	CurrentModels []model.ModelConfig
	// Metadata 是策略间共享的元数据。
	Metadata map[string]any
}

// RouterStrategy 是路由策略接口。
// Apply 返回 continueNext 控制是否继续执行下一个策略（短路控制）。
type RouterStrategy interface {
	Name() string
	Apply(ctx context.Context, state *RoutingState) (continueNext bool, err error)
}
