package router

import (
	"context"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/gwerr"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/log"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/registry"
	"go.uber.org/zap"
)

// Router 是路由决策引擎，按策略链依次执行。
type Router struct {
	strategies []RouterStrategy
}

// NewRouter 创建路由引擎，注册默认策略链：Identity → Version → Condition。
func NewRouter(reg *registry.ModelRegistry) *Router {
	return &Router{
		strategies: []RouterStrategy{
			NewIdentityStrategy(reg),
			NewVersionStrategy(),
			NewConditionStrategy(),
		},
	}
}

// Select 执行路由策略链，返回最终目标模型配置。
func (r *Router) Select(ctx context.Context, req *model.Request) (*model.ModelConfig, error) {
	state := &RoutingState{
		Request:  req,
		Metadata: map[string]any{},
	}
	r.applyMetadata(state)

	for _, strategy := range r.strategies {
		cont, err := strategy.Apply(ctx, state)
		if err != nil {
			// 身份策略失败直接返回；其余策略失败仅记录并继续（容错）。
			if strategy.Name() == "identity" {
				return nil, err
			}
			log.Warn("router strategy failed",
				zap.String("strategy", strategy.Name()), zap.Error(err))
			continue
		}
		if !cont {
			break
		}
	}

	if len(state.CurrentModels) == 0 {
		return nil, gwerr.ErrModelNotFound.WithCause(errString("no candidate after routing"))
	}
	result := state.CurrentModels[0]
	return &result, nil
}

// applyMetadata 从请求中提取路由所需元数据。
func (r *Router) applyMetadata(state *RoutingState) {
	req := state.Request
	m := state.Metadata
	m["request_model"] = req.Model
	m["user"] = req.User
	m["stream"] = req.Stream
	m["function_calling"] = len(req.Tools) > 0
	m["prompt_length"] = model.EstimatedPromptTokens(req)
	if req.ResponseFormat != nil {
		m["response_format"] = string(req.ResponseFormat)
	}
	// 合并请求自带 metadata（如 version）。
	for k, v := range req.Metadata {
		m[k] = v
	}
}
