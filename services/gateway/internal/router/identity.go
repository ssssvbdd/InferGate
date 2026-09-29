package router

import (
	"context"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/gwerr"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/registry"
)

// IdentityStrategy 根据请求模型名从注册中心加载候选模型集合。
type IdentityStrategy struct {
	reg *registry.ModelRegistry
}

// NewIdentityStrategy 创建身份策略。
func NewIdentityStrategy(reg *registry.ModelRegistry) *IdentityStrategy {
	return &IdentityStrategy{reg: reg}
}

// Name 返回策略名。
func (s *IdentityStrategy) Name() string { return "identity" }

// Apply 加载模型名对应的候选配置。
func (s *IdentityStrategy) Apply(ctx context.Context, state *RoutingState) (bool, error) {
	name := parseModelName(state.Request.Model)
	models, ok := s.reg.GetModels(ctx, name)
	if !ok || len(models) == 0 {
		return false, gwerr.ErrModelNotFound.WithCause(errString("model=" + name))
	}
	state.CurrentModels = models
	return true, nil
}

// parseModelName 规整模型名（去除内置/自定义前缀等）。
func parseModelName(raw string) string {
	// 去除可能的租户前缀 "tenant/model"。
	for i := len(raw) - 1; i >= 0; i-- {
		if raw[i] == '/' {
			return raw[i+1:]
		}
	}
	return raw
}

type errString string

func (e errString) Error() string { return string(e) }
