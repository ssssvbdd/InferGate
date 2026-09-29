package router

import (
	"context"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/registry"
)

// VersionStrategy 按版本优先级排序候选模型（default 最前，其余版本号倒序）。
type VersionStrategy struct{}

// NewVersionStrategy 创建版本策略。
func NewVersionStrategy() *VersionStrategy { return &VersionStrategy{} }

// Name 返回策略名。
func (s *VersionStrategy) Name() string { return "version" }

// Apply 对候选模型做版本排序，并支持请求指定版本时的重匹配。
func (s *VersionStrategy) Apply(ctx context.Context, state *RoutingState) (bool, error) {
	if len(state.CurrentModels) == 0 {
		return true, nil
	}

	// 请求可通过 metadata 指定版本。
	if v, ok := state.Metadata["version"].(string); ok && v != "" {
		var matched []model.ModelConfig
		for _, m := range state.CurrentModels {
			if m.Version == v {
				matched = append(matched, m)
			}
		}
		if len(matched) > 0 {
			state.CurrentModels = matched
			return true, nil
		}
	}

	state.CurrentModels = registry.SortedVersions(state.CurrentModels)
	return true, nil
}
