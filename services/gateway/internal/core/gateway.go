// Package core 是网关核心，编排请求验证、限流、路由、实例选择、模型调用与指标上报。
package core

import (
	"context"
	"time"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/limiter"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/metrics"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/plugin"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/provider"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/retry"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/router"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/selector"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/validator"
)

// Gateway 是网关核心结构，持有路由器、验证器、限流器工厂与选择管理器。
type Gateway struct {
	router          *router.Router
	validator       validator.RequestValidator
	limiterFactory  *limiter.Factory
	selectorManager *selector.Manager
	upstreamTimeout time.Duration
}

// Option 是 Gateway 选项。
type Option func(*Gateway)

// WithRouter 注入路由器。
func WithRouter(r *router.Router) Option { return func(g *Gateway) { g.router = r } }

// WithLimiterFactory 注入限流器工厂。
func WithLimiterFactory(f *limiter.Factory) Option { return func(g *Gateway) { g.limiterFactory = f } }

// WithSelectorManager 注入选择管理器。
func WithSelectorManager(m *selector.Manager) Option {
	return func(g *Gateway) { g.selectorManager = m }
}

// WithUpstreamTimeout 设置上游超时。
func WithUpstreamTimeout(d time.Duration) Option {
	return func(g *Gateway) { g.upstreamTimeout = d }
}

// NewGateway 创建 Gateway。
func NewGateway(opts ...Option) *Gateway {
	g := &Gateway{
		validator:       validator.New(),
		upstreamTimeout: 3 * time.Second,
	}
	for _, o := range opts {
		o(g)
	}
	return g
}

// hookAdapter 把 plugin.Manager 桥接为 provider.Hooks。
type hookAdapter struct {
	mgr *plugin.Manager
}

func (h *hookAdapter) PreRequest(ctx context.Context, req *model.Request) error {
	return h.mgr.PreRequest(ctx, req)
}
func (h *hookAdapter) OnStream(ctx context.Context, chunk *model.StreamChunk) (bool, error) {
	return h.mgr.OnStream(ctx, chunk)
}
func (h *hookAdapter) PostRequest(ctx context.Context, resp *model.Response, callErr error) error {
	return h.mgr.PostRequest(ctx, resp, callErr)
}

// prepare 执行验证 → 限流 → 路由 → 实例选择 的公共前置流程。
func (g *Gateway) prepare(ctx context.Context, req *model.Request) (*model.ModelConfig, *selector.Selection, error) {
	if err := g.validator.Validate(req); err != nil {
		return nil, nil, err
	}

	// 限流（验证之后、路由之前）。
	if g.limiterFactory != nil {
		trustedUser := req.User
		if tenant, ok := req.Metadata["tenant"].(string); ok && tenant != "" {
			trustedUser = tenant
		}
		env := limiter.Env{
			Model:        req.Model,
			User:         trustedUser,
			Stream:       req.Stream,
			PromptLength: model.EstimatedPromptTokens(req),
		}
		allowed, _ := g.limiterFactory.Allow(ctx, env)
		if !allowed {
			return nil, nil, errRateLimited()
		}
	}

	// 路由决策。
	mc, err := g.router.Select(ctx, req)
	if err != nil {
		return nil, nil, err
	}

	// 实例选择。
	sel, err := g.selectorManager.Select(ctx, mc)
	if err != nil {
		return nil, nil, err
	}
	return mc, sel, nil
}

func (g *Gateway) newHooks(mc *model.ModelConfig) provider.Hooks {
	return &hookAdapter{mgr: plugin.NewManager(mc.Plugins)}
}

func (g *Gateway) callTimeout(mc *model.ModelConfig) time.Duration {
	if mc.Timeout > 0 {
		return mc.Timeout
	}
	return g.upstreamTimeout
}

func retryOption(mc *model.ModelConfig) retry.Option {
	opt := retry.DefaultOption()
	// Zero is the safe default: an inference request may already consume GPU
	// work or bill tokens before a transport error becomes visible.
	opt.MaxRetries = mc.MaxRetries
	return opt
}

func buildDimension(mc *model.ModelConfig, sel *selector.Selection, req *model.Request, mgr *selector.Manager, errCode string) metrics.ChatDimension {
	return metrics.ChatDimension{
		User:                req.User,
		Model:               mc.Name,
		Version:             mc.Version,
		ModelType:           string(mc.ModelType),
		ProviderName:        string(mc.ProviderType),
		ProviderType:        string(mc.ProviderType),
		ProviderModel:       mc.ProviderModel,
		Endpoint:            sel.Endpoint.ID,
		CircuitBreakerState: mgr.BreakerState(sel.CBKey()),
		ErrorCode:           errCode,
	}
}
