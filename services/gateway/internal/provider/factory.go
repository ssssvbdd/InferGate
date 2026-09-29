package provider

import (
	"context"
	"sync"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/gwerr"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
)

// adapterFactory 是适配器工厂函数。
type adapterFactory func() ProviderAdapter

// registry 保存 ProviderType -> 工厂 的映射（双层选择第一层）。
var (
	registry = map[model.ProviderType]adapterFactory{
		model.ProviderOpenAI:    newOpenAICompatAdapter,
		model.ProviderGeneric:   newOpenAICompatAdapter,
		model.ProviderOllama:    newOpenAICompatAdapter,
		model.ProviderAnthropic: newAnthropicAdapter,
	}
	registryMu sync.RWMutex
)

// Register 注册自定义适配器工厂，便于扩展新的提供商。
func Register(pt model.ProviderType, f adapterFactory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry[pt] = f
}

// GetAdapter 通过 ProviderType + ModelType 双层选择返回适配器实例。
func GetAdapter(pt model.ProviderType, mt model.ModelType) (ProviderAdapter, error) {
	registryMu.RLock()
	f, ok := registry[pt]
	registryMu.RUnlock()
	if !ok {
		return nil, gwerr.New("unsupported_provider", gwerr.TypeInvalidRequest, 400,
			"unsupported provider type: "+string(pt))
	}
	// 第二层：按 ModelType 校验支持性（此处所有适配器均支持三类，保留扩展点）。
	switch mt {
	case model.ModelTypeLLM, model.ModelTypeEmbedding, model.ModelTypeRerank:
	default:
		return nil, gwerr.New("unsupported_model_type", gwerr.TypeInvalidRequest, 400,
			"unsupported model type: "+string(mt))
	}
	return f(), nil
}

// ModelProvider 组合适配器与插件管理器，编排完整调用链。
type ModelProvider struct {
	adapter ProviderAdapter
	inst    Instance
	hooks   Hooks
}

// Hooks 是插件生命周期钩子集合（由 core 层注入，避免包循环）。
type Hooks interface {
	PreRequest(ctx context.Context, req *model.Request) error
	OnStream(ctx context.Context, chunk *model.StreamChunk) (skip bool, err error)
	PostRequest(ctx context.Context, resp *model.Response, callErr error) error
}

// noopHooks 是空实现。
type noopHooks struct{}

func (noopHooks) PreRequest(context.Context, *model.Request) error { return nil }
func (noopHooks) OnStream(context.Context, *model.StreamChunk) (bool, error) {
	return false, nil
}
func (noopHooks) PostRequest(context.Context, *model.Response, error) error { return nil }

// NewModelProvider 创建 ModelProvider。
func NewModelProvider(ctx context.Context, inst Instance, hooks Hooks) (*ModelProvider, error) {
	adapter, err := GetAdapter(inst.Model.ProviderType, inst.Model.ModelType)
	if err != nil {
		return nil, err
	}
	if hooks == nil {
		hooks = noopHooks{}
	}
	return &ModelProvider{adapter: adapter, inst: inst, hooks: hooks}, nil
}

// SendRequest 执行非流式调用链：
// PreRequest → Create → AdapterRequest → Invoke → PostRequest → AdapterResponse/AdapterError → Close。
func (p *ModelProvider) SendRequest(ctx context.Context, req *model.Request) (resp *model.Response, err error) {
	cp := req.Clone()
	cp.Model = p.inst.Model.ProviderModel

	if err = p.hooks.PreRequest(ctx, cp); err != nil {
		return nil, err
	}
	if err = p.adapter.Create(ctx, p.inst); err != nil {
		return nil, err
	}
	defer func() { _ = p.adapter.Close() }()

	body, err := p.adapter.AdapterRequest(ctx, cp)
	if err != nil {
		return nil, err
	}

	status, respBody, callErr := p.adapter.Invoke(ctx, body)
	if callErr == nil && status >= 400 {
		callErr = p.adapter.AdapterError(status, respBody)
	}
	if callErr == nil {
		resp, callErr = p.adapter.AdapterResponse(respBody)
	}
	// PostRequest observes the final adapted error and parsed usage.
	if hErr := p.hooks.PostRequest(ctx, resp, callErr); hErr != nil && callErr == nil {
		callErr = hErr
	}
	return resp, callErr
}

// StreamRequest 执行流式调用链，使用 channel 传递流式响应。
func (p *ModelProvider) StreamRequest(ctx context.Context, req *model.Request) (<-chan *model.StreamChunk, error) {
	cp := req.Clone()
	cp.Model = p.inst.Model.ProviderModel
	cp.Stream = true

	if err := p.hooks.PreRequest(ctx, cp); err != nil {
		return nil, err
	}
	if err := p.adapter.Create(ctx, p.inst); err != nil {
		return nil, err
	}
	body, err := p.adapter.AdapterRequest(ctx, cp)
	if err != nil {
		_ = p.adapter.Close()
		return nil, err
	}

	rawCh := make(chan rawChunk, 32)
	outCh := make(chan *model.StreamChunk, 32)

	// 上游流式协程。
	go func() {
		defer close(rawCh)
		if err := p.adapter.InvokeStream(ctx, body, rawCh); err != nil {
			rawCh <- rawChunk{err: err}
		}
	}()

	// 转换 + 插件 OnStream 协程。
	go func() {
		defer close(outCh)
		defer func() { _ = p.adapter.Close() }()
		var lastUsage *model.Usage
		for rc := range rawCh {
			chunk, cErr := p.adapter.AdapterStreamResponse(rc)
			if chunk == nil {
				continue
			}
			if chunk.Usage != nil {
				lastUsage = chunk.Usage
			}
			skip, hErr := p.hooks.OnStream(ctx, chunk)
			if hErr != nil {
				chunk.Err = hErr
			}
			if !skip {
				select {
				case outCh <- chunk:
				case <-ctx.Done():
					return
				}
			}
			if cErr != nil || chunk.Done || chunk.Err != nil {
				break
			}
		}
		_ = p.hooks.PostRequest(ctx, &model.Response{Usage: lastUsage}, nil)
	}()

	return outCh, nil
}

// Usage 暴露实例的目标模型配置，供上层监控使用。
func (p *ModelProvider) Instance() Instance { return p.inst }
