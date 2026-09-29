// Package plugin 定义插件三钩子生命周期与管理器。
package plugin

import (
	"context"
	"sync"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
)

// Plugin 定义插件三个生命周期钩子。
type Plugin interface {
	// Name 返回插件名。
	Name() string
	// PreRequest 在请求发送前执行（用于请求预处理）。
	PreRequest(ctx context.Context, req *model.Request) error
	// OnStream 在流式响应处理时对每个 chunk 执行，返回 skip 支持短路。
	OnStream(ctx context.Context, chunk *model.StreamChunk) (skip bool, err error)
	// PostRequest 在请求完成后执行（即使出错也执行）。
	PostRequest(ctx context.Context, resp *model.Response, callErr error) error
}

// Factory 是插件工厂函数。
type Factory func() Plugin

var (
	pluginMap = map[string]Factory{}
	mu        sync.RWMutex
)

// Register 注册插件工厂。
func Register(name string, f Factory) {
	mu.Lock()
	defer mu.Unlock()
	pluginMap[name] = f
}

// Manager 按顺序管理一组插件实例。
type Manager struct {
	plugins []Plugin
}

// NewManager 根据插件名列表创建管理器，未知插件名会被跳过。
func NewManager(names []string) *Manager {
	mu.RLock()
	defer mu.RUnlock()
	m := &Manager{}
	for _, name := range names {
		if f, ok := pluginMap[name]; ok {
			m.plugins = append(m.plugins, f())
		}
	}
	return m
}

// PreRequest 顺序调用各插件 PreRequest，任一失败则终止。
func (m *Manager) PreRequest(ctx context.Context, req *model.Request) error {
	for _, p := range m.plugins {
		if err := p.PreRequest(ctx, req); err != nil {
			return err
		}
	}
	return nil
}

// OnStream 遍历插件，支持 skip 短路（跳过后续插件）。
func (m *Manager) OnStream(ctx context.Context, chunk *model.StreamChunk) (bool, error) {
	for _, p := range m.plugins {
		skip, err := p.OnStream(ctx, chunk)
		if err != nil {
			return false, err
		}
		if skip {
			return true, nil
		}
	}
	return false, nil
}

// PostRequest 逆序调用各插件 PostRequest。
func (m *Manager) PostRequest(ctx context.Context, resp *model.Response, callErr error) error {
	var firstErr error
	for i := len(m.plugins) - 1; i >= 0; i-- {
		if err := m.plugins[i].PostRequest(ctx, resp, callErr); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// BasePlugin 提供空实现，便于内置插件按需覆盖。
type BasePlugin struct{}

// PreRequest 空实现。
func (BasePlugin) PreRequest(context.Context, *model.Request) error { return nil }

// OnStream 空实现。
func (BasePlugin) OnStream(context.Context, *model.StreamChunk) (bool, error) { return false, nil }

// PostRequest 空实现。
func (BasePlugin) PostRequest(context.Context, *model.Response, error) error { return nil }
