// Package provider 定义提供商适配器接口与工厂，负责协议转换与上游调用。
package provider

import (
	"context"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
)

// Instance 描述本次调用选中的上游实例与目标模型配置。
type Instance struct {
	Endpoint model.Endpoint
	Model    *model.ModelConfig
}

// ProviderAdapter 是提供商适配器接口，定义完整的请求生命周期（8 个核心方法）。
type ProviderAdapter interface {
	// Create 建立连接/初始化适配器实例。
	Create(ctx context.Context, inst Instance) error
	// Close 关闭适配器，释放资源。
	Close() error

	// AdapterRequest 将网关统一请求转换为提供商特定的请求体。
	AdapterRequest(ctx context.Context, req *model.Request) ([]byte, error)
	// AdapterError 将提供商错误响应转换为网关错误。
	AdapterError(statusCode int, body []byte) error

	// Invoke 执行非流式调用，返回上游原始响应体与状态码。
	Invoke(ctx context.Context, body []byte) (statusCode int, respBody []byte, err error)
	// InvokeStream 执行流式调用，通过 out channel 逐块推送原始 SSE 负载。
	InvokeStream(ctx context.Context, body []byte, out chan<- rawChunk) error

	// AdapterResponse 将提供商非流式响应转换为网关统一响应。
	AdapterResponse(respBody []byte) (*model.Response, error)
	// AdapterStreamResponse 将提供商流式 chunk 转换为网关统一 StreamChunk。
	AdapterStreamResponse(raw rawChunk) (*model.StreamChunk, error)
}

// rawChunk 是上游流式原始数据块。
type rawChunk struct {
	data []byte
	done bool
	err  error
}
