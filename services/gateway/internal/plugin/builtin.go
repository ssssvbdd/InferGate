package plugin

import (
	"context"
	"time"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/log"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

func init() {
	Register("tracing", func() Plugin { return &TracingPlugin{} })
	Register("thinking", func() Plugin { return &ThinkingPlugin{} })
	Register("reasoning", func() Plugin { return &ReasoningPlugin{} })
	Register("increment", func() Plugin { return &IncrementPlugin{} })
	Register("access_log", func() Plugin { return &AccessLogPlugin{} })
}

// TracingPlugin 在 PreRequest 创建 span，PostRequest 结束 span（对应可观测性钩子）。
type TracingPlugin struct {
	BasePlugin
	span    trace.Span
	started time.Time
}

// Name 返回插件名。
func (p *TracingPlugin) Name() string { return "tracing" }

// PreRequest 创建 span。
func (p *TracingPlugin) PreRequest(ctx context.Context, req *model.Request) error {
	tracer := otel.Tracer("infergate")
	_, span := tracer.Start(ctx, "llm.request")
	span.SetAttributes(
		attribute.String("llm.model", req.Model),
		attribute.Bool("llm.stream", req.Stream),
	)
	p.span = span
	p.started = time.Now()
	return nil
}

// PostRequest 结束 span 并记录错误。
func (p *TracingPlugin) PostRequest(ctx context.Context, resp *model.Response, callErr error) error {
	if p.span == nil {
		return nil
	}
	if callErr != nil {
		p.span.RecordError(callErr)
	}
	if resp != nil && resp.Usage != nil {
		p.span.SetAttributes(
			attribute.Int("llm.prompt_tokens", resp.Usage.PromptTokens),
			attribute.Int("llm.completion_tokens", resp.Usage.CompletionTokens),
		)
	}
	p.span.End()
	return nil
}

// ThinkingPlugin 为支持思考模式的模型注入配置（示例：通过 metadata 开启）。
type ThinkingPlugin struct{ BasePlugin }

// Name 返回插件名。
func (p *ThinkingPlugin) Name() string { return "thinking" }

// PreRequest 根据请求 metadata 配置思考模式。
func (p *ThinkingPlugin) PreRequest(ctx context.Context, req *model.Request) error {
	if req.Metadata == nil {
		return nil
	}
	if v, ok := req.Metadata["thinking"].(bool); ok && v {
		log.Debug("thinking mode enabled", zap.String("model", req.Model))
	}
	return nil
}

// ReasoningPlugin 在流式响应中解析思维链（示例：仅记录，不修改内容）。
type ReasoningPlugin struct{ BasePlugin }

// Name 返回插件名。
func (p *ReasoningPlugin) Name() string { return "reasoning" }

// OnStream 处理推理链 chunk。
func (p *ReasoningPlugin) OnStream(ctx context.Context, chunk *model.StreamChunk) (bool, error) {
	// 示例：此处可解析 reasoning_content 字段，当前实现透传不短路。
	return false, nil
}

// IncrementPlugin 对流式响应做增量去重（示例：跳过完全重复的相邻 chunk）。
type IncrementPlugin struct {
	BasePlugin
	last []byte
}

// Name 返回插件名。
func (p *IncrementPlugin) Name() string { return "increment" }

// OnStream 相邻重复 chunk 跳过。
func (p *IncrementPlugin) OnStream(ctx context.Context, chunk *model.StreamChunk) (bool, error) {
	if chunk.Done || len(chunk.Data) == 0 {
		return false, nil
	}
	if p.last != nil && string(p.last) == string(chunk.Data) {
		return true, nil // 短路跳过重复块
	}
	p.last = append(p.last[:0], chunk.Data...)
	return false, nil
}

// AccessLogPlugin 记录请求访问日志。
type AccessLogPlugin struct {
	BasePlugin
	model string
	start time.Time
}

// Name 返回插件名。
func (p *AccessLogPlugin) Name() string { return "access_log" }

// PreRequest 记录开始时间。
func (p *AccessLogPlugin) PreRequest(ctx context.Context, req *model.Request) error {
	p.model = req.Model
	p.start = time.Now()
	return nil
}

// PostRequest 输出访问日志。
func (p *AccessLogPlugin) PostRequest(ctx context.Context, resp *model.Response, callErr error) error {
	fields := []zap.Field{
		zap.String("model", p.model),
		zap.Duration("cost", time.Since(p.start)),
	}
	if callErr != nil {
		fields = append(fields, zap.Error(callErr))
		log.Warn("llm request failed", fields...)
	} else {
		log.Info("llm request ok", fields...)
	}
	return nil
}
