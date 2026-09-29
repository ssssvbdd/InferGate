package core

import (
	"context"
	"time"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/gwerr"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/metrics"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/provider"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/retry"
)

func errRateLimited() error { return gwerr.ErrRateLimited }

// ForwardRequest 是非流式请求转发总入口，执行 5 步核心流程。
func (g *Gateway) ForwardRequest(ctx context.Context, req *model.Request) (resp *model.Response, err error) {
	mc, sel, err := g.prepare(ctx, req)
	if err != nil {
		return nil, err
	}
	defer sel.Release(ctx)

	timing := metrics.TimingMetrics{StartTime: time.Now()}
	metrics.IncConcurrency(mc.Name)

	// 指标上报（defer 确保执行）。
	defer func() {
		timing.EndTime = time.Now()
		metrics.DecConcurrency(mc.Name)
		errCode := ""
		if err != nil {
			_, ge := gwerr.HandleError(err)
			if ge != nil {
				errCode = ge.Code
			}
		}
		if resp != nil && resp.Usage != nil {
			timing.PromptTokens = resp.Usage.PromptTokens
			timing.CompletionTokens = resp.Usage.CompletionTokens
			timing.FirstTokenAt = timing.EndTime // 非流式以结束近似首包
		}
		dim := buildDimension(mc, sel, req, g.selectorManager, errCode)
		g.selectorManager.RecordResult(sel.CBKey(), err)
		go metrics.Report(dim, timing)
	}()

	// 模型调用（带智能重试）。
	hooks := g.newHooks(mc)
	callCtx, cancel := context.WithTimeout(ctx, g.callTimeout(mc))
	defer cancel()

	resp, err = retry.WithRetry(callCtx, retryOption(mc),
		func(rctx context.Context, attempt int) (*model.Response, error) {
			mp, perr := provider.NewModelProvider(rctx, provider.Instance{
				Endpoint: sel.Endpoint,
				Model:    mc,
			}, hooks)
			if perr != nil {
				return nil, perr
			}
			return mp.SendRequest(rctx, req)
		})
	return resp, err
}

// StreamForwardRequest 是流式请求转发总入口，返回 StreamChunk channel。
func (g *Gateway) StreamForwardRequest(ctx context.Context, req *model.Request) (<-chan *model.StreamChunk, func(), error) {
	req.Stream = true
	mc, sel, err := g.prepare(ctx, req)
	if err != nil {
		return nil, nil, err
	}

	timing := metrics.TimingMetrics{StartTime: time.Now()}
	metrics.IncConcurrency(mc.Name)

	hooks := g.newHooks(mc)
	mp, err := provider.NewModelProvider(ctx, provider.Instance{
		Endpoint: sel.Endpoint,
		Model:    mc,
	}, hooks)
	if err != nil {
		sel.Release(ctx)
		metrics.DecConcurrency(mc.Name)
		return nil, nil, err
	}

	upstream, err := mp.StreamRequest(ctx, req)
	if err != nil {
		sel.Release(ctx)
		metrics.DecConcurrency(mc.Name)
		return nil, nil, err
	}

	out := make(chan *model.StreamChunk, 32)
	go func() {
		defer close(out)
		var firstToken bool
		var streamErr error
		for chunk := range upstream {
			if !firstToken && len(chunk.Data) > 0 {
				timing.FirstTokenAt = time.Now()
				firstToken = true
			}
			if chunk.Usage != nil {
				timing.PromptTokens = chunk.Usage.PromptTokens
				timing.CompletionTokens = chunk.Usage.CompletionTokens
			}
			if chunk.Err != nil {
				streamErr = chunk.Err
			}
			select {
			case out <- chunk:
			case <-ctx.Done():
				streamErr = gwerr.ErrTimeout.WithCause(ctx.Err())
			}
		}
		// 上报。
		timing.EndTime = time.Now()
		metrics.DecConcurrency(mc.Name)
		errCode := ""
		if streamErr != nil {
			if _, ge := gwerr.HandleError(streamErr); ge != nil {
				errCode = ge.Code
			}
		}
		dim := buildDimension(mc, sel, req, g.selectorManager, errCode)
		g.selectorManager.RecordResult(sel.CBKey(), streamErr)
		metrics.Report(dim, timing)
	}()

	cleanup := func() { sel.Release(ctx) }
	return out, cleanup, nil
}
