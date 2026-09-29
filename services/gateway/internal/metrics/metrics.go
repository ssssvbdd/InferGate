// Package metrics 提供 Prometheus 指标与 11 维监控上报。
package metrics

import (
	"time"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// requestsTotal 请求总数。
	requestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "infergate",
		Name:      "requests_total",
		Help:      "Total number of LLM requests.",
	}, []string{"model", "provider", "model_type", "status"})

	// concurrency 并发数。
	concurrency = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "infergate",
		Name:      "concurrency",
		Help:      "Current concurrent LLM requests.",
	}, []string{"model"})

	// ttft 首包时延（秒）。
	ttft = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "infergate",
		Name:      "ttft_seconds",
		Help:      "Time to first token.",
		Buckets:   []float64{0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10},
	}, []string{"model", "provider"})

	// totalTime 总耗时（秒）。
	totalTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "infergate",
		Name:      "total_time_seconds",
		Help:      "Total request time.",
		Buckets:   []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60},
	}, []string{"model", "provider"})

	// tokenPerSecond 每秒 token 数。
	tokenPerSecond = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "infergate",
		Name:      "token_per_second",
		Help:      "Output tokens per second.",
		Buckets:   []float64{5, 10, 20, 40, 80, 160},
	}, []string{"model", "provider"})

	// promptTokens 输入 token 计数。
	promptTokens = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "infergate",
		Name:      "prompt_tokens_total",
		Help:      "Total prompt tokens.",
	}, []string{"model", "provider"})

	// completionTokens 输出 token 计数。
	completionTokens = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "infergate",
		Name:      "completion_tokens_total",
		Help:      "Total completion tokens.",
	}, []string{"model", "provider"})

	workloadRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "infergate", Name: "workload_requests_total",
		Help: "Requests grouped by estimated dominant workload."}, []string{"class", "stream"})
	estimatedPromptTokens = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "infergate", Name: "estimated_prompt_tokens_total",
		Help: "Privacy-safe prompt token estimates used for routing observations."})
	requestedOutputTokens = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "infergate", Name: "requested_output_tokens_total",
		Help: "Requested output token budget."})
	tenantPromptTokens = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "infergate", Name: "tenant_prompt_tokens_total",
		Help: "Metered prompt tokens by authenticated tenant."}, []string{"tenant"})
	tenantCompletionTokens = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "infergate", Name: "tenant_completion_tokens_total",
		Help: "Metered completion tokens by authenticated tenant."}, []string{"tenant"})
)

func ObserveWorkload(req *model.Request) {
	estimated := model.EstimatedPromptTokens(req)
	requested := 0
	if req != nil {
		if req.MaxTokens != nil {
			requested = *req.MaxTokens
		}
		if req.MaxCompletionTokens != nil {
			requested = *req.MaxCompletionTokens
		}
	}
	class := "balanced"
	if estimated >= 2048 {
		class = "prefill"
	} else if requested >= 256 {
		class = "decode"
	} else if estimated == 0 && requested == 0 {
		class = "unknown"
	}
	stream := "false"
	if req != nil && req.Stream {
		stream = "true"
	}
	workloadRequests.WithLabelValues(class, stream).Inc()
	estimatedPromptTokens.Add(float64(estimated))
	requestedOutputTokens.Add(float64(requested))
}

func ReportTenantUsage(tenant string, usage *model.Usage) {
	if tenant == "" || usage == nil {
		return
	}
	tenantPromptTokens.WithLabelValues(tenant).Add(float64(usage.PromptTokens))
	tenantCompletionTokens.WithLabelValues(tenant).Add(float64(usage.CompletionTokens))
}

// ChatDimension 是一次调用的监控维度（11 维）。
type ChatDimension struct {
	User                string
	Model               string
	Version             string
	ModelType           string
	ProviderName        string
	ProviderType        string
	ProviderModel       string
	Endpoint            string
	CircuitBreakerState string
	ErrorCode           string
	Metadata            map[string]string
}

// TimingMetrics 是性能计时指标。
type TimingMetrics struct {
	StartTime        time.Time
	FirstTokenAt     time.Time
	EndTime          time.Time
	PromptTokens     int
	CompletionTokens int
	Canceled         bool
	Timeout          bool
}

// TTFT 首包时延。
func (t TimingMetrics) TTFT() time.Duration {
	if t.FirstTokenAt.IsZero() {
		return 0
	}
	return t.FirstTokenAt.Sub(t.StartTime)
}

// TotalTime 总耗时。
func (t TimingMetrics) TotalTime() time.Duration {
	if t.EndTime.IsZero() {
		return time.Since(t.StartTime)
	}
	return t.EndTime.Sub(t.StartTime)
}

// TIT 推理耗时 = TotalTime - TTFT。
func (t TimingMetrics) TIT() time.Duration {
	tit := t.TotalTime() - t.TTFT()
	if tit < 0 {
		return 0
	}
	return tit
}

// TPOT 每 token 时延。
func (t TimingMetrics) TPOT() time.Duration {
	if t.CompletionTokens <= 0 {
		return 0
	}
	return t.TIT() / time.Duration(t.CompletionTokens)
}

// TokenPerSecond 每秒 token 数。
func (t TimingMetrics) TokenPerSecond() float64 {
	tit := t.TIT().Seconds()
	if tit <= 0 || t.CompletionTokens <= 0 {
		return 0
	}
	return float64(t.CompletionTokens) / tit
}

// IncConcurrency 并发 +1。
func IncConcurrency(model string) { concurrency.WithLabelValues(model).Inc() }

// DecConcurrency 并发 -1。
func DecConcurrency(model string) { concurrency.WithLabelValues(model).Dec() }

// Report 异步上报一次调用的全部指标（11 维）。
func Report(dim ChatDimension, timing TimingMetrics) {
	status := "ok"
	if dim.ErrorCode != "" {
		status = "error"
	}
	requestsTotal.WithLabelValues(dim.Model, dim.ProviderType, dim.ModelType, status).Inc()
	totalTime.WithLabelValues(dim.Model, dim.ProviderType).Observe(timing.TotalTime().Seconds())

	if ttftVal := timing.TTFT(); ttftVal > 0 {
		ttft.WithLabelValues(dim.Model, dim.ProviderType).Observe(ttftVal.Seconds())
	}
	if tps := timing.TokenPerSecond(); tps > 0 {
		tokenPerSecond.WithLabelValues(dim.Model, dim.ProviderType).Observe(tps)
	}
	if timing.PromptTokens > 0 {
		promptTokens.WithLabelValues(dim.Model, dim.ProviderType).Add(float64(timing.PromptTokens))
	}
	if timing.CompletionTokens > 0 {
		completionTokens.WithLabelValues(dim.Model, dim.ProviderType).Add(float64(timing.CompletionTokens))
	}
}
