package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/gwerr"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
)

// openAICompatAdapter 适配所有 OpenAI 兼容协议的提供商
// （OpenAI 官方、vLLM/TGI/LocalAI 等开源推理服务、Ollama 的 OpenAI 端点）。
type openAICompatAdapter struct {
	inst   Instance
	cli    *http.Client
	apiKey string
	// allowInternal 表示是否允许连接内网（仅用于自建/本地推理）。
	allowInternal bool
}

func newOpenAICompatAdapter() ProviderAdapter { return &openAICompatAdapter{} }

func (a *openAICompatAdapter) Create(ctx context.Context, inst Instance) error {
	a.inst = inst
	a.apiKey = apiKeyFromEnv(inst.Model.Credential.APIKeyEnv)
	// 本地推理（ollama/generic 指向内网）允许内网连接。
	a.allowInternal = inst.Model.ProviderType == model.ProviderOllama ||
		metaBool(inst.Endpoint.Metadata, "allow_internal")
	timeout := inst.Model.Timeout
	a.cli = httpClient(timeout, a.allowInternal)
	return nil
}

func (a *openAICompatAdapter) Close() error { return nil }

func (a *openAICompatAdapter) AdapterRequest(ctx context.Context, req *model.Request) ([]byte, error) {
	// 克隆并替换为上游真实模型名。
	cp := req.Clone()
	cp.Model = a.inst.Model.ProviderModel
	return json.Marshal(cp)
}

func (a *openAICompatAdapter) endpointURL() string {
	base := a.baseURL()
	switch a.inst.Model.ModelType {
	case model.ModelTypeEmbedding:
		return base + "/v1/embeddings"
	case model.ModelTypeRerank:
		return base + "/v1/rerank"
	default:
		return base + "/v1/chat/completions"
	}
}

func (a *openAICompatAdapter) baseURL() string {
	// 优先使用选中实例地址，否则回退到模型配置的 BaseURL。
	ep := a.inst.Endpoint
	if ep.Host != "" {
		scheme := ep.Scheme
		if scheme == "" {
			scheme = "https"
		}
		if ep.Port > 0 {
			return fmt.Sprintf("%s://%s:%d", scheme, ep.Host, ep.Port)
		}
		return fmt.Sprintf("%s://%s", scheme, ep.Host)
	}
	return strings.TrimRight(a.inst.Model.BaseURL, "/")
}

func (a *openAICompatAdapter) headers() map[string]string {
	h := map[string]string{}
	if a.apiKey != "" {
		h["Authorization"] = "Bearer " + a.apiKey
	}
	if org := a.inst.Model.Credential.Organization; org != "" {
		h["OpenAI-Organization"] = org
	}
	return h
}

func (a *openAICompatAdapter) Invoke(ctx context.Context, body []byte) (int, []byte, error) {
	return doJSON(ctx, a.cli, http.MethodPost, a.endpointURL(), a.headers(), body)
}

func (a *openAICompatAdapter) InvokeStream(ctx context.Context, body []byte, out chan<- rawChunk) error {
	return doStream(ctx, a.cli, a.endpointURL(), a.headers(), body, out)
}

func (a *openAICompatAdapter) AdapterError(statusCode int, body []byte) error {
	var t gwerr.ErrorType
	switch {
	case statusCode == http.StatusTooManyRequests:
		return gwerr.ErrRateLimited.WithCause(bodyErr(body))
	case statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden:
		return gwerr.ErrUnauthorized.WithCause(bodyErr(body))
	case statusCode == http.StatusNotFound:
		return gwerr.ErrModelNotFound.WithCause(bodyErr(body))
	case statusCode >= 500:
		return gwerr.ErrUpstream.WithCause(bodyErr(body))
	case statusCode >= 400:
		t = gwerr.TypeInvalidRequest
		return gwerr.New("upstream_bad_request", t, statusCode, string(body))
	default:
		return nil
	}
}

func (a *openAICompatAdapter) AdapterResponse(respBody []byte) (*model.Response, error) {
	resp := &model.Response{Raw: append([]byte(nil), respBody...)}
	// 解析用量与顶层字段（choices/data 保持透传）。
	var envelope struct {
		ID      string          `json:"id"`
		Object  string          `json:"object"`
		Created int64           `json:"created"`
		Model   string          `json:"model"`
		Choices json.RawMessage `json:"choices"`
		Data    json.RawMessage `json:"data"`
		Usage   *model.Usage    `json:"usage"`
	}
	if err := json.Unmarshal(respBody, &envelope); err != nil {
		// 上游返回非标准 JSON 时直接透传原始体。
		return resp, nil
	}
	resp.ID = envelope.ID
	resp.Object = envelope.Object
	resp.Created = envelope.Created
	resp.Model = envelope.Model
	resp.Choices = envelope.Choices
	resp.Data = envelope.Data
	resp.Usage = envelope.Usage
	return resp, nil
}

func (a *openAICompatAdapter) AdapterStreamResponse(raw rawChunk) (*model.StreamChunk, error) {
	if raw.err != nil {
		return &model.StreamChunk{Err: raw.err}, raw.err
	}
	if raw.done {
		return &model.StreamChunk{Done: true}, nil
	}
	chunk := &model.StreamChunk{Data: append([]byte(nil), raw.data...)}
	// 尝试解析末包 usage。
	var probe struct {
		Usage *model.Usage `json:"usage"`
	}
	if err := json.Unmarshal(raw.data, &probe); err == nil && probe.Usage != nil {
		chunk.Usage = probe.Usage
	}
	return chunk, nil
}

func metaBool(m map[string]string, key string) bool {
	if m == nil {
		return false
	}
	v := strings.ToLower(m[key])
	return v == "true" || v == "1" || v == "yes"
}

func bodyErr(body []byte) error {
	if len(body) == 0 {
		return nil
	}
	return fmt.Errorf("%s", string(body))
}
