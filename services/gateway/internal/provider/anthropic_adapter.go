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

// anthropicAdapter 适配 Anthropic Messages API。
type anthropicAdapter struct {
	inst   Instance
	cli    *http.Client
	apiKey string
}

func newAnthropicAdapter() ProviderAdapter { return &anthropicAdapter{} }

const anthropicVersion = "2023-06-01"

func (a *anthropicAdapter) Create(ctx context.Context, inst Instance) error {
	a.inst = inst
	a.apiKey = apiKeyFromEnv(inst.Model.Credential.APIKeyEnv)
	a.cli = httpClient(inst.Model.Timeout, false)
	return nil
}

func (a *anthropicAdapter) Close() error { return nil }

func (a *anthropicAdapter) baseURL() string {
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
	if a.inst.Model.BaseURL != "" {
		return strings.TrimRight(a.inst.Model.BaseURL, "/")
	}
	return "https://api.anthropic.com"
}

// AdapterRequest 将 OpenAI 风格请求转换为 Anthropic Messages 格式。
func (a *anthropicAdapter) AdapterRequest(ctx context.Context, req *model.Request) ([]byte, error) {
	type anthropicMessage struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	}
	out := map[string]any{
		"model":  a.inst.Model.ProviderModel,
		"stream": req.Stream,
	}

	// system 消息单独提取。
	var msgs []anthropicMessage
	var systemParts []string
	for _, m := range req.Messages {
		if m.Role == "system" {
			var s string
			if json.Unmarshal(m.Content, &s) == nil {
				systemParts = append(systemParts, s)
			}
			continue
		}
		role := m.Role
		if role == "assistant" || role == "user" {
			msgs = append(msgs, anthropicMessage{Role: role, Content: m.Content})
		}
	}
	out["messages"] = msgs
	if len(systemParts) > 0 {
		out["system"] = strings.Join(systemParts, "\n")
	}

	// Anthropic 要求 max_tokens 必填。
	if req.MaxTokens != nil {
		out["max_tokens"] = *req.MaxTokens
	} else {
		out["max_tokens"] = 4096
	}
	if req.Temperature != nil {
		out["temperature"] = *req.Temperature
	}
	if req.TopP != nil {
		out["top_p"] = *req.TopP
	}
	return json.Marshal(out)
}

func (a *anthropicAdapter) headers() map[string]string {
	h := map[string]string{
		"anthropic-version": anthropicVersion,
	}
	if a.apiKey != "" {
		h["x-api-key"] = a.apiKey
	}
	return h
}

func (a *anthropicAdapter) Invoke(ctx context.Context, body []byte) (int, []byte, error) {
	return doJSON(ctx, a.cli, http.MethodPost, a.baseURL()+"/v1/messages", a.headers(), body)
}

func (a *anthropicAdapter) InvokeStream(ctx context.Context, body []byte, out chan<- rawChunk) error {
	return doStream(ctx, a.cli, a.baseURL()+"/v1/messages", a.headers(), body, out)
}

func (a *anthropicAdapter) AdapterError(statusCode int, body []byte) error {
	switch {
	case statusCode == http.StatusTooManyRequests:
		return gwerr.ErrRateLimited.WithCause(bodyErr(body))
	case statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden:
		return gwerr.ErrUnauthorized.WithCause(bodyErr(body))
	case statusCode >= 500:
		return gwerr.ErrUpstream.WithCause(bodyErr(body))
	case statusCode >= 400:
		return gwerr.New("upstream_bad_request", gwerr.TypeInvalidRequest, statusCode, string(body))
	default:
		return nil
	}
}

func (a *anthropicAdapter) AdapterResponse(respBody []byte) (*model.Response, error) {
	resp := &model.Response{Raw: append([]byte(nil), respBody...), Object: "chat.completion"}
	var envelope struct {
		ID    string `json:"id"`
		Model string `json:"model"`
		Usage struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(respBody, &envelope); err == nil {
		resp.ID = envelope.ID
		resp.Model = envelope.Model
		resp.Usage = &model.Usage{
			PromptTokens:     envelope.Usage.InputTokens,
			CompletionTokens: envelope.Usage.OutputTokens,
			TotalTokens:      envelope.Usage.InputTokens + envelope.Usage.OutputTokens,
		}
	}
	return resp, nil
}

func (a *anthropicAdapter) AdapterStreamResponse(raw rawChunk) (*model.StreamChunk, error) {
	if raw.err != nil {
		return &model.StreamChunk{Err: raw.err}, raw.err
	}
	if raw.done {
		return &model.StreamChunk{Done: true}, nil
	}
	return &model.StreamChunk{Data: append([]byte(nil), raw.data...)}, nil
}
