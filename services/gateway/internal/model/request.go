// Package model 定义网关的核心数据模型：请求、响应、流式 chunk 以及模型/实例配置。
package model

import (
	"encoding/json"
	"unicode"
)

// ModelType 表示模型类型。
type ModelType string

const (
	ModelTypeLLM       ModelType = "llm"       // 大语言模型（chat）
	ModelTypeEmbedding ModelType = "embedding" // 文本嵌入
	ModelTypeRerank    ModelType = "rerank"    // 重排序
)

// ProviderType 表示提供商类型（开源/公开可用的提供商）。
type ProviderType string

const (
	ProviderOpenAI    ProviderType = "openai"
	ProviderAnthropic ProviderType = "anthropic"
	ProviderOllama    ProviderType = "ollama"
	// ProviderGeneric 使用 OpenAI 兼容协议，覆盖大量开源推理服务（vLLM、TGI、LocalAI 等）。
	ProviderGeneric ProviderType = "generic"
)

// Message 是一条对话消息。
type Message struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content,omitempty"`
	Name    string          `json:"name,omitempty"`
	// ToolCalls 承载模型返回的工具调用。
	ToolCalls  json.RawMessage `json:"tool_calls,omitempty"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
}

// Request 是网关统一的入站请求模型（兼容 OpenAI Chat Completions 语义）。
type Request struct {
	// Model 是用户请求的模型名（逻辑名，非上游真实模型）。
	Model    string    `json:"model"`
	Messages []Message `json:"messages,omitempty"`
	// Input 用于 embedding/rerank。
	Input any `json:"input,omitempty"`

	Stream              bool     `json:"stream,omitempty"`
	MaxTokens           *int     `json:"max_tokens,omitempty"`
	MaxCompletionTokens *int     `json:"max_completion_tokens,omitempty"`
	Temperature         *float64 `json:"temperature,omitempty"`
	TopP                *float64 `json:"top_p,omitempty"`
	N                   *int     `json:"n,omitempty"`
	Stop                any      `json:"stop,omitempty"`
	PresencePenalty     *float64 `json:"presence_penalty,omitempty"`
	FrequencyPenalty    *float64 `json:"frequency_penalty,omitempty"`

	Tools             json.RawMessage `json:"tools,omitempty"`
	ToolChoice        json.RawMessage `json:"tool_choice,omitempty"`
	ParallelToolCalls *bool           `json:"parallel_tool_calls,omitempty"`
	ResponseFormat    json.RawMessage `json:"response_format,omitempty"`
	StreamOptions     json.RawMessage `json:"stream_options,omitempty"`
	// CacheSalt is always overwritten from the authenticated tenant. A caller
	// cannot choose a salt and share prefix-cache entries across tenants.
	CacheSalt string `json:"cache_salt,omitempty"`

	User string `json:"user,omitempty"`

	// Metadata 是网关内部使用的元数据，不透传到上游。
	Metadata map[string]any `json:"-"`
	// Extra 保存未显式建模的透传字段。
	Extra map[string]json.RawMessage `json:"-"`
}

var requestKnownFields = map[string]struct{}{
	"model": {}, "messages": {}, "input": {}, "stream": {}, "max_tokens": {},
	"max_completion_tokens": {}, "temperature": {}, "top_p": {}, "n": {},
	"stop": {}, "presence_penalty": {}, "frequency_penalty": {}, "tools": {},
	"tool_choice": {}, "parallel_tool_calls": {}, "response_format": {},
	"stream_options": {}, "cache_salt": {}, "user": {},
}

// UnmarshalJSON keeps provider-specific OpenAI fields so the gateway remains
// protocol compatible when vLLM adds request options before this struct does.
func (r *Request) UnmarshalJSON(data []byte) error {
	type plain Request
	var decoded plain
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	var all map[string]json.RawMessage
	if err := json.Unmarshal(data, &all); err != nil {
		return err
	}
	for name := range requestKnownFields {
		delete(all, name)
	}
	*r = Request(decoded)
	if len(all) > 0 {
		r.Extra = all
	}
	return nil
}

// MarshalJSON merges unknown fields back without allowing them to override
// validated or tenant-controlled fields.
func (r Request) MarshalJSON() ([]byte, error) {
	type plain Request
	known, err := json.Marshal(plain(r))
	if err != nil {
		return nil, err
	}
	var output map[string]json.RawMessage
	if err := json.Unmarshal(known, &output); err != nil {
		return nil, err
	}
	for name, value := range r.Extra {
		if _, protected := requestKnownFields[name]; protected {
			continue
		}
		if _, exists := output[name]; !exists {
			output[name] = value
		}
	}
	return json.Marshal(output)
}

// Clone 返回请求的浅拷贝（Metadata/Extra 复制 map，切片共享底层）。
func (r *Request) Clone() *Request {
	if r == nil {
		return nil
	}
	cp := *r
	if r.Metadata != nil {
		cp.Metadata = make(map[string]any, len(r.Metadata))
		for k, v := range r.Metadata {
			cp.Metadata[k] = v
		}
	}
	if r.Extra != nil {
		cp.Extra = make(map[string]json.RawMessage, len(r.Extra))
		for k, v := range r.Extra {
			cp.Extra[k] = v
		}
	}
	return &cp
}

// EstimatedPromptTokens is a privacy-safe routing estimate. Actual accounting
// always uses the usage returned by the model server.
func EstimatedPromptTokens(r *Request) int {
	if r == nil {
		return 0
	}
	total := 0
	for _, message := range r.Messages {
		var content any
		if len(message.Content) > 0 && json.Unmarshal(message.Content, &content) == nil {
			total += estimateContent(content)
		}
	}
	return total
}

func estimateContent(value any) int {
	switch typed := value.(type) {
	case string:
		cjk, ascii, other := 0, 0, 0
		for _, r := range typed {
			switch {
			case unicode.Is(unicode.Han, r):
				cjk++
			case r < 128:
				if !unicode.IsSpace(r) {
					ascii++
				}
			case !unicode.IsSpace(r):
				other++
			}
		}
		return cjk + other + (ascii+3)/4
	case []any:
		total := 0
		for _, item := range typed {
			total += estimateContent(item)
		}
		return total
	case map[string]any:
		total := 0
		if text, ok := typed["text"]; ok {
			total += estimateContent(text)
		}
		if content, ok := typed["content"]; ok {
			total += estimateContent(content)
		}
		return total
	default:
		return 0
	}
}

// Usage 是 token 用量。
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// Response 是网关统一的非流式响应模型。
type Response struct {
	ID      string          `json:"id,omitempty"`
	Object  string          `json:"object,omitempty"`
	Created int64           `json:"created,omitempty"`
	Model   string          `json:"model,omitempty"`
	Choices json.RawMessage `json:"choices,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
	Usage   *Usage          `json:"usage,omitempty"`
	// Raw 保存上游原始响应体，用于透传。
	Raw json.RawMessage `json:"-"`
}

// StreamChunk 是流式响应的一个数据块。
type StreamChunk struct {
	// Data 是一个 SSE data 行的负载（已是网关统一格式）。
	Data json.RawMessage
	// Usage 在末包（若上游提供）携带 token 用量。
	Usage *Usage
	// Done 标记流结束（对应 SSE 的 [DONE]）。
	Done bool
	// Err 承载流式过程中出现的错误。
	Err error
}
