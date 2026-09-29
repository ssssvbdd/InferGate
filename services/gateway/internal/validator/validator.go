// Package validator 校验并规整入站请求参数。
package validator

import (
	"github.com/ssssvbdd/InferGate/services/gateway/internal/gwerr"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
)

// RequestValidator 定义请求校验接口。
type RequestValidator interface {
	Validate(req *model.Request) error
}

// DefaultValidator 是默认实现：对 MaxTokens/TopP 等做零值处理与默认值校正。
type DefaultValidator struct{}

// New 创建默认校验器。
func New() RequestValidator { return &DefaultValidator{} }

// Validate 校验请求。
func (v *DefaultValidator) Validate(req *model.Request) error {
	if req == nil {
		return gwerr.ErrInvalidRequest.WithCause(nil)
	}
	if req.Model == "" {
		return gwerr.ErrInvalidRequest.WithCause(errString("model is required"))
	}

	// MaxTokens 为 0 视为未设置，置 nil（交由上游默认）。
	if req.MaxTokens != nil && *req.MaxTokens <= 0 {
		req.MaxTokens = nil
	}
	if req.MaxCompletionTokens != nil && *req.MaxCompletionTokens <= 0 {
		req.MaxCompletionTokens = nil
	}
	// TopP 为 0 视为未设置。
	if req.TopP != nil && *req.TopP <= 0 {
		req.TopP = nil
	}
	// Temperature 合法区间 [0, 2]。
	if req.Temperature != nil {
		if *req.Temperature < 0 {
			z := 0.0
			req.Temperature = &z
		} else if *req.Temperature > 2 {
			m := 2.0
			req.Temperature = &m
		}
	}
	// N 至少为 1。
	if req.N != nil && *req.N < 1 {
		one := 1
		req.N = &one
	}
	// ParallelToolCalls 默认 true（当带 tools 时）。
	if req.ParallelToolCalls == nil && len(req.Tools) > 0 {
		t := true
		req.ParallelToolCalls = &t
	}
	return nil
}

type errString string

func (e errString) Error() string { return string(e) }
