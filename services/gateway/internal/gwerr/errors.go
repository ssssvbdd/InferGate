// Package gwerr 定义网关三层错误结构：GatewayError（网关层）、APIError（接口层）、RequestError（HTTP 层）。
package gwerr

import (
	"errors"
	"fmt"
	"net/http"
)

// ErrorType 是错误分类。
type ErrorType string

const (
	TypeInvalidRequest ErrorType = "invalid_request_error"
	TypeAuth           ErrorType = "authentication_error"
	TypePermission     ErrorType = "permission_error"
	TypeNotFound       ErrorType = "not_found_error"
	TypeRateLimit      ErrorType = "rate_limit_error"
	TypeOverloaded     ErrorType = "overloaded_error"
	TypeUpstream       ErrorType = "upstream_error"
	TypeTimeout        ErrorType = "timeout_error"
	TypeInternal       ErrorType = "internal_error"
)

// GatewayError 是网关核心层错误。
type GatewayError struct {
	Code           string
	Message        string
	Param          string
	Type           ErrorType
	HTTPStatusCode int
	ID             string
	// wrapped 保存底层错误，支持错误链。
	wrapped error
}

// Error 实现 error 接口。
func (e *GatewayError) Error() string {
	if e.wrapped != nil {
		return fmt.Sprintf("[%s] %s: %v", e.Code, e.Message, e.wrapped)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// Unwrap 支持 errors.Is/As 错误链解包。
func (e *GatewayError) Unwrap() error { return e.wrapped }

// WithCause 附加底层错误。
func (e *GatewayError) WithCause(err error) *GatewayError {
	ne := *e
	ne.wrapped = err
	return &ne
}

// New 创建一个 GatewayError。
func New(code string, t ErrorType, httpStatus int, msg string) *GatewayError {
	return &GatewayError{Code: code, Type: t, HTTPStatusCode: httpStatus, Message: msg}
}

// APIError 是接口层错误，包含链路追踪 ID。
type APIError struct {
	*GatewayError
	RequestID string
}

// RequestError 是 HTTP 层错误。
type RequestError struct {
	StatusCode int
	Body       string
	err        error
}

func (e *RequestError) Error() string {
	return fmt.Sprintf("http %d: %s", e.StatusCode, e.Body)
}

// Unwrap 支持错误解包。
func (e *RequestError) Unwrap() error { return e.err }

// NewRequestError 构造 HTTP 层错误。
func NewRequestError(status int, body string, cause error) *RequestError {
	return &RequestError{StatusCode: status, Body: body, err: cause}
}

// 预定义错误。
var (
	ErrInvalidRequest = New("invalid_request", TypeInvalidRequest, http.StatusBadRequest, "invalid request")
	ErrModelNotFound  = New("model_not_found", TypeNotFound, http.StatusNotFound, "model not found")
	ErrNoInstance     = New("no_instance", TypeOverloaded, http.StatusServiceUnavailable, "no healthy instance available")
	ErrRateLimited    = New("rate_limited", TypeRateLimit, http.StatusTooManyRequests, "rate limit exceeded")
	ErrUnauthorized   = New("unauthorized", TypeAuth, http.StatusUnauthorized, "unauthorized")
	ErrOverloaded     = New("overloaded", TypeOverloaded, http.StatusServiceUnavailable, "model overloaded")
	ErrUpstream       = New("upstream_error", TypeUpstream, http.StatusBadGateway, "upstream error")
	ErrTimeout        = New("timeout", TypeTimeout, http.StatusGatewayTimeout, "request timeout")
	ErrInternal       = New("internal_error", TypeInternal, http.StatusInternalServerError, "internal error")
)

// HandleError 是统一错误处理入口，将任意 error 归一化为 (httpStatus, GatewayError)。
func HandleError(err error) (int, *GatewayError) {
	if err == nil {
		return http.StatusOK, nil
	}
	var ge *GatewayError
	if errors.As(err, &ge) {
		status := ge.HTTPStatusCode
		if status == 0 {
			status = http.StatusInternalServerError
		}
		return status, ge
	}
	var re *RequestError
	if errors.As(err, &re) {
		return re.StatusCode, ErrUpstream.WithCause(err)
	}
	return http.StatusInternalServerError, ErrInternal.WithCause(err)
}

// IsRetryable 判断错误是否可重试（5xx、网络错误、429 可重试；4xx、上下文取消不可重试）。
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	var ge *GatewayError
	if errors.As(err, &ge) {
		switch ge.Type {
		case TypeUpstream, TypeOverloaded, TypeTimeout, TypeRateLimit:
			return true
		default:
			return false
		}
	}
	var re *RequestError
	if errors.As(err, &re) {
		return re.StatusCode >= 500 || re.StatusCode == http.StatusTooManyRequests
	}
	// 未知错误（多为网络错误）视为可重试。
	return true
}
