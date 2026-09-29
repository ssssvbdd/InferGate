package api

import (
	"crypto/rand"
	"encoding/hex"
	"regexp"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/log"
	"go.uber.org/zap"
)

var requestIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,80}$`)

// RequestIDMiddleware validates a caller-provided request id or creates one.
func RequestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID := c.GetHeader("X-Request-ID")
		if !requestIDPattern.MatchString(requestID) {
			var raw [16]byte
			if _, err := rand.Read(raw[:]); err == nil {
				requestID = hex.EncodeToString(raw[:])
			} else {
				requestID = "request-id-unavailable"
			}
		}
		c.Set("request_id", requestID)
		c.Header("X-Request-ID", requestID)
		c.Next()
	}
}

// RecoveryMiddleware 捕获 panic，返回 500。
func RecoveryMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				log.Error("panic recovered", zap.Any("panic", r), zap.String("path", c.Request.URL.Path))
				c.AbortWithStatusJSON(500, gin.H{"error": gin.H{"message": "internal error", "type": "internal_error"}})
			}
		}()
		c.Next()
	}
}

// TracingMiddleware 为每个请求创建 span 并注入 context。
func TracingMiddleware() gin.HandlerFunc {
	tracer := otel.Tracer("infergate.http")
	return func(c *gin.Context) {
		ctx, span := tracer.Start(c.Request.Context(), c.Request.Method+" "+c.FullPath())
		defer span.End()
		c.Request = c.Request.WithContext(ctx)
		c.Next()
		span.SetAttributes()
		_ = trace.SpanFromContext(ctx)
	}
}

// AccessLogMiddleware 记录请求耗时。
func AccessLogMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		log.Info("http access",
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("cost", time.Since(start)),
		)
	}
}
