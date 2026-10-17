package api

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"regexp"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/log"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/metrics"
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
				log.Error("panic recovered", zap.String("panic_type", fmt.Sprintf("%T", r)),
					zap.String("path", c.Request.URL.Path), zap.String("request_id", c.GetString("request_id")))
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
		incoming := otel.GetTextMapPropagator().Extract(c.Request.Context(),
			propagation.HeaderCarrier(c.Request.Header))
		ctx, span := tracer.Start(incoming, c.Request.Method+" "+c.FullPath())
		defer span.End()
		c.Request = c.Request.WithContext(ctx)
		c.Next()
		span.SetAttributes(attribute.String("http.request.method", c.Request.Method),
			attribute.String("http.route", c.FullPath()),
			attribute.Int("http.response.status_code", c.Writer.Status()))
	}
}

// AccessLogMiddleware 记录请求耗时。
func AccessLogMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Set("request_started_at", start)
		c.Next()
		metrics.ReportHTTP(c.FullPath(), c.Writer.Status(), time.Since(start))
		spanContext := trace.SpanFromContext(c.Request.Context()).SpanContext()
		log.Info("http access",
			zap.String("request_id", c.GetString("request_id")),
			zap.String("trace_id", spanContext.TraceID().String()),
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("cost", time.Since(start)),
		)
	}
}
