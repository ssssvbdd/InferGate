// Package api provides the authenticated OpenAI-compatible HTTP data plane.
package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/auth"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/core"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/gwerr"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/health"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/log"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/metrics"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/registry"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/usage"
	"go.uber.org/zap"
)

const (
	maxRequestBody = 10 * 1024 * 1024
	identityKey    = "authenticated_identity"
)

type Handler struct {
	gw       *core.Gateway
	auth     *auth.Manager
	registry *registry.ModelRegistry
	ledger   *usage.Ledger
	health   *health.Checker
}

func NewHandler(gw *core.Gateway, authManager *auth.Manager, reg *registry.ModelRegistry,
	ledger *usage.Ledger, checker *health.Checker) *Handler {
	return &Handler{gw: gw, auth: authManager, registry: reg, ledger: ledger, health: checker}
}

func (h *Handler) RegisterRoutes(r *gin.Engine) {
	r.GET("/healthz", func(c *gin.Context) { c.String(http.StatusOK, "ok") })
	r.GET("/readyz", func(c *gin.Context) {
		if !h.health.Ready(c.Request.Context()) {
			c.String(http.StatusServiceUnavailable, "unavailable")
			return
		}
		c.String(http.StatusOK, "ready")
	})

	v1 := r.Group("/v1", h.authenticate())
	{
		v1.GET("/models", h.models)
		v1.POST("/chat/completions", h.chatCompletions)
		v1.POST("/embeddings", h.embeddings)
	}
}

func (h *Handler) authenticate() gin.HandlerFunc {
	return func(c *gin.Context) {
		header := c.GetHeader("Authorization")
		if !strings.HasPrefix(header, "Bearer ") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": gin.H{"message": "invalid API key", "type": "authentication_error"}})
			return
		}
		identity, err := h.auth.Authenticate(strings.TrimPrefix(header, "Bearer "))
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": gin.H{"message": "invalid API key", "type": "authentication_error"}})
			return
		}
		retryAfter, err := h.auth.Acquire(identity)
		if err != nil {
			status := http.StatusTooManyRequests
			if errors.Is(err, auth.ErrGlobalBusy) {
				status = http.StatusServiceUnavailable
			}
			c.Header("Retry-After", strconv.Itoa(max(1, int(retryAfter.Seconds()))))
			c.AbortWithStatusJSON(status, gin.H{"error": gin.H{"message": err.Error(), "type": "rate_limit_error"}})
			return
		}
		defer h.auth.Release(identity)
		c.Set(identityKey, identity)
		c.Next()
	}
}

func (h *Handler) models(c *gin.Context) {
	items := make([]gin.H, 0)
	for _, name := range h.registry.ListNames() {
		items = append(items, gin.H{"id": name, "object": "model", "owned_by": "infergate"})
	}
	c.JSON(http.StatusOK, gin.H{"object": "list", "data": items})
}

func (h *Handler) chatCompletions(c *gin.Context) {
	req, err := parseRequest(c)
	if err != nil {
		writeError(c, err)
		return
	}
	identity := authenticatedIdentity(c)
	prepareRequest(c, req, identity)
	metrics.ObserveWorkload(req)
	if req.Stream {
		h.handleStream(c, req, identity)
		return
	}
	resp, err := h.gw.ForwardRequest(c.Request.Context(), req)
	if err != nil {
		writeError(c, err)
		h.record(c, req, identity, nil)
		return
	}
	writeResponse(c, resp)
	h.record(c, req, identity, resp.Usage)
}

func (h *Handler) embeddings(c *gin.Context) {
	req, err := parseRequest(c)
	if err != nil {
		writeError(c, err)
		return
	}
	identity := authenticatedIdentity(c)
	prepareRequest(c, req, identity)
	req.Stream = false
	metrics.ObserveWorkload(req)
	resp, err := h.gw.ForwardRequest(c.Request.Context(), req)
	if err != nil {
		writeError(c, err)
		h.record(c, req, identity, nil)
		return
	}
	writeResponse(c, resp)
	h.record(c, req, identity, resp.Usage)
}

func (h *Handler) handleStream(c *gin.Context, req *model.Request, identity *auth.Identity) {
	stream, cleanup, err := h.gw.StreamForwardRequest(c.Request.Context(), req)
	if err != nil {
		writeError(c, err)
		h.record(c, req, identity, nil)
		return
	}
	if cleanup != nil {
		defer cleanup()
	}
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.WriteHeader(http.StatusOK)
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		writeError(c, gwerr.ErrInternal)
		return
	}
	var lastUsage *model.Usage
	defer func() { h.record(c, req, identity, lastUsage) }()
	for chunk := range stream {
		if chunk.Usage != nil {
			lastUsage = chunk.Usage
		}
		if chunk.Err != nil {
			_, ge := gwerr.HandleError(chunk.Err)
			payload, _ := json.Marshal(gin.H{"error": errorBody(ge)})
			_, _ = c.Writer.Write([]byte("data: "))
			_, _ = c.Writer.Write(payload)
			_, _ = c.Writer.Write([]byte("\n\n"))
			flusher.Flush()
			return
		}
		if chunk.Done {
			_, _ = c.Writer.Write([]byte("data: [DONE]\n\n"))
			flusher.Flush()
			return
		}
		if len(chunk.Data) > 0 {
			_, _ = c.Writer.Write([]byte("data: "))
			_, _ = c.Writer.Write(chunk.Data)
			_, _ = c.Writer.Write([]byte("\n\n"))
			flusher.Flush()
		}
	}
}

func parseRequest(c *gin.Context) (*model.Request, error) {
	body, err := io.ReadAll(io.LimitReader(c.Request.Body, maxRequestBody+1))
	if err != nil || len(body) > maxRequestBody {
		return nil, gwerr.ErrInvalidRequest.WithCause(errors.New("request body exceeds 10 MiB"))
	}
	req := &model.Request{}
	if err := json.Unmarshal(body, req); err != nil {
		return nil, gwerr.ErrInvalidRequest.WithCause(err)
	}
	return req, nil
}

func authenticatedIdentity(c *gin.Context) *auth.Identity {
	value, _ := c.Get(identityKey)
	identity, _ := value.(*auth.Identity)
	return identity
}

func prepareRequest(c *gin.Context, req *model.Request, identity *auth.Identity) {
	req.Metadata = extractMetadata(c)
	if identity != nil {
		req.Metadata["tenant"] = identity.Tenant
		req.CacheSalt = identity.CacheSalt
	}
}

func extractMetadata(c *gin.Context) map[string]any {
	meta := map[string]any{}
	if v := c.GetHeader("X-Model-Version"); v != "" {
		meta["version"] = v
	}
	if v := c.GetHeader("X-Thinking"); v == "true" {
		meta["thinking"] = true
	}
	return meta
}

func (h *Handler) record(c *gin.Context, req *model.Request, identity *auth.Identity, tokenUsage *model.Usage) {
	if h.ledger == nil || identity == nil || req == nil {
		return
	}
	event := usage.Event{Timestamp: time.Now().UTC(), RequestID: c.GetString("request_id"),
		Tenant: identity.Tenant, Route: c.FullPath(), Model: req.Model,
		Status: c.Writer.Status(), Stream: req.Stream}
	if tokenUsage != nil {
		event.PromptTokens = tokenUsage.PromptTokens
		event.CompletionTokens = tokenUsage.CompletionTokens
		event.Metered = true
		metrics.ReportTenantUsage(identity.Tenant, tokenUsage)
	}
	if err := h.ledger.Record(event); err != nil {
		log.Error("usage ledger write failed", zap.Error(err), zap.String("request_id", event.RequestID))
	}
}

func writeResponse(c *gin.Context, resp *model.Response) {
	c.Header("Content-Type", "application/json")
	if len(resp.Raw) > 0 {
		c.Status(http.StatusOK)
		_, _ = c.Writer.Write(resp.Raw)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func writeError(c *gin.Context, err error) {
	status, ge := gwerr.HandleError(err)
	c.JSON(status, gin.H{"error": errorBody(ge)})
}

func errorBody(ge *gwerr.GatewayError) gin.H {
	if ge == nil {
		return gin.H{"message": "unknown error", "type": "internal_error"}
	}
	return gin.H{"message": ge.Message, "type": string(ge.Type), "code": ge.Code}
}
