// Package admin 提供管理后台：模型注册、路由规则配置、凭证管理，使用 HMAC-SHA256 签名鉴权。
package admin

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/registry"
)

// Server 是管理后台。
type Server struct {
	reg       *registry.ModelRegistry
	secretKey []byte
}

// NewServer 创建管理后台。
func NewServer(reg *registry.ModelRegistry, secretKey string) *Server {
	return &Server{reg: reg, secretKey: []byte(secretKey)}
}

// RegisterRoutes 注册管理路由，全部经过 HMAC 签名校验。
func (s *Server) RegisterRoutes(r *gin.Engine) {
	g := r.Group("/admin", s.authMiddleware())
	{
		g.GET("/models/:name", s.getModel)
		g.POST("/models", s.registerModel)
	}
}

// authMiddleware 校验 HMAC-SHA256 签名。
// 客户端需提供：X-Timestamp（秒）、X-Signature = hex(HMAC-SHA256(secret, method+path+timestamp))。
func (s *Server) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		ts := c.GetHeader("X-Timestamp")
		sig := c.GetHeader("X-Signature")
		if ts == "" || sig == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing signature"})
			return
		}
		// 防重放：时间戳偏差不超过 5 分钟。
		tsi, err := strconv.ParseInt(ts, 10, 64)
		if err != nil || absDuration(time.Since(time.Unix(tsi, 0))) > 5*time.Minute {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid or expired timestamp"})
			return
		}
		expected := s.sign(c.Request.Method + c.Request.URL.Path + ts)
		if subtle.ConstantTimeCompare([]byte(sig), []byte(expected)) != 1 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid signature"})
			return
		}
		c.Next()
	}
}

func (s *Server) sign(payload string) string {
	mac := hmac.New(sha256.New, s.secretKey)
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// getModel 查询模型配置。
func (s *Server) getModel(c *gin.Context) {
	name := c.Param("name")
	models, ok := s.reg.GetModels(context.Background(), name)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "model not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"name": name, "versions": models})
}

// registerModel 注册/更新模型配置。
func (s *Server) registerModel(c *gin.Context) {
	var cfg model.ModelConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if cfg.Name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name is required"})
		return
	}
	s.reg.Register(cfg)
	_ = s.reg.Persist(context.Background(), cfg.Name)
	c.JSON(http.StatusOK, gin.H{"status": "registered", "name": cfg.Name})
}
