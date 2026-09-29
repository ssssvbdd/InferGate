// Command server 是 infergate 的主入口。
package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/admin"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/api"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/auth"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/breaker"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/config"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/core"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/discovery"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/health"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/limiter"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/log"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/registry"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/router"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/selector"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/store"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/tracing"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/usage"
)

func main() {
	confPath := flag.String("conf", "config.yaml", "path to config file")
	modelsPath := flag.String("models", "models.yaml", "path to models config file")
	flag.Parse()

	cfg, err := config.Load(*confPath)
	if err != nil {
		panic(err)
	}

	if err := log.Init(cfg.Log.Level, cfg.Log.Encoding); err != nil {
		panic(err)
	}
	defer log.Sync()
	log.Info("starting infergate", zap.String("addr", cfg.Server.Addr))

	ctx := context.Background()

	// 初始化 Redis（可选）。
	if cfg.Redis.Enabled && cfg.Redis.Addr != "" {
		if err := store.InitRedis(cfg.Redis); err != nil {
			log.Warn("redis init failed, running in degraded mode", zap.Error(err))
		}
	}
	rdb := store.GetRedisClient()

	// 初始化追踪。
	shutdownTracing, err := tracing.Init(ctx, cfg.Tracing)
	if err != nil {
		log.Warn("tracing init failed", zap.Error(err))
	}
	defer func() { _ = shutdownTracing(context.Background()) }()

	// 组装网关。
	gw, reg := setupGateway(cfg, rdb, *modelsPath)
	authManager, err := auth.Load(cfg.Auth.KeysFile, cfg.Auth.CacheSaltSecretFile, cfg.Auth.MaxInFlight)
	if err != nil {
		panic(err)
	}
	ledger, err := usage.Open(cfg.Usage.LedgerFile)
	if err != nil {
		panic(err)
	}
	defer func() { _ = ledger.Close() }()
	checker := health.New(cfg.Readiness.URL, cfg.Readiness.Timeout)

	// 启动监控端口。
	if cfg.Metrics.Enabled {
		go serveMetrics(cfg.Metrics, checker)
	}

	// 启动管理后台。
	if cfg.Admin.Enabled {
		go serveAdmin(cfg.Admin, reg)
	}

	// 启动主 HTTP 服务。
	engine := setupHTTPServer(gw, authManager, reg, ledger, checker)
	srv := &http.Server{
		Addr:         cfg.Server.Addr,
		Handler:      engine,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server failed", zap.Error(err))
			os.Exit(1)
		}
	}()
	log.Info("infergate started", zap.String("addr", cfg.Server.Addr))

	// 优雅关闭。
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", zap.Error(err))
	}
	_ = store.Close()
	log.Info("bye")
}

// setupGateway 组装网关核心组件（依赖注入）。
func setupGateway(cfg *config.Config, rdb redisClient, modelsPath string) (*core.Gateway, *registry.ModelRegistry) {
	reg := registry.Init(rdb)

	// 加载模型配置。
	models, err := loadModels(modelsPath)
	if err != nil {
		log.Warn("load models failed", zap.Error(err), zap.String("path", modelsPath))
	}
	reg.RegisterAll(models)
	log.Info("models loaded", zap.Int("count", len(models)))

	// 限流器工厂。
	limiterFactory, err := buildLimiter(cfg, rdb)
	if err != nil {
		log.Warn("limiter init failed", zap.Error(err))
	}

	// 服务发现 + 熔断器 + 选择管理器。
	disc := discovery.NewManager(cfg.Registry.CacheTTL)
	cb := breaker.New(breaker.DefaultConfig())
	selectorMgr := selector.NewManager(disc, cb, rdb, cfg.Server.UpstreamTimeout)

	// 路由器。
	r := router.NewRouter(reg)

	gw := core.NewGateway(
		core.WithRouter(r),
		core.WithLimiterFactory(limiterFactory),
		core.WithSelectorManager(selectorMgr),
		core.WithUpstreamTimeout(cfg.Server.UpstreamTimeout),
	)
	return gw, reg
}

func buildLimiter(cfg *config.Config, rdb redisClient) (*limiter.Factory, error) {
	rules := make([]limiter.Rule, 0, len(cfg.Limiter.Rules))
	for _, rc := range cfg.Limiter.Rules {
		rules = append(rules, limiter.Rule{
			Match:   rc.Match,
			KeyExpr: rc.KeyExpr,
			Limit:   model.Limit{Rate: rc.Rate, Burst: rc.Burst, Period: rc.Period},
		})
	}
	return limiter.NewFactory(rdb,
		limiter.WithRules(rules),
		limiter.WithDefaultLimit(model.Limit{
			Rate:   cfg.Limiter.DefaultRate,
			Burst:  cfg.Limiter.DefaultBurst,
			Period: cfg.Limiter.DefaultPeriod,
		}),
		limiter.WithExprCacheSize(cfg.Limiter.ExprCacheSize),
	)
}

// setupHTTPServer 创建 gin 引擎并注册路由与中间件。
func setupHTTPServer(gw *core.Gateway, authManager *auth.Manager, reg *registry.ModelRegistry,
	ledger *usage.Ledger, checker *health.Checker) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(api.RecoveryMiddleware(), api.RequestIDMiddleware(), api.TracingMiddleware(), api.AccessLogMiddleware())
	api.NewHandler(gw, authManager, reg, ledger, checker).RegisterRoutes(engine)
	return engine
}

func serveMetrics(cfg config.MetricsConfig, checker *health.Checker) {
	mux := http.NewServeMux()
	mux.Handle(cfg.Path, promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !checker.Ready(r.Context()) {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("unavailable"))
			return
		}
		_, _ = w.Write([]byte("ready"))
	})
	log.Info("metrics server started", zap.String("addr", cfg.Addr))
	srv := &http.Server{Addr: cfg.Addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("metrics server failed", zap.Error(err))
	}
}

func serveAdmin(cfg config.AdminConfig, reg *registry.ModelRegistry) {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(api.RecoveryMiddleware())
	admin.NewServer(reg, cfg.SecretKey).RegisterRoutes(engine)
	log.Info("admin server started", zap.String("addr", cfg.Addr))
	srv := &http.Server{Addr: cfg.Addr, Handler: engine, ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("admin server failed", zap.Error(err))
	}
}
