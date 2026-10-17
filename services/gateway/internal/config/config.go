package config

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/spf13/viper"
)

// Config 是网关的顶层配置。
type Config struct {
	Server    ServerConfig    `mapstructure:"server"`
	Admin     AdminConfig     `mapstructure:"admin"`
	Redis     RedisConfig     `mapstructure:"redis"`
	Log       LogConfig       `mapstructure:"log"`
	Tracing   TracingConfig   `mapstructure:"tracing"`
	Metrics   MetricsConfig   `mapstructure:"metrics"`
	Registry  RegistryConfig  `mapstructure:"registry"`
	Limiter   LimiterConfig   `mapstructure:"limiter"`
	Auth      AuthConfig      `mapstructure:"auth"`
	Usage     UsageConfig     `mapstructure:"usage"`
	Readiness ReadinessConfig `mapstructure:"readiness"`
}

type AuthConfig struct {
	KeysFile            string        `mapstructure:"keys_file"`
	CacheSaltSecretFile string        `mapstructure:"cache_salt_secret_file"`
	MaxInFlight         int           `mapstructure:"max_in_flight"`
	Distributed         bool          `mapstructure:"distributed"`
	QuotaNamespace      string        `mapstructure:"quota_namespace"`
	LeaseTTL            time.Duration `mapstructure:"lease_ttl"`
}

type UsageConfig struct {
	LedgerFile  string `mapstructure:"ledger_file"`
	RedisStream string `mapstructure:"redis_stream"`
}

type ReadinessConfig struct {
	URL     string        `mapstructure:"url"`
	URLs    []string      `mapstructure:"urls"`
	Timeout time.Duration `mapstructure:"timeout"`
}

// ServerConfig 是 HTTP 接入层配置。
type ServerConfig struct {
	Addr              string        `mapstructure:"addr"`
	ReadTimeout       time.Duration `mapstructure:"read_timeout"`
	ReadHeaderTimeout time.Duration `mapstructure:"read_header_timeout"`
	WriteTimeout      time.Duration `mapstructure:"write_timeout"`
	IdleTimeout       time.Duration `mapstructure:"idle_timeout"`
	MaxHeaderBytes    int           `mapstructure:"max_header_bytes"`
	ShutdownTimeout   time.Duration `mapstructure:"shutdown_timeout"`
	// UpstreamTimeout 是转发到上游模型的默认超时。
	UpstreamTimeout time.Duration `mapstructure:"upstream_timeout"`
}

// AdminConfig 是管理后台配置，使用 HMAC-SHA256 签名鉴权。
type AdminConfig struct {
	Addr      string `mapstructure:"addr"`
	SecretKey string `mapstructure:"secret_key"`
	Enabled   bool   `mapstructure:"enabled"`
}

// RedisConfig 是 Redis 连接配置。
type RedisConfig struct {
	Enabled       bool          `mapstructure:"enabled"`
	Addr          string        `mapstructure:"addr"`
	Password      string        `mapstructure:"password"`
	PasswordFile  string        `mapstructure:"password_file"`
	TLSEnabled    bool          `mapstructure:"tls_enabled"`
	TLSCAFile     string        `mapstructure:"tls_ca_file"`
	TLSServerName string        `mapstructure:"tls_server_name"`
	DB            int           `mapstructure:"db"`
	PoolSize      int           `mapstructure:"pool_size"`
	DialTimeout   time.Duration `mapstructure:"dial_timeout"`
	ReadTimeout   time.Duration `mapstructure:"read_timeout"`
	WriteTimeout  time.Duration `mapstructure:"write_timeout"`
}

// LogConfig 是日志配置。
type LogConfig struct {
	Level    string `mapstructure:"level"`
	Encoding string `mapstructure:"encoding"`
}

// TracingConfig 是 OpenTelemetry 追踪配置。
type TracingConfig struct {
	Enabled     bool    `mapstructure:"enabled"`
	Endpoint    string  `mapstructure:"endpoint"`
	ServiceName string  `mapstructure:"service_name"`
	SampleRatio float64 `mapstructure:"sample_ratio"`
}

// MetricsConfig 是 Prometheus 指标配置。
type MetricsConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Addr    string `mapstructure:"addr"`
	Path    string `mapstructure:"path"`
}

// RegistryConfig 是服务发现配置。
type RegistryConfig struct {
	// Type 支持 memory / dns / static。
	Type       string        `mapstructure:"type"`
	CacheTTL   time.Duration `mapstructure:"cache_ttl"`
	RefreshTTL time.Duration `mapstructure:"refresh_ttl"`
}

// LimiterConfig 是限流器工厂配置。
type LimiterConfig struct {
	ExprCacheSize int           `mapstructure:"expr_cache_size"`
	DefaultRate   int           `mapstructure:"default_rate"`
	DefaultBurst  int           `mapstructure:"default_burst"`
	DefaultPeriod time.Duration `mapstructure:"default_period"`
	Rules         []RuleConfig  `mapstructure:"rules"`
}

// RuleConfig 是单条限流规则配置。
type RuleConfig struct {
	Match   string        `mapstructure:"match"`
	KeyExpr string        `mapstructure:"key_expr"`
	Rate    int           `mapstructure:"rate"`
	Burst   int           `mapstructure:"burst"`
	Period  time.Duration `mapstructure:"period"`
}

// Load 从指定路径加载配置并填充默认值。
func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	setDefaults(v)

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	cfg := &Config{}
	if err := v.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	if cfg.Redis.PasswordFile != "" {
		if cfg.Redis.Password != "" {
			return nil, fmt.Errorf("redis.password and password_file are mutually exclusive")
		}
		secret, err := os.ReadFile(cfg.Redis.PasswordFile)
		if err != nil {
			return nil, fmt.Errorf("read redis password file: %w", err)
		}
		cfg.Redis.Password = string(bytes.TrimSpace(secret))
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("server.addr", ":8080")
	v.SetDefault("server.read_timeout", "30s")
	v.SetDefault("server.read_header_timeout", "5s")
	v.SetDefault("server.write_timeout", "0s")
	v.SetDefault("server.idle_timeout", "90s")
	v.SetDefault("server.max_header_bytes", 32768)
	v.SetDefault("server.shutdown_timeout", "15s")
	v.SetDefault("server.upstream_timeout", "3s")

	v.SetDefault("admin.addr", ":8090")
	v.SetDefault("admin.enabled", true)

	v.SetDefault("redis.addr", "127.0.0.1:6379")
	v.SetDefault("redis.enabled", false)
	v.SetDefault("redis.db", 0)
	v.SetDefault("redis.pool_size", 64)
	v.SetDefault("redis.dial_timeout", "3s")
	v.SetDefault("redis.read_timeout", "2s")
	v.SetDefault("redis.write_timeout", "2s")

	v.SetDefault("log.level", "info")
	v.SetDefault("log.encoding", "json")

	v.SetDefault("tracing.enabled", false)
	v.SetDefault("tracing.service_name", "infergate")
	v.SetDefault("tracing.sample_ratio", 1.0)

	v.SetDefault("metrics.enabled", true)
	v.SetDefault("metrics.addr", ":6060")
	v.SetDefault("metrics.path", "/metrics")

	v.SetDefault("registry.type", "static")
	v.SetDefault("registry.cache_ttl", "30s")
	v.SetDefault("registry.refresh_ttl", "10s")

	v.SetDefault("limiter.expr_cache_size", 5*1024*1024)
	v.SetDefault("limiter.default_rate", 0)
	v.SetDefault("limiter.default_burst", 0)
	v.SetDefault("limiter.default_period", "1s")

	v.SetDefault("auth.max_in_flight", 64)
	v.SetDefault("auth.distributed", false)
	v.SetDefault("auth.quota_namespace", "infergate")
	v.SetDefault("auth.lease_ttl", "2m")
	v.SetDefault("usage.ledger_file", "run/usage.jsonl")
	v.SetDefault("readiness.timeout", "2s")
}

func (c *Config) validate() error {
	if c.Server.Addr == "" {
		return fmt.Errorf("server.addr must not be empty")
	}
	if c.Server.ReadHeaderTimeout <= 0 || c.Server.IdleTimeout <= 0 || c.Server.MaxHeaderBytes < 1024 {
		return fmt.Errorf("invalid HTTP header or idle limits")
	}
	if c.Admin.Enabled && c.Admin.SecretKey == "" {
		return fmt.Errorf("admin.secret_key must be set when admin is enabled")
	}
	if c.Server.UpstreamTimeout <= 0 {
		c.Server.UpstreamTimeout = 3 * time.Second
	}
	if c.Auth.KeysFile == "" || c.Auth.CacheSaltSecretFile == "" {
		return fmt.Errorf("auth.keys_file and auth.cache_salt_secret_file must be set")
	}
	if c.Auth.MaxInFlight < 1 {
		return fmt.Errorf("auth.max_in_flight must be positive")
	}
	if c.Redis.Enabled && c.Redis.Addr == "" {
		return fmt.Errorf("redis.addr must be set when redis is enabled")
	}
	if !c.Redis.TLSEnabled && (c.Redis.TLSCAFile != "" || c.Redis.TLSServerName != "") {
		return fmt.Errorf("redis.tls_enabled is required when TLS options are set")
	}
	if c.Usage.LedgerFile == "" {
		return fmt.Errorf("usage.ledger_file must be set")
	}
	if (c.Auth.Distributed || c.Usage.RedisStream != "") && !c.Redis.Enabled {
		return fmt.Errorf("redis.enabled must be true for distributed quotas or centralized usage")
	}
	if c.Auth.Distributed && (c.Auth.QuotaNamespace == "" || c.Auth.LeaseTTL < 15*time.Second || c.Auth.LeaseTTL > 30*time.Minute) {
		return fmt.Errorf("invalid distributed quota namespace or lease_ttl")
	}
	if c.Readiness.URL == "" && len(c.Readiness.URLs) == 0 {
		return fmt.Errorf("readiness.url or readiness.urls must be set")
	}
	return nil
}
