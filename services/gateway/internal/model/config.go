package model

import "time"

// Discovery 表示实例发现方式。
type Discovery string

const (
	DiscoveryStatic Discovery = "static" // 配置中静态声明的实例
	DiscoveryDNS    Discovery = "dns"    // 通过 DNS A 记录解析
	DiscoveryDirect Discovery = "direct" // 直连（单实例）
)

// LoadBalanceStrategy 表示负载均衡策略。
type LoadBalanceStrategy string

const (
	LBLeastConn  LoadBalanceStrategy = "least_conn"  // 最少连接（Redis ZSET）
	LBIdleRandom LoadBalanceStrategy = "idle_random" // 空闲随机（Redis 信号量）
	LBWeighted   LoadBalanceStrategy = "weighted"    // 平滑加权随机
	LBRoundRobin LoadBalanceStrategy = "round_robin" // 轮询
	LBDirect     LoadBalanceStrategy = "direct"      // 直连第一个
)

// Endpoint 是一个上游实例。
type Endpoint struct {
	ID       string            `json:"id" yaml:"id"`
	Host     string            `json:"host" yaml:"host"`
	Port     int               `json:"port" yaml:"port"`
	Scheme   string            `json:"scheme" yaml:"scheme"` // http / https
	Weight   int               `json:"weight" yaml:"weight"`
	Healthy  bool              `json:"healthy" yaml:"-"`
	Metadata map[string]string `json:"metadata,omitempty" yaml:"metadata"`
}

// Credential 是访问上游的凭证。
type Credential struct {
	// APIKeyEnv 指定从哪个环境变量读取 API Key（密钥只从 env 读取，不落配置文件）。
	APIKeyEnv string `json:"api_key_env" yaml:"api_key_env"`
	// Organization 可选。
	Organization string `json:"organization,omitempty" yaml:"organization"`
}

// Limit 是限流三参数。
type Limit struct {
	Rate   int           `json:"rate" yaml:"rate"`
	Burst  int           `json:"burst" yaml:"burst"`
	Period time.Duration `json:"period" yaml:"period"`
}

// Condition 是 Mongo 风格条件表达式（如 {"prompt_length": {"$gt": 1000}}）。
type Condition map[string]any

// ModelConfig 是一个可路由目标模型的完整配置。
type ModelConfig struct {
	// Name 是逻辑模型名（用户请求的 model）。
	Name string `json:"name" yaml:"name"`
	// Version 是版本标识（default 优先级最高）。
	Version string `json:"version" yaml:"version"`
	// ModelType 决定使用哪类适配器方法。
	ModelType ModelType `json:"model_type" yaml:"model_type"`

	ProviderType ProviderType `json:"provider_type" yaml:"provider_type"`
	// ProviderModel 是上游真实模型名（替换请求中的 Model）。
	ProviderModel string `json:"provider_model" yaml:"provider_model"`
	// BaseURL 是上游基础地址（当无静态实例时使用）。
	BaseURL string `json:"base_url,omitempty" yaml:"base_url"`

	Credential Credential `json:"credential" yaml:"credential"`

	Discovery   Discovery           `json:"discovery" yaml:"discovery"`
	LoadBalance LoadBalanceStrategy `json:"load_balance" yaml:"load_balance"`
	Endpoints   []Endpoint          `json:"endpoints,omitempty" yaml:"endpoints"`
	// DNSName 用于 DNS 发现。
	DNSName string `json:"dns_name,omitempty" yaml:"dns_name"`

	// Priority 用于条件路由排序（越大越优先）。
	Priority int `json:"priority" yaml:"priority"`
	// Weight 用于同优先级加权轮询。
	Weight int `json:"weight" yaml:"weight"`
	// Condition 是命中该配置的条件表达式。
	Condition Condition `json:"condition,omitempty" yaml:"condition"`

	// Limit 是该模型的限流配置（可选）。
	Limit *Limit `json:"limit,omitempty" yaml:"limit"`

	// Plugins 是启用的插件名列表（按顺序执行）。
	Plugins []string `json:"plugins,omitempty" yaml:"plugins"`

	// Timeout 覆盖默认上游超时。
	Timeout time.Duration `json:"timeout,omitempty" yaml:"timeout"`

	// MaxRetries 覆盖默认重试次数。
	MaxRetries int `json:"max_retries,omitempty" yaml:"max_retries"`
}
