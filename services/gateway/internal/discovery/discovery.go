// Package discovery 提供实例发现能力：static / dns / direct。
package discovery

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
)

// Discoverer 是实例发现接口。
type Discoverer interface {
	Discover(ctx context.Context, mc *model.ModelConfig) ([]model.Endpoint, error)
}

// staticDiscoverer 返回配置中静态声明的实例。
type staticDiscoverer struct{}

func (staticDiscoverer) Discover(ctx context.Context, mc *model.ModelConfig) ([]model.Endpoint, error) {
	if len(mc.Endpoints) == 0 {
		// 无静态实例，回退为一个基于 BaseURL 的空实例（由适配器使用 BaseURL）。
		return []model.Endpoint{{ID: "base", Healthy: true, Weight: 1}}, nil
	}
	out := make([]model.Endpoint, 0, len(mc.Endpoints))
	for _, ep := range mc.Endpoints {
		e := ep
		if e.Weight <= 0 {
			e.Weight = 1
		}
		e.Healthy = true
		out = append(out, e)
	}
	return out, nil
}

// directDiscoverer 直连：返回单个空实例。
type directDiscoverer struct{}

func (directDiscoverer) Discover(ctx context.Context, mc *model.ModelConfig) ([]model.Endpoint, error) {
	return []model.Endpoint{{ID: "direct", Healthy: true, Weight: 1}}, nil
}

// dnsDiscoverer 通过 DNS 解析获取实例，带缓存。
type dnsDiscoverer struct {
	ttl   time.Duration
	mu    sync.Mutex
	cache map[string]dnsCacheEntry
}

type dnsCacheEntry struct {
	endpoints []model.Endpoint
	expireAt  time.Time
}

func newDNSDiscoverer(ttl time.Duration) *dnsDiscoverer {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &dnsDiscoverer{ttl: ttl, cache: make(map[string]dnsCacheEntry)}
}

func (d *dnsDiscoverer) Discover(ctx context.Context, mc *model.ModelConfig) ([]model.Endpoint, error) {
	name := mc.DNSName
	d.mu.Lock()
	if e, ok := d.cache[name]; ok && time.Now().Before(e.expireAt) {
		d.mu.Unlock()
		return e.endpoints, nil
	}
	d.mu.Unlock()

	ips, err := net.DefaultResolver.LookupIPAddr(ctx, name)
	if err != nil {
		return nil, err
	}
	scheme := "https"
	port := 443
	eps := make([]model.Endpoint, 0, len(ips))
	for i, ip := range ips {
		eps = append(eps, model.Endpoint{
			ID:      name + "-" + ip.IP.String(),
			Host:    ip.IP.String(),
			Port:    port,
			Scheme:  scheme,
			Weight:  1,
			Healthy: true,
		})
		_ = i
	}
	d.mu.Lock()
	d.cache[name] = dnsCacheEntry{endpoints: eps, expireAt: time.Now().Add(d.ttl)}
	d.mu.Unlock()
	return eps, nil
}

// Manager 根据发现类型分发到具体 Discoverer。
type Manager struct {
	static Discoverer
	direct Discoverer
	dns    Discoverer
}

// NewManager 创建发现管理器。
func NewManager(dnsTTL time.Duration) *Manager {
	return &Manager{
		static: staticDiscoverer{},
		direct: directDiscoverer{},
		dns:    newDNSDiscoverer(dnsTTL),
	}
}

// Discover 按模型配置选择发现方式。
func (m *Manager) Discover(ctx context.Context, mc *model.ModelConfig) ([]model.Endpoint, error) {
	switch mc.Discovery {
	case model.DiscoveryDNS:
		return m.dns.Discover(ctx, mc)
	case model.DiscoveryDirect:
		return m.direct.Discover(ctx, mc)
	default:
		return m.static.Discover(ctx, mc)
	}
}
