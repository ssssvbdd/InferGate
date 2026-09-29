// Package registry 提供模型配置注册中心，实现 内存 → Redis 三级缓存的内存层与回源。
package registry

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/log"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
	"go.uber.org/zap"
)

// ModelRegistry 是模型配置注册中心（全局单例）。
type ModelRegistry struct {
	mu sync.RWMutex
	// byName 保存 逻辑模型名 -> 该名下所有版本配置。
	byName map[string][]model.ModelConfig

	rdb       *redis.Client
	cacheTTL  time.Duration
	keyPrefix string
}

var (
	instance *ModelRegistry
	initOnce sync.Once
)

// Init 初始化全局注册中心。
func Init(rdb *redis.Client) *ModelRegistry {
	initOnce.Do(func() {
		instance = &ModelRegistry{
			byName:    make(map[string][]model.ModelConfig),
			rdb:       rdb,
			cacheTTL:  5 * time.Minute,
			keyPrefix: "infergate:model:",
		}
	})
	return instance
}

// Get 返回全局注册中心。
func Get() *ModelRegistry { return instance }

// Register 注册一个模型配置（内存层），同名多版本追加。
func (r *ModelRegistry) Register(cfg model.ModelConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	list := r.byName[cfg.Name]
	// 覆盖同名同版本。
	for i, c := range list {
		if c.Version == cfg.Version {
			list[i] = cfg
			r.byName[cfg.Name] = list
			return
		}
	}
	r.byName[cfg.Name] = append(list, cfg)
}

// RegisterAll 批量注册。
func (r *ModelRegistry) RegisterAll(cfgs []model.ModelConfig) {
	for _, c := range cfgs {
		r.Register(c)
	}
}

// ListNames returns a stable snapshot of logical model names for /v1/models.
func (r *ModelRegistry) ListNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.byName))
	for name := range r.byName {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// GetModels 三级回退查询同名模型的所有版本配置：内存 → Redis → （回填内存）。
func (r *ModelRegistry) GetModels(ctx context.Context, name string) ([]model.ModelConfig, bool) {
	r.mu.RLock()
	list, ok := r.byName[name]
	r.mu.RUnlock()
	if ok && len(list) > 0 {
		return cloneModels(list), true
	}

	// Redis 回源。
	if r.rdb != nil {
		if cfgs, err := r.loadFromRedis(ctx, name); err == nil && len(cfgs) > 0 {
			// 异步回填内存。
			go r.RegisterAll(cfgs)
			return cfgs, true
		}
	}
	return nil, false
}

func (r *ModelRegistry) loadFromRedis(ctx context.Context, name string) ([]model.ModelConfig, error) {
	raw, err := r.rdb.Get(ctx, r.keyPrefix+name).Bytes()
	if err != nil {
		return nil, err
	}
	var cfgs []model.ModelConfig
	if err := json.Unmarshal(raw, &cfgs); err != nil {
		return nil, err
	}
	return cfgs, nil
}

// Persist 将某模型的全部版本写入 Redis（双写，供多实例共享）。
func (r *ModelRegistry) Persist(ctx context.Context, name string) error {
	if r.rdb == nil {
		return nil
	}
	r.mu.RLock()
	list := cloneModels(r.byName[name])
	r.mu.RUnlock()
	data, err := json.Marshal(list)
	if err != nil {
		return err
	}
	if err := r.rdb.Set(ctx, r.keyPrefix+name, data, r.cacheTTL).Err(); err != nil {
		log.Warn("persist model to redis failed", zap.String("model", name), zap.Error(err))
		return err
	}
	return nil
}

// SortedVersions 返回按版本优先级排序后的配置（default 最前，其余版本号倒序）。
func SortedVersions(list []model.ModelConfig) []model.ModelConfig {
	out := cloneModels(list)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Version == "default" {
			return true
		}
		if out[j].Version == "default" {
			return false
		}
		return out[i].Version > out[j].Version
	})
	return out
}

func cloneModels(list []model.ModelConfig) []model.ModelConfig {
	if list == nil {
		return nil
	}
	out := make([]model.ModelConfig, len(list))
	copy(out, list)
	return out
}
