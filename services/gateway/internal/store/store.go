// Package store 管理 Redis 等基础设施连接，按需初始化。
package store

import (
	"context"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/config"
)

var (
	redisClient *redis.Client
	once        sync.Once
)

// InitRedis 初始化全局 Redis 客户端。
func InitRedis(cfg config.RedisConfig) error {
	var err error
	once.Do(func() {
		redisClient = redis.NewClient(&redis.Options{
			Addr:         cfg.Addr,
			Password:     cfg.Password,
			DB:           cfg.DB,
			PoolSize:     cfg.PoolSize,
			DialTimeout:  cfg.DialTimeout,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
		})
		ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
		defer cancel()
		err = redisClient.Ping(ctx).Err()
	})
	return err
}

// GetRedisClient 返回全局 Redis 客户端，可能为 nil（未初始化）。
func GetRedisClient() *redis.Client {
	return redisClient
}

// Close 关闭 Redis 连接。
func Close() error {
	if redisClient != nil {
		return redisClient.Close()
	}
	return nil
}
