// Package store 管理 Redis 等基础设施连接，按需初始化。
package store

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
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
		tlsConfig, tlsErr := makeTLSConfig(cfg)
		if tlsErr != nil {
			err = tlsErr
			return
		}
		redisClient = redis.NewClient(&redis.Options{
			Addr:         cfg.Addr,
			Password:     cfg.Password,
			DB:           cfg.DB,
			PoolSize:     cfg.PoolSize,
			DialTimeout:  cfg.DialTimeout,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
			TLSConfig:    tlsConfig,
		})
		ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
		defer cancel()
		err = redisClient.Ping(ctx).Err()
	})
	return err
}

func makeTLSConfig(cfg config.RedisConfig) (*tls.Config, error) {
	if !cfg.TLSEnabled {
		return nil, nil
	}
	host, _, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("redis TLS address: %w", err)
	}
	if cfg.TLSServerName != "" {
		host = cfg.TLSServerName
	}
	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}
	if cfg.TLSCAFile != "" {
		ca, err := os.ReadFile(cfg.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("read redis CA: %w", err)
		}
		if !roots.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("redis CA file has no certificates")
		}
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, ServerName: host, RootCAs: roots}, nil
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
