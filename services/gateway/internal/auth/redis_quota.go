package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisQuota atomically enforces a rolling RPM window and expiring concurrent
// leases across all gateway replicas. Its three keys share a Redis Cluster slot.
type RedisQuota struct {
	client   *redis.Client
	prefix   string
	leaseTTL time.Duration
	interval time.Duration
}

type Lease struct {
	Context context.Context
	close   func()
	once    sync.Once
}

func (l *Lease) Release() {
	if l != nil {
		l.once.Do(l.close)
	}
}

var ErrQuotaUnavailable = errors.New("shared quota store unavailable")

var acquireScript = redis.NewScript(`
local now_parts = redis.call('TIME')
local now = now_parts[1] * 1000 + math.floor(now_parts[2] / 1000)
local rpm = tonumber(ARGV[1])
local tenant_max = tonumber(ARGV[2])
local global_max = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local id = ARGV[5]
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - 60000)
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', now)
if redis.call('ZCARD', KEYS[1]) >= rpm then
  local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
  return {1, math.max(1000, 60000 - (now - tonumber(oldest[2])))}
end
if redis.call('ZCARD', KEYS[2]) >= tenant_max then return {2, 1000} end
if redis.call('ZCARD', KEYS[3]) >= global_max then return {3, 1000} end
redis.call('ZADD', KEYS[1], now, id)
redis.call('ZADD', KEYS[2], now + ttl, id)
redis.call('ZADD', KEYS[3], now + ttl, id)
redis.call('PEXPIRE', KEYS[1], 61000)
redis.call('PEXPIRE', KEYS[2], ttl + 1000)
redis.call('PEXPIRE', KEYS[3], ttl + 1000)
return {0, 0}
`)

var renewScript = redis.NewScript(`
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local id = ARGV[1]
local ttl = tonumber(ARGV[2])
if not redis.call('ZSCORE', KEYS[1], id) or not redis.call('ZSCORE', KEYS[2], id) then return 0 end
if tonumber(redis.call('ZSCORE', KEYS[1], id)) <= now then return 0 end
redis.call('ZADD', KEYS[1], now + ttl, id)
redis.call('ZADD', KEYS[2], now + ttl, id)
redis.call('PEXPIRE', KEYS[1], ttl + 1000)
redis.call('PEXPIRE', KEYS[2], ttl + 1000)
return 1
`)

var releaseScript = redis.NewScript(`
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return 1
`)

func NewRedisQuota(client *redis.Client, namespace string, leaseTTL time.Duration) (*RedisQuota, error) {
	if client == nil || namespace == "" || strings.ContainsAny(namespace, "{} \t\r\n") {
		return nil, errors.New("redis quota requires a client and a simple namespace")
	}
	if leaseTTL < 15*time.Second || leaseTTL > 30*time.Minute {
		return nil, errors.New("quota lease TTL must be between 15s and 30m")
	}
	return &RedisQuota{client: client, prefix: "llm:quota:{" + namespace + "}:", leaseTTL: leaseTTL, interval: leaseTTL / 3}, nil
}

func (q *RedisQuota) keys(tenant string) []string {
	return []string{q.prefix + "rpm:" + tenant, q.prefix + "active:" + tenant, q.prefix + "active:global"}
}

func (q *RedisQuota) Acquire(ctx context.Context, identity *Identity, globalMax int) (*Lease, time.Duration, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, 0, fmt.Errorf("generate quota lease: %w", err)
	}
	id := hex.EncodeToString(random[:])
	keys := q.keys(identity.Tenant)
	result, err := acquireScript.Run(ctx, q.client, keys, identity.key.RPM, identity.key.Concurrency, globalMax, q.leaseTTL.Milliseconds(), id).Int64Slice()
	if err != nil || len(result) != 2 {
		return nil, 0, ErrQuotaUnavailable
	}
	if result[0] != 0 {
		retry := time.Duration(result[1]) * time.Millisecond
		switch result[0] {
		case 1:
			return nil, retry, ErrRateLimited
		case 2:
			return nil, retry, ErrBusy
		case 3:
			return nil, retry, ErrGlobalBusy
		default:
			return nil, 0, ErrQuotaUnavailable
		}
	}
	requestCtx, cancel := context.WithCancel(ctx)
	stop := make(chan struct{})
	done := make(chan struct{})
	lease := &Lease{Context: requestCtx}
	lease.close = func() {
		close(stop)
		cancel()
		<-done
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer releaseCancel()
		_ = releaseScript.Run(releaseCtx, q.client, keys[1:], id).Err()
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(q.interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-requestCtx.Done():
				return
			case <-ticker.C:
				renewCtx, renewCancel := context.WithTimeout(requestCtx, 2*time.Second)
				ok, err := renewScript.Run(renewCtx, q.client, keys[1:], id, q.leaseTTL.Milliseconds()).Int()
				renewCancel()
				if err != nil || ok != 1 {
					cancel()
					return
				}
			}
		}
	}()
	return lease, 0, nil
}
