// Package limiter 实现基于 Redis 令牌桶 + expr 表达式引擎的分布式限流。
package limiter

import (
	"context"
	"fmt"
	"time"

	"github.com/coocood/freecache"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/redis/go-redis/v9"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
)

// Rule 是一条限流规则。
type Rule struct {
	// Match 是命中该规则的表达式（返回 bool），为空表示始终匹配。
	Match string
	// KeyExpr 是限流键表达式（返回 string）。
	KeyExpr string
	// Limit 是限流三参数。
	Limit model.Limit

	matchVM *vm.Program
	keyVM   *vm.Program
}

// Factory 是限流器工厂。
type Factory struct {
	rdb          *redis.Client
	rules        []*Rule
	defaultLimit model.Limit
	exprCache    *freecache.Cache
	// tokenBucket 脚本。
	script *redis.Script
}

// Option 是工厂选项。
type Option func(*Factory)

// WithRules 设置限流规则。
func WithRules(rules []Rule) Option {
	return func(f *Factory) {
		for i := range rules {
			r := rules[i]
			f.rules = append(f.rules, &r)
		}
	}
}

// WithDefaultLimit 设置默认限流。
func WithDefaultLimit(l model.Limit) Option {
	return func(f *Factory) { f.defaultLimit = l }
}

// WithExprCacheSize 设置表达式缓存大小（字节）。
func WithExprCacheSize(size int) Option {
	return func(f *Factory) {
		if size > 0 {
			f.exprCache = freecache.NewCache(size)
		}
	}
}

// 令牌桶 Lua 脚本：KEYS[1]=key，ARGV=rate,burst,period(ms),now(ms),cost。
// 返回 1 允许，0 拒绝。
var tokenBucketLua = redis.NewScript(`
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local period = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local cost = tonumber(ARGV[5])

local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then
  tokens = burst
  ts = now
end

local elapsed = now - ts
if elapsed < 0 then elapsed = 0 end
local refill = elapsed * rate / period
tokens = math.min(burst, tokens + refill)

local allowed = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
end

redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', key, period * 2)
return allowed
`)

// NewFactory 创建限流器工厂。
func NewFactory(rdb *redis.Client, opts ...Option) (*Factory, error) {
	f := &Factory{
		rdb:       rdb,
		exprCache: freecache.NewCache(5 * 1024 * 1024),
		script:    tokenBucketLua,
	}
	for _, o := range opts {
		o(f)
	}
	// 预编译规则表达式。
	for _, r := range f.rules {
		if r.Match != "" {
			p, err := expr.Compile(r.Match, expr.AsBool())
			if err != nil {
				return nil, fmt.Errorf("compile match expr %q: %w", r.Match, err)
			}
			r.matchVM = p
		}
		if r.KeyExpr != "" {
			p, err := expr.Compile(r.KeyExpr)
			if err != nil {
				return nil, fmt.Errorf("compile key expr %q: %w", r.KeyExpr, err)
			}
			r.keyVM = p
		}
	}
	return f, nil
}

// Env 是表达式求值环境。
type Env struct {
	Model        string `expr:"model"`
	User         string `expr:"user"`
	Stream       bool   `expr:"stream"`
	PromptLength int    `expr:"prompt_length"`
	Provider     string `expr:"provider"`
}

// Allow 判断请求是否被允许（未命中任何规则则放行）。
func (f *Factory) Allow(ctx context.Context, env Env) (bool, error) {
	if f.rdb == nil {
		return true, nil
	}
	rule := f.matchRule(env)
	if rule == nil {
		return true, nil
	}

	key := f.limitKey(rule, env)
	limit := rule.Limit
	if limit.Rate <= 0 {
		limit = f.defaultLimit
	}
	if limit.Rate <= 0 {
		return true, nil
	}
	if limit.Burst <= 0 {
		limit.Burst = limit.Rate
	}
	if limit.Period <= 0 {
		limit.Period = time.Second
	}

	now := time.Now().UnixMilli()
	res, err := f.script.Run(ctx, f.rdb, []string{key},
		limit.Rate, limit.Burst, limit.Period.Milliseconds(), now, 1).Int()
	if err != nil {
		// 限流器故障时放行（fail-open），避免因 Redis 抖动拒绝全部流量。
		return true, nil
	}
	return res == 1, nil
}

func (f *Factory) matchRule(env Env) *Rule {
	for _, r := range f.rules {
		if r.matchVM == nil {
			return r
		}
		out, err := expr.Run(r.matchVM, env)
		if err != nil {
			continue
		}
		if b, ok := out.(bool); ok && b {
			return r
		}
	}
	return nil
}

func (f *Factory) limitKey(rule *Rule, env Env) string {
	if rule.keyVM != nil {
		out, err := expr.Run(rule.keyVM, env)
		if err == nil {
			if s, ok := out.(string); ok && s != "" {
				return "infergate:rl:" + s
			}
		}
	}
	return "infergate:rl:" + env.Model
}
