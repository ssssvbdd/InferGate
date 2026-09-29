package selector

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
)

var errNoInstance = errors.New("no healthy instance")

// leastConnSelector 基于 Redis ZSET 的最少连接选择器。
// ZSET member 为实例 ID，score 为当前连接数；选中最小 score 并 +1，Release 时 -1。
type leastConnSelector struct {
	rdb *redis.Client
	ttl time.Duration
}

func newLeastConnSelector(rdb *redis.Client) *leastConnSelector {
	return &leastConnSelector{rdb: rdb, ttl: 10 * time.Second}
}

func (s *leastConnSelector) key(modelName string) string {
	return "infergate:leastconn:" + modelName
}

// selectLua 原子选出连接数最小的实例并将其 score+1。
// KEYS[1]=zset key, ARGV[1..N]=候选实例ID, ARGV[N+1]=ttl秒
var selectLua = redis.NewScript(`
local key = KEYS[1]
local ttl = tonumber(ARGV[#ARGV])
local best = nil
local bestScore = nil
for i = 1, #ARGV - 1 do
  local id = ARGV[i]
  local sc = redis.call('ZSCORE', key, id)
  if sc == false then sc = 0 else sc = tonumber(sc) end
  if bestScore == nil or sc < bestScore then
    bestScore = sc
    best = id
  end
end
if best ~= nil then
  redis.call('ZINCRBY', key, 1, best)
  redis.call('EXPIRE', key, ttl)
end
return best
`)

var releaseLua = redis.NewScript(`
local key = KEYS[1]
local id = ARGV[1]
local sc = redis.call('ZSCORE', key, id)
if sc ~= false and tonumber(sc) > 0 then
  redis.call('ZINCRBY', key, -1, id)
end
return 1
`)

func (s *leastConnSelector) Select(ctx context.Context, modelName string, eps []model.Endpoint) (model.Endpoint, error) {
	if len(eps) == 0 {
		return model.Endpoint{}, errNoInstance
	}
	if s.rdb == nil {
		// 降级：本地随机。
		return eps[rand.Intn(len(eps))], nil
	}
	args := make([]any, 0, len(eps)+1)
	idMap := make(map[string]model.Endpoint, len(eps))
	for _, ep := range eps {
		id := epID(modelName, ep)
		args = append(args, id)
		idMap[id] = ep
	}
	args = append(args, int(s.ttl.Seconds()))

	res, err := selectLua.Run(ctx, s.rdb, []string{s.key(modelName)}, args...).Result()
	if err != nil {
		// Redis 故障降级本地随机。
		return eps[rand.Intn(len(eps))], nil
	}
	id, _ := res.(string)
	if ep, ok := idMap[id]; ok {
		return ep, nil
	}
	return eps[0], nil
}

func (s *leastConnSelector) Release(ctx context.Context, modelName string, ep model.Endpoint) {
	if s.rdb == nil {
		return
	}
	_ = releaseLua.Run(ctx, s.rdb, []string{s.key(modelName)}, epID(modelName, ep)).Err()
}

// idleRandomSelector 基于 Redis 信号量的空闲随机选择器。
// 为每个实例维护一个并发计数，优先选择低于阈值的实例，否则回退随机。
type idleRandomSelector struct {
	rdb        *redis.Client
	maxPerInst int
	ttl        time.Duration
}

func newIdleRandomSelector(rdb *redis.Client) *idleRandomSelector {
	return &idleRandomSelector{rdb: rdb, maxPerInst: 100, ttl: 10 * time.Second}
}

func (s *idleRandomSelector) key(modelName string) string {
	return "infergate:idle:" + modelName
}

var acquireLua = redis.NewScript(`
local key = KEYS[1]
local ttl = tonumber(ARGV[#ARGV])
local maxc = tonumber(ARGV[#ARGV - 1])
-- 打乱顺序从 ARGV[1..N-2] 中选一个未超阈值的实例
for i = 1, #ARGV - 2 do
  local id = ARGV[i]
  local c = redis.call('HGET', key, id)
  if c == false then c = 0 else c = tonumber(c) end
  if c < maxc then
    redis.call('HINCRBY', key, id, 1)
    redis.call('EXPIRE', key, ttl)
    return id
  end
end
return ARGV[1]
`)

var idleReleaseLua = redis.NewScript(`
local key = KEYS[1]
local id = ARGV[1]
local c = redis.call('HGET', key, id)
if c ~= false and tonumber(c) > 0 then
  redis.call('HINCRBY', key, id, -1)
end
return 1
`)

func (s *idleRandomSelector) Select(ctx context.Context, modelName string, eps []model.Endpoint) (model.Endpoint, error) {
	if len(eps) == 0 {
		return model.Endpoint{}, errNoInstance
	}
	if s.rdb == nil {
		return eps[rand.Intn(len(eps))], nil
	}
	// 随机打乱候选顺序。
	shuffled := make([]model.Endpoint, len(eps))
	copy(shuffled, eps)
	rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })

	idMap := make(map[string]model.Endpoint, len(shuffled))
	args := make([]any, 0, len(shuffled)+2)
	for _, ep := range shuffled {
		id := epID(modelName, ep)
		args = append(args, id)
		idMap[id] = ep
	}
	args = append(args, s.maxPerInst, int(s.ttl.Seconds()))

	res, err := acquireLua.Run(ctx, s.rdb, []string{s.key(modelName)}, args...).Result()
	if err != nil {
		return shuffled[0], nil
	}
	id, _ := res.(string)
	if ep, ok := idMap[id]; ok {
		return ep, nil
	}
	return shuffled[0], nil
}

func (s *idleRandomSelector) Release(ctx context.Context, modelName string, ep model.Endpoint) {
	if s.rdb == nil {
		return
	}
	_ = idleReleaseLua.Run(ctx, s.rdb, []string{s.key(modelName)}, epID(modelName, ep)).Err()
}

func epID(modelName string, ep model.Endpoint) string {
	if ep.ID != "" {
		return ep.ID
	}
	return fmt.Sprintf("%s:%d", ep.Host, ep.Port)
}
