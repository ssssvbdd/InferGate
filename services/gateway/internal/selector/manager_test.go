package selector

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/breaker"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/discovery"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/gwerr"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
)

func TestAllOpenEndpointsFailFast(t *testing.T) {
	cb := breaker.New(breaker.Config{FailureWindow: time.Second, MaxFailCount: 1, RecoveryInterval: time.Minute})
	defer cb.Close()
	mc := &model.ModelConfig{Name: "test", Discovery: model.DiscoveryStatic, LoadBalance: model.LBRoundRobin,
		Endpoints: []model.Endpoint{{ID: "a", Host: "127.0.0.1"}, {ID: "b", Host: "127.0.0.2"}}}
	manager := NewManager(discovery.NewManager(time.Second), cb, nil, time.Second)
	for _, ep := range mc.Endpoints {
		cb.RecordFailure(cbKey(mc.Name, ep))
	}
	_, err := manager.Select(context.Background(), mc)
	if !errors.Is(err, gwerr.ErrNoInstance) {
		t.Fatalf("want no healthy instance, got %v", err)
	}
}
