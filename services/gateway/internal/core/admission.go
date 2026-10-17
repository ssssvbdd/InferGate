package core

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/gwerr"
)

// localAdmission bounds work already accepted by a gateway replica. The auth
// quota remains the shared cross-replica limit; these gates protect individual
// models and upstream connection pools on each replica.
type localAdmission struct {
	mu    sync.Mutex
	gates map[string]*admissionGate
}

type admissionGate struct {
	slots   chan struct{}
	mu      sync.Mutex
	waiters int
}

func newLocalAdmission() *localAdmission {
	return &localAdmission{gates: make(map[string]*admissionGate)}
}

func (a *localAdmission) gate(key string, capacity int) *admissionGate {
	if capacity <= 0 {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	// A capacity change creates a new gate. Operators should drain the old
	// model version before changing live limits.
	fullKey := key + ":" + strconv.Itoa(capacity)
	if gate := a.gates[fullKey]; gate != nil {
		return gate
	}
	gate := &admissionGate{slots: make(chan struct{}, capacity)}
	a.gates[fullKey] = gate
	return gate
}

func (g *admissionGate) acquire(ctx context.Context, maxWaiting int, waitTimeout time.Duration) (func(), error) {
	if g == nil {
		return func() {}, nil
	}
	select {
	case g.slots <- struct{}{}:
		return func() { <-g.slots }, nil
	default:
	}
	if maxWaiting <= 0 {
		return nil, gwerr.ErrOverloaded
	}
	g.mu.Lock()
	if g.waiters >= maxWaiting {
		g.mu.Unlock()
		return nil, gwerr.ErrOverloaded
	}
	g.waiters++
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		g.waiters--
		g.mu.Unlock()
	}()
	if waitTimeout <= 0 {
		waitTimeout = 2 * time.Second
	}
	deadline, cancel := context.WithTimeout(ctx, waitTimeout)
	defer cancel()
	select {
	case g.slots <- struct{}{}:
		return func() { <-g.slots }, nil
	case <-deadline.Done():
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, gwerr.ErrOverloaded
	}
}

func (a *localAdmission) acquire(ctx context.Context, modelName, endpointID string,
	modelCap, endpointCap, maxWaiting int, waitTimeout time.Duration) (func(), error) {
	modelRelease, err := a.gate("model:"+modelName, modelCap).acquire(ctx, maxWaiting, waitTimeout)
	if err != nil {
		return nil, err
	}
	endpointRelease, err := a.gate("endpoint:"+modelName+":"+endpointID, endpointCap).acquire(ctx, 0, 0)
	if err != nil {
		modelRelease()
		return nil, err
	}
	return func() { endpointRelease(); modelRelease() }, nil
}
