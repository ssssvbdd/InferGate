package auth

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisQuotaAcrossReplicas(t *testing.T) {
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("set REDIS_TEST_ADDR for integration test")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	namespace := "quota-test-" + time.Now().UTC().Format("20060102150405.000000000")
	first, err := NewRedisQuota(client, namespace, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewRedisQuota(client, namespace, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	identity := &Identity{Tenant: "team-a", key: &Key{RPM: 2, Concurrency: 1}}
	defer client.Del(ctx, first.keys(identity.Tenant)...)
	lease, _, err := first.Acquire(ctx, identity, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := second.Acquire(ctx, identity, 1); !errors.Is(err, ErrBusy) {
		t.Fatalf("expected cross-replica concurrency rejection, got %v", err)
	}
	lease.Release()
	lease2, _, err := second.Acquire(ctx, identity, 1)
	if err != nil {
		t.Fatal(err)
	}
	lease2.Release()
	if _, _, err := first.Acquire(ctx, identity, 1); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("expected shared rolling RPM rejection, got %v", err)
	}
}

func TestRedisGlobalQuotaAndLeaseRenewal(t *testing.T) {
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("set REDIS_TEST_ADDR for integration test")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	ctx := context.Background()
	namespace := "lease-test-" + time.Now().UTC().Format("20060102150405.000000000")
	quota, err := NewRedisQuota(client, namespace, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// Short TTL makes renewal observable without a long-running test.
	quota.leaseTTL = 300 * time.Millisecond
	quota.interval = 80 * time.Millisecond
	a := &Identity{Tenant: "a", key: &Key{RPM: 10, Concurrency: 1}}
	b := &Identity{Tenant: "b", key: &Key{RPM: 10, Concurrency: 1}}
	defer client.Del(ctx, append(quota.keys(a.Tenant), quota.keys(b.Tenant)...)...)
	lease, _, err := quota.Acquire(ctx, a, 1)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(550 * time.Millisecond)
	if lease.Context.Err() != nil {
		t.Fatal("active lease was lost despite renewal")
	}
	if _, _, err := quota.Acquire(ctx, b, 1); !errors.Is(err, ErrGlobalBusy) {
		t.Fatalf("global quota was not shared: %v", err)
	}
	lease.Release()
	other, _, err := quota.Acquire(ctx, b, 1)
	if err != nil {
		t.Fatal(err)
	}
	other.Release()
}

func TestRedisQuotaFailsClosed(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 50 * time.Millisecond,
		ReadTimeout: 50 * time.Millisecond, MaxRetries: 0})
	defer client.Close()
	quota, err := NewRedisQuota(client, "unavailable-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, _, err = quota.Acquire(ctx, &Identity{Tenant: "a", key: &Key{RPM: 1, Concurrency: 1}}, 1)
	if !errors.Is(err, ErrQuotaUnavailable) {
		t.Fatalf("expected fail-closed quota, got %v", err)
	}
}
