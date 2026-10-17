package usage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestCentralUsageReplayAndDedup(t *testing.T) {
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
	stream := "llm:usage:{test-" + time.Now().UTC().Format("20060102150405.000000000") + "}:events"
	seen := stream + ":seen:" + time.Now().UTC().Format("20060102")
	defer client.Del(ctx, stream, seen)
	path := filepath.Join(t.TempDir(), "usage.jsonl")
	ledger, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.EnableRedis(client, stream); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Record(Event{Tenant: "team-a", Model: "qwen3-4b", Status: 200, Metered: true,
		PromptTokens: 3, CompletionTokens: 5}); err != nil {
		t.Fatal(err)
	}
	if ledger.PendingBytes() == 0 {
		t.Fatal("event was not journaled")
	}
	if count, err := ledger.DeliverPending(ctx); err != nil || count != 1 {
		t.Fatalf("first delivery: count=%d err=%v", count, err)
	}
	if ledger.PendingBytes() != 0 {
		t.Fatal("delivery offset did not advance")
	}
	if err := ledger.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if err := restarted.EnableRedis(client, stream); err != nil {
		t.Fatal(err)
	}
	if count, err := restarted.DeliverPending(ctx); err != nil || count != 0 {
		t.Fatalf("restart replay: count=%d err=%v", count, err)
	}
	if n := client.XLen(ctx, stream).Val(); n != 1 {
		t.Fatalf("expected one central event, got %d", n)
	}
	// A lost local checkpoint replays the same event id and must not double count.
	if err := os.Remove(path + ".offset"); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}
	fromZero, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer fromZero.Close()
	if err := fromZero.EnableRedis(client, stream); err != nil {
		t.Fatal(err)
	}
	if _, err := fromZero.DeliverPending(ctx); err != nil {
		t.Fatal(err)
	}
	if n := client.XLen(ctx, stream).Val(); n != 1 {
		t.Fatalf("duplicate central event: %d", n)
	}
}

func TestCentralUsageRecoversAfterRedisOutage(t *testing.T) {
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("set REDIS_TEST_ADDR for integration test")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	stream := "llm:usage:{outage-" + time.Now().UTC().Format("20060102150405.000000000") + "}:events"
	ctx := context.Background()
	defer client.Del(ctx, stream, stream+":seen:"+time.Now().UTC().Format("20060102"))
	ledger, err := Open(filepath.Join(t.TempDir(), "usage.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	badClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 50 * time.Millisecond,
		ReadTimeout: 50 * time.Millisecond, MaxRetries: 0})
	defer badClient.Close()
	if err := ledger.EnableRedis(badClient, stream); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Record(Event{Tenant: "a", Model: "m", Status: 200}); err != nil {
		t.Fatalf("local journal must survive Redis outage: %v", err)
	}
	deliveryCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if _, err := ledger.DeliverPending(deliveryCtx); err == nil {
		t.Fatal("expected central delivery failure")
	}
	if ledger.PendingBytes() == 0 {
		t.Fatal("undelivered event was lost")
	}
	ledger.redis = client
	if count, err := ledger.DeliverPending(ctx); err != nil || count != 1 {
		t.Fatalf("recovery delivery count=%d err=%v", count, err)
	}
	if n := client.XLen(ctx, stream).Val(); n != 1 {
		t.Fatalf("central stream count=%d", n)
	}
}
