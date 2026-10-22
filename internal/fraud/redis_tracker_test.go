package fraud_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Rohan1857/fintech-platform/internal/fraud"
	"github.com/Rohan1857/fintech-platform/pkg/money"
	"github.com/redis/go-redis/v9"
)

func getTestRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		t.Fatal("REDIS_ADDR must point to a disposable Redis instance")
	}

	client := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		t.Fatalf("redis ping failed at %s: %v", redisAddr, err)
	}

	return client
}

func cleanupRedisKeys(t *testing.T, client *redis.Client, pattern string) {
	t.Helper()
	ctx := context.Background()
	keys, err := client.Keys(ctx, pattern).Result()
	if err == nil && len(keys) > 0 {
		_ = client.Del(ctx, keys...).Err()
	}
}

func TestRedisVelocityTracker_RealSlidingWindow(t *testing.T) {
	client := getTestRedisClient(t)
	defer client.Close()

	acct := "acct_redis_test_1"
	cleanupRedisKeys(t, client, fmt.Sprintf("velocity:*:%s*", acct))
	defer cleanupRedisKeys(t, client, fmt.Sprintf("velocity:*:%s*", acct))

	tracker := fraud.NewRedisVelocityTracker(client)
	ctx := context.Background()

	if !tracker.IsHealthy(ctx) {
		t.Fatal("expected redis tracker to be healthy")
	}

	now := time.Now().UTC()

	// Record 3 transactions within the last 10 seconds: 100, 200, 300 minor units
	_ = tracker.RecordTransaction(ctx, acct, "bene_1", 100, now.Add(-5*time.Second))
	_ = tracker.RecordTransaction(ctx, acct, "bene_1", 200, now.Add(-3*time.Second))
	_ = tracker.RecordTransaction(ctx, acct, "bene_2", 300, now.Add(-1*time.Second))

	// Also record 1 older transaction (2 minutes ago)
	_ = tracker.RecordTransaction(ctx, acct, "bene_3", 500, now.Add(-2*time.Minute))

	// Query 1-minute window
	count, sum, err := tracker.GetVelocity(ctx, acct, 1*time.Minute)
	if err != nil {
		t.Fatalf("GetVelocity failed: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 transactions in 1-min window, got %d", count)
	}
	if sum != 600 {
		t.Errorf("expected 600 sum in 1-min window, got %d", sum)
	}

	// Query 5-minute window (should include the 500 transaction)
	count5, sum5, err := tracker.GetVelocity(ctx, acct, 5*time.Minute)
	if err != nil {
		t.Fatalf("GetVelocity 5m failed: %v", err)
	}
	if count5 != 4 {
		t.Errorf("expected 4 transactions in 5-min window, got %d", count5)
	}
	if sum5 != 1100 {
		t.Errorf("expected 1100 sum in 5-min window, got %d", sum5)
	}

	// Query beneficiary frequency for bene_1 (2 txns in 5 min)
	freq, err := tracker.GetBeneficiaryFrequency(ctx, acct, "bene_1", 5*time.Minute)
	if err != nil {
		t.Fatalf("GetBeneficiaryFrequency failed: %v", err)
	}
	if freq != 2 {
		t.Errorf("expected freq 2 for bene_1, got %d", freq)
	}
}

func TestRedisVelocityTracker_ConcurrentUpdates(t *testing.T) {
	client := getTestRedisClient(t)
	defer client.Close()

	acct := "acct_redis_concurrent"
	cleanupRedisKeys(t, client, fmt.Sprintf("velocity:*:%s*", acct))
	defer cleanupRedisKeys(t, client, fmt.Sprintf("velocity:*:%s*", acct))

	tracker := fraud.NewRedisVelocityTracker(client)
	ctx := context.Background()

	const workers = 50
	var wg sync.WaitGroup

	now := time.Now().UTC()
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_ = tracker.RecordTransaction(ctx, acct, "bene_target", 10, now)
		}(i)
	}
	wg.Wait()

	count, sum, err := tracker.GetVelocity(ctx, acct, 1*time.Minute)
	if err != nil {
		t.Fatalf("GetVelocity failed: %v", err)
	}
	if count != workers {
		t.Errorf("expected count %d, got %d", workers, count)
	}
	if sum != workers*10 {
		t.Errorf("expected sum %d, got %d", workers*10, sum)
	}
}

func TestRedis_FailClosed_PolicyOnOutage(t *testing.T) {
	// Point to dead/unreachable redis port
	deadClient := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:6399", // Unused port
		DialTimeout: 200 * time.Millisecond,
	})
	defer deadClient.Close()

	deadTracker := fraud.NewRedisVelocityTracker(deadClient)
	ctx := context.Background()

	// 1. Health check must be false
	if deadTracker.IsHealthy(ctx) {
		t.Fatal("expected dead tracker to report unhealthy")
	}

	// 2. Fraud Engine must activate FailClosed policy and REJECT transaction
	cfg := fraud.DefaultConfig()
	cfg.FailClosed = true
	cfg.FallbackDecisionOnError = fraud.DecisionReject

	memAudit := fraud.NewMemAuditStore()
	engine := fraud.NewEngine(cfg, deadTracker, memAudit)

	req := fraud.EvaluationRequest{
		AccountID:     "user_victim",
		BeneficiaryID: "shop_merchant",
		Amount:        1000,
		Currency:      money.CurrencyINR,
	}

	result, err := engine.Evaluate(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// CRITICAL SECURITY ASSERTION: Must never silently approve when security dependency is down
	if result.Decision != fraud.DecisionReject {
		t.Fatalf("SECURITY VIOLATION: expected REJECT when Redis is down, got %s", result.Decision)
	}
	if result.RiskScore != 100.0 {
		t.Errorf("expected risk score 100.0, got %f", result.RiskScore)
	}
	if len(result.Reasons) == 0 || result.Reasons[0] != "security dependency unavailable: fail-closed policy activated" {
		t.Errorf("expected fail-closed reason, got %v", result.Reasons)
	}
}
