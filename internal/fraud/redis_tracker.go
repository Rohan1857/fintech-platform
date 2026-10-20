package fraud

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/messi/fintech-platform/pkg/money"
	"github.com/redis/go-redis/v9"
)

// RedisVelocityTracker implements sliding-window velocity checking via Redis sorted sets.
type RedisVelocityTracker struct {
	client *redis.Client
}

func NewRedisVelocityTracker(client *redis.Client) *RedisVelocityTracker {
	return &RedisVelocityTracker{client: client}
}

func (r *RedisVelocityTracker) IsHealthy(ctx context.Context) bool {
	if r.client == nil {
		return false
	}
	ctxTimeout, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	return r.client.Ping(ctxTimeout).Err() == nil
}

func (r *RedisVelocityTracker) RecordTransaction(ctx context.Context, accountID, beneficiaryID string, amount money.Money, t time.Time) error {
	if r.client == nil {
		return fmt.Errorf("redis client not initialized")
	}

	milli := float64(t.UnixMilli())
	member := fmt.Sprintf("%s:%d", uuid.New().String(), int64(amount))

	pipe := r.client.Pipeline()
	// Account velocity
	acctKey := fmt.Sprintf("velocity:acct:%s", accountID)
	pipe.ZAdd(ctx, acctKey, redis.Z{Score: milli, Member: member})
	pipe.Expire(ctx, acctKey, 2*time.Hour)

	// Beneficiary specific velocity
	if beneficiaryID != "" {
		beneKey := fmt.Sprintf("velocity:bene:%s:%s", accountID, beneficiaryID)
		pipe.ZAdd(ctx, beneKey, redis.Z{Score: milli, Member: uuid.New().String()})
		pipe.Expire(ctx, beneKey, 2*time.Hour)
	}

	_, err := pipe.Exec(ctx)
	return err
}

func (r *RedisVelocityTracker) GetVelocity(ctx context.Context, accountID string, window time.Duration) (int64, money.Money, error) {
	if r.client == nil {
		return 0, 0, fmt.Errorf("redis client unavailable")
	}

	now := time.Now().UTC()
	cutoff := float64(now.Add(-window).UnixMilli())
	acctKey := fmt.Sprintf("velocity:acct:%s", accountID)

	// Prune old entries outside maximum retention window (2 hours), preserving multi-window queries
	maxRetentionCutoff := float64(now.Add(-2 * time.Hour).UnixMilli())
	_ = r.client.ZRemRangeByScore(ctx, acctKey, "-inf", fmt.Sprintf("(%f", maxRetentionCutoff)).Err()

	// Fetch recent entries
	entries, err := r.client.ZRangeByScore(ctx, acctKey, &redis.ZRangeBy{
		Min: fmt.Sprintf("%f", cutoff),
		Max: "+inf",
	}).Result()
	if err != nil {
		return 0, 0, err
	}

	var sum money.Money
	for _, entry := range entries {
		parts := strings.Split(entry, ":")
		if len(parts) == 2 {
			amt, _ := strconv.ParseInt(parts[1], 10, 64)
			sum += money.Money(amt)
		}
	}

	return int64(len(entries)), sum, nil
}

func (r *RedisVelocityTracker) GetBeneficiaryFrequency(ctx context.Context, accountID, beneficiaryID string, window time.Duration) (int64, error) {
	if r.client == nil {
		return 0, fmt.Errorf("redis client unavailable")
	}

	now := time.Now().UTC()
	cutoff := fmt.Sprintf("%f", float64(now.Add(-window).UnixMilli()))
	beneKey := fmt.Sprintf("velocity:bene:%s:%s", accountID, beneficiaryID)

	return r.client.ZCount(ctx, beneKey, cutoff, "+inf").Result()
}
