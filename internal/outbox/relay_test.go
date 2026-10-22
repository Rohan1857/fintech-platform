package outbox_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/Rohan1857/fintech-platform/internal/events"
	"github.com/Rohan1857/fintech-platform/internal/ledger"
	"github.com/Rohan1857/fintech-platform/internal/outbox"
	"github.com/Rohan1857/fintech-platform/internal/store/postgres"
	"github.com/Rohan1857/fintech-platform/pkg/money"
	"github.com/jackc/pgx/v5/pgxpool"
)

func getTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Fatal("DATABASE_URL must point to a disposable test database (fixtures truncate tables)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("postgres ping failed: %v", err)
	}
	return pool
}

type mockFailingPublisher struct {
	failCount int
	maxFails  int
	published []events.Event
}

func (m *mockFailingPublisher) Publish(ctx context.Context, topic string, key string, event events.Event) error {
	if m.failCount < m.maxFails {
		m.failCount++
		return errors.New("simulated publisher network outage")
	}
	m.published = append(m.published, event)
	return nil
}

func (m *mockFailingPublisher) Close() error {
	return nil
}

func TestOutboxRelay_RealPostgres_PublishAndRetry(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	_, _ = pool.Exec(ctx, "TRUNCATE TABLE outbox_events CASCADE; TRUNCATE TABLE transfers CASCADE; TRUNCATE TABLE entries CASCADE; TRUNCATE TABLE accounts CASCADE;")

	store := postgres.New(pool)

	// 1. Create accounts and apply transfer (which writes to outbox_events in same DB tx)
	_ = store.CreateAccount(ctx, ledger.Account{ID: "sender_outbox", Currency: money.CurrencyINR, Balance: 5000})
	_ = store.CreateAccount(ctx, ledger.Account{ID: "receiver_outbox", Currency: money.CurrencyINR, Balance: 0})

	_, err := store.ApplyTransfer(ctx, ledger.TransferRequest{
		IdempotencyKey: "outbox_test_tx_1",
		FromAccountID:  "sender_outbox",
		ToAccountID:    "receiver_outbox",
		Amount:         1000,
		Currency:       money.CurrencyINR,
	})
	if err != nil {
		t.Fatalf("apply transfer failed: %v", err)
	}

	// Verify outbox row exists and published_at is NULL
	var unpublishedCount int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL").Scan(&unpublishedCount)
	if err != nil || unpublishedCount < 1 {
		t.Fatalf("expected at least 1 unpublished outbox event, got %d, err: %v", unpublishedCount, err)
	}

	// 2. Test Relay with a publisher that fails 2 times then succeeds
	failPub := &mockFailingPublisher{maxFails: 2}
	cfg := outbox.Config{
		BatchSize:   10,
		MaxRetries:  5,
		TopicEvents: "payment.events",
		TopicDLQ:    "payment.dlq",
	}
	relay := outbox.NewRelay(pool, failPub, cfg)

	// First attempt: should fail and increment retry_count
	count, _ := relay.ProcessBatch(ctx)
	if count != 0 {
		t.Errorf("expected 0 published on first failure, got %d", count)
	}

	var retries int
	var lastErr *string
	_ = pool.QueryRow(ctx, "SELECT retry_count, last_error FROM outbox_events WHERE published_at IS NULL LIMIT 1").Scan(&retries, &lastErr)
	if retries != 1 || lastErr == nil || *lastErr == "" {
		t.Errorf("expected retry_count=1 and last_error recorded, got count=%d", retries)
	}

	// Second attempt: should fail again
	_, _ = relay.ProcessBatch(ctx)

	// Third attempt: should succeed!
	count3, err := relay.ProcessBatch(ctx)
	if err != nil {
		t.Fatalf("process batch 3 failed: %v", err)
	}
	if count3 < 1 {
		t.Fatalf("expected at least 1 published on third attempt, got %d", count3)
	}

	// Verify published_at is now set
	var remainingUnpublished int
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL").Scan(&remainingUnpublished)
	if remainingUnpublished != 0 {
		t.Errorf("expected 0 unpublished events, got %d", remainingUnpublished)
	}
}
