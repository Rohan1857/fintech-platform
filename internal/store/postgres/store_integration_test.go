package postgres_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Rohan1857/fintech-platform/internal/ledger"
	"github.com/Rohan1857/fintech-platform/internal/store/postgres"
	"github.com/Rohan1857/fintech-platform/pkg/money"
	"github.com/jackc/pgx/v5/pgxpool"
)

func getTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://ledger:ledger_password@127.0.0.1:5432/ledger?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	config, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		t.Fatalf("failed to parse db url: %v", err)
	}
	config.MaxConns = 50

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("postgres ping failed: %v", err)
	}

	return pool
}

func resetDB(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	queries := []string{
		"TRUNCATE TABLE processed_events CASCADE",
		"TRUNCATE TABLE fraud_evaluations CASCADE",
		"TRUNCATE TABLE outbox_events CASCADE",
		"TRUNCATE TABLE refunds CASCADE",
		"TRUNCATE TABLE holds CASCADE",
		"TRUNCATE TABLE entries CASCADE",
		"TRUNCATE TABLE transfers CASCADE",
		"TRUNCATE TABLE accounts CASCADE",
	}
	for _, q := range queries {
		if _, err := pool.Exec(ctx, q); err != nil {
			t.Fatalf("resetDB failed on %q: %v", q, err)
		}
	}
}

func TestPostgres_100ConcurrentTransfers_SingleAccountNoOverspend(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()
	resetDB(t, pool)

	s := postgres.New(pool)
	ctx := context.Background()

	// Initial balance: 5,000 minor units (50 INR)
	const initialBalance money.Money = 5000
	const transferAmount money.Money = 100
	const numAttempts = 100 // 100 * 100 = 10,000 > 5,000 (can only afford 50)

	sourceID := "source_pool"
	destID := "dest_pool"

	if err := s.CreateAccount(ctx, ledger.Account{ID: sourceID, Currency: money.CurrencyINR, Balance: initialBalance}); err != nil {
		t.Fatalf("create source failed: %v", err)
	}
	if err := s.CreateAccount(ctx, ledger.Account{ID: destID, Currency: money.CurrencyINR, Balance: 0}); err != nil {
		t.Fatalf("create dest failed: %v", err)
	}

	var wg sync.WaitGroup
	var successCount int64
	var failCount int64

	for i := 0; i < numAttempts; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := ledger.TransferRequest{
				IdempotencyKey: fmt.Sprintf("concurrent_tx_%d", idx),
				FromAccountID:  sourceID,
				ToAccountID:    destID,
				Amount:         transferAmount,
				Currency:       money.CurrencyINR,
			}
			_, err := s.ApplyTransfer(ctx, req)
			if err == nil {
				atomic.AddInt64(&successCount, 1)
			} else {
				atomic.AddInt64(&failCount, 1)
			}
		}(i)
	}

	wg.Wait()

	expectedSuccess := int64(initialBalance / transferAmount) // exactly 50
	if successCount != expectedSuccess {
		t.Errorf("expected exactly %d successes, got %d", expectedSuccess, successCount)
	}
	expectedFails := int64(numAttempts) - expectedSuccess // 50
	if failCount != expectedFails {
		t.Errorf("expected exactly %d failures, got %d", expectedFails, failCount)
	}

	src, err := s.GetAccount(ctx, sourceID)
	if err != nil {
		t.Fatalf("get source failed: %v", err)
	}
	dest, err := s.GetAccount(ctx, destID)
	if err != nil {
		t.Fatalf("get dest failed: %v", err)
	}

	if src.Balance != 0 {
		t.Errorf("expected source balance 0, got %d", src.Balance)
	}
	if dest.Balance != initialBalance {
		t.Errorf("expected dest balance %d, got %d", initialBalance, dest.Balance)
	}

	// Verify ledger reconciliation
	report, err := s.Reconcile(ctx)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if !report.IsClean {
		t.Errorf("reconciliation failed: %+v", report)
	}
}

func TestPostgres_100ConcurrentRetries_SingleTransferIdempotency(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()
	resetDB(t, pool)

	s := postgres.New(pool)
	ctx := context.Background()

	sourceID := "idem_source"
	destID := "idem_dest"

	if err := s.CreateAccount(ctx, ledger.Account{ID: sourceID, Currency: money.CurrencyINR, Balance: 10000}); err != nil {
		t.Fatalf("create source failed: %v", err)
	}
	if err := s.CreateAccount(ctx, ledger.Account{ID: destID, Currency: money.CurrencyINR, Balance: 0}); err != nil {
		t.Fatalf("create dest failed: %v", err)
	}

	const numRetries = 100
	const idemKey = "fixed_idem_key_100"
	var wg sync.WaitGroup
	var successCount int64
	var errCount int64
	var firstTransferID string
	var mu sync.Mutex

	for i := 0; i < numRetries; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx, err := s.ApplyTransfer(ctx, ledger.TransferRequest{
				IdempotencyKey: idemKey,
				FromAccountID:  sourceID,
				ToAccountID:    destID,
				Amount:         1000,
				Currency:       money.CurrencyINR,
			})
			if err != nil {
				atomic.AddInt64(&errCount, 1)
				return
			}
			atomic.AddInt64(&successCount, 1)
			mu.Lock()
			if firstTransferID == "" {
				firstTransferID = tx.ID
			} else if firstTransferID != tx.ID {
				t.Errorf("inconsistent transfer ID returned: %s vs %s", firstTransferID, tx.ID)
			}
			mu.Unlock()
		}()
	}

	wg.Wait()

	if errCount > 0 {
		t.Errorf("expected 0 errors on idempotent retries, got %d", errCount)
	}
	if successCount != numRetries {
		t.Errorf("expected all %d retries to succeed idempotently, got %d", numRetries, successCount)
	}

	src, _ := s.GetAccount(ctx, sourceID)
	dest, _ := s.GetAccount(ctx, destID)
	if src.Balance != 9000 {
		t.Errorf("expected source balance 9000, got %d", src.Balance)
	}
	if dest.Balance != 1000 {
		t.Errorf("expected dest balance 1000, got %d", dest.Balance)
	}

	report, err := s.Reconcile(ctx)
	if err != nil || !report.IsClean {
		t.Errorf("reconciliation dirty after idempotent retries: %+v, err: %v", report, err)
	}
}

func TestPostgres_DeadlockPrevention_SortedLocking(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()
	resetDB(t, pool)

	s := postgres.New(pool)
	ctx := context.Background()

	acctA := "node_alpha"
	acctB := "node_beta"
	acctC := "node_gamma"

	_ = s.CreateAccount(ctx, ledger.Account{ID: acctA, Currency: money.CurrencyINR, Balance: 100000})
	_ = s.CreateAccount(ctx, ledger.Account{ID: acctB, Currency: money.CurrencyINR, Balance: 100000})
	_ = s.CreateAccount(ctx, ledger.Account{ID: acctC, Currency: money.CurrencyINR, Balance: 100000})

	// Concurrent cyclic transfers: A->B, B->C, C->A
	const workers = 30
	var wg sync.WaitGroup
	var errCount int64

	for i := 0; i < workers; i++ {
		wg.Add(3)
		idx := i
		go func() {
			defer wg.Done()
			_, err := s.ApplyTransfer(ctx, ledger.TransferRequest{
				IdempotencyKey: fmt.Sprintf("ab_%d", idx),
				FromAccountID:  acctA,
				ToAccountID:    acctB,
				Amount:         10,
				Currency:       money.CurrencyINR,
			})
			if err != nil {
				atomic.AddInt64(&errCount, 1)
			}
		}()
		go func() {
			defer wg.Done()
			_, err := s.ApplyTransfer(ctx, ledger.TransferRequest{
				IdempotencyKey: fmt.Sprintf("bc_%d", idx),
				FromAccountID:  acctB,
				ToAccountID:    acctC,
				Amount:         10,
				Currency:       money.CurrencyINR,
			})
			if err != nil {
				atomic.AddInt64(&errCount, 1)
			}
		}()
		go func() {
			defer wg.Done()
			_, err := s.ApplyTransfer(ctx, ledger.TransferRequest{
				IdempotencyKey: fmt.Sprintf("ca_%d", idx),
				FromAccountID:  acctC,
				ToAccountID:    acctA,
				Amount:         10,
				Currency:       money.CurrencyINR,
			})
			if err != nil {
				atomic.AddInt64(&errCount, 1)
			}
		}()
	}

	wg.Wait()

	if errCount > 0 {
		t.Fatalf("expected 0 deadlock errors, got %d", errCount)
	}

	report, err := s.Reconcile(ctx)
	if err != nil || !report.IsClean {
		t.Fatalf("reconciliation dirty after cyclic transfers: %+v, err: %v", report, err)
	}
}

func TestPostgres_HoldLifecycle_PartialCapture_Refund(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()
	resetDB(t, pool)

	s := postgres.New(pool)
	ctx := context.Background()

	buyerID := "buyer_acct"
	merchantID := "merchant_acct"

	_ = s.CreateAccount(ctx, ledger.Account{ID: buyerID, Currency: money.CurrencyINR, Balance: 10000})
	_ = s.CreateAccount(ctx, ledger.Account{ID: merchantID, Currency: money.CurrencyINR, Balance: 0})

	// 1. Authorize hold for 4000
	hold, err := s.AuthorizeHold(ctx, ledger.AuthorizeRequest{
		IdempotencyKey: "hold_lifecycle_1",
		FromAccountID:  buyerID,
		ToAccountID:    merchantID,
		Amount:         4000,
		Currency:       money.CurrencyINR,
	})
	if err != nil {
		t.Fatalf("authorize hold failed: %v", err)
	}

	b, _ := s.GetAccount(ctx, buyerID)
	if b.Balance != 10000 || b.Held != 4000 || b.Available() != 6000 {
		t.Fatalf("unexpected balance after hold: %+v", b)
	}

	// 2. Partial capture of 2500 (releases remaining 1500)
	capTx, err := s.CaptureHold(ctx, ledger.CaptureRequest{
		IdempotencyKey: "cap_lifecycle_1",
		HoldID:         hold.ID,
		Amount:         2500,
	})
	if err != nil {
		t.Fatalf("partial capture failed: %v", err)
	}

	b, _ = s.GetAccount(ctx, buyerID)
	m, _ := s.GetAccount(ctx, merchantID)
	// Buyer: 10000 - 2500 = 7500, held = 0
	if b.Balance != 7500 || b.Held != 0 {
		t.Errorf("buyer balance after capture expected 7500 held 0, got bal=%d held=%d", b.Balance, b.Held)
	}
	// Merchant: 0 + 2500 = 2500
	if m.Balance != 2500 {
		t.Errorf("merchant balance after capture expected 2500, got %d", m.Balance)
	}

	// 3. Partial refund of 1000
	ref1, _, err := s.RefundTransfer(ctx, ledger.RefundRequest{
		IdempotencyKey: "ref_lifecycle_1",
		TransferID:     capTx.ID,
		Amount:         1000,
		Reason:         "item returned",
	})
	if err != nil {
		t.Fatalf("partial refund 1 failed: %v", err)
	}
	if ref1.Amount != 1000 {
		t.Errorf("refund 1 amount mismatch: %d", ref1.Amount)
	}

	b, _ = s.GetAccount(ctx, buyerID)
	m, _ = s.GetAccount(ctx, merchantID)
	if b.Balance != 8500 {
		t.Errorf("buyer balance after refund 1 expected 8500, got %d", b.Balance)
	}
	if m.Balance != 1500 {
		t.Errorf("merchant balance after refund 1 expected 1500, got %d", m.Balance)
	}

	// 4. Over-refund should fail (attempt 2000 when only 1500 remaining)
	_, _, err = s.RefundTransfer(ctx, ledger.RefundRequest{
		IdempotencyKey: "ref_lifecycle_2",
		TransferID:     capTx.ID,
		Amount:         2000,
	})
	if err != ledger.ErrRefundExceedsTransfer {
		t.Errorf("expected ErrRefundExceedsTransfer, got %v", err)
	}

	// 5. Clean reconciliation check
	report, err := s.Reconcile(ctx)
	if err != nil || !report.IsClean {
		t.Fatalf("reconcile dirty after hold capture and refund: %+v, err: %v", report, err)
	}
}

func TestPostgres_MultiLegPosting_AtomicAggregation(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()
	resetDB(t, pool)

	s := postgres.New(pool)
	ctx := context.Background()

	payer := "multi_payer"
	payee1 := "multi_payee_1"
	payee2 := "multi_payee_2"

	_ = s.CreateAccount(ctx, ledger.Account{ID: payer, Currency: money.CurrencyINR, Balance: 1000})
	_ = s.CreateAccount(ctx, ledger.Account{ID: payee1, Currency: money.CurrencyINR, Balance: 0})
	_ = s.CreateAccount(ctx, ledger.Account{ID: payee2, Currency: money.CurrencyINR, Balance: 0})

	// Leg 1: payer -600, payee1 +600
	// Leg 2: payee1 -200, payee2 +200
	// Total: payer net -600, payee1 net +400, payee2 net +200 (Sum = 0)
	postReq := ledger.PostRequest{
		IdempotencyKey: "multi_leg_split_1",
		Currency:       money.CurrencyINR,
		Postings: []ledger.Posting{
			{AccountID: payer, Amount: -600},
			{AccountID: payee1, Amount: 600},
			{AccountID: payee1, Amount: -200},
			{AccountID: payee2, Amount: 200},
		},
	}

	tx, err := s.ApplyPosting(ctx, postReq)
	if err != nil {
		t.Fatalf("multi-leg post failed: %v", err)
	}
	if len(tx.Entries) != 4 {
		t.Errorf("expected 4 entries, got %d", len(tx.Entries))
	}

	p, _ := s.GetAccount(ctx, payer)
	p1, _ := s.GetAccount(ctx, payee1)
	p2, _ := s.GetAccount(ctx, payee2)

	if p.Balance != 400 || p1.Balance != 400 || p2.Balance != 200 {
		t.Errorf("balances incorrect: p=%d p1=%d p2=%d", p.Balance, p1.Balance, p2.Balance)
	}

	report, err := s.Reconcile(ctx)
	if err != nil || !report.IsClean {
		t.Fatalf("reconcile dirty after multi-leg post: %+v, err: %v", report, err)
	}
}
