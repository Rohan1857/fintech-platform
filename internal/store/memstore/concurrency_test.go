package memstore

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/messi/fintech-platform/internal/ledger"
	"github.com/messi/fintech-platform/pkg/money"
)

// Acceptance Test 1: 100 concurrent transfers from one funded account cannot overspend it.
func TestConcurrency_CannotOverspend(t *testing.T) {
	ctx := context.Background()
	s := New()

	// Initial balance: 5,000 paise (50 INR)
	initialBalance := money.Money(5000)
	transferAmount := money.Money(100) // 1 INR each

	_ = s.CreateAccount(ctx, ledger.Account{ID: "source", Currency: money.CurrencyINR, Balance: initialBalance})

	// 10 destination accounts
	numDest := 10
	for i := 0; i < numDest; i++ {
		_ = s.CreateAccount(ctx, ledger.Account{ID: fmt.Sprintf("dest_%d", i), Currency: money.CurrencyINR, Balance: 0})
	}

	// 100 concurrent transfers of 100 paise each (total attempted: 10,000 paise)
	// Since initial balance is 5,000 paise, exactly 50 transfers must succeed, and 50 must fail with ErrInsufficientFunds!
	numTransfers := 100
	var wg sync.WaitGroup
	var successCount atomic.Int32
	var failCount atomic.Int32

	for i := 0; i < numTransfers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			destID := fmt.Sprintf("dest_%d", idx%numDest)
			_, err := s.ApplyTransfer(ctx, ledger.TransferRequest{
				IdempotencyKey: fmt.Sprintf("transfer_race_%d", idx),
				FromAccountID:  "source",
				ToAccountID:    destID,
				Amount:         transferAmount,
				Currency:       money.CurrencyINR,
			})
			if err == nil {
				successCount.Add(1)
			} else {
				failCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	src, _ := s.GetAccount(ctx, "source")
	if src.Balance < 0 {
		t.Fatalf("CRITICAL: source account overspent! Balance=%d", src.Balance)
	}
	if src.Balance != 0 {
		t.Fatalf("expected source balance to be 0 after exhausting funds, got %d", src.Balance)
	}
	if successCount.Load() != 50 {
		t.Fatalf("expected exactly 50 successful transfers, got %d", successCount.Load())
	}
	if failCount.Load() != 50 {
		t.Fatalf("expected exactly 50 failed transfers, got %d", failCount.Load())
	}

	// Verify total balance across all accounts is strictly conserved
	var totalDestBalance money.Money
	for i := 0; i < numDest; i++ {
		d, _ := s.GetAccount(ctx, fmt.Sprintf("dest_%d", i))
		totalDestBalance += d.Balance
	}
	if src.Balance+totalDestBalance != initialBalance {
		t.Fatalf("money created or destroyed: total=%d, expected=%d", src.Balance+totalDestBalance, initialBalance)
	}
}

// Acceptance Test 2: 100 concurrent retries of one payment create exactly one transfer.
func TestConcurrency_ConcurrentRetriesIdempotency(t *testing.T) {
	ctx := context.Background()
	s := New()

	_ = s.CreateAccount(ctx, ledger.Account{ID: "sender_idem", Currency: money.CurrencyINR, Balance: 100000})
	_ = s.CreateAccount(ctx, ledger.Account{ID: "receiver_idem", Currency: money.CurrencyINR, Balance: 0})

	key := "shared_idempotency_key_100_retries"
	amount := money.Money(2500) // 25 INR

	numWorkers := 100
	var wg sync.WaitGroup
	results := make([]ledger.Transfer, numWorkers)
	errors := make([]error, numWorkers)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			tx, err := s.ApplyTransfer(ctx, ledger.TransferRequest{
				IdempotencyKey: key,
				FromAccountID:  "sender_idem",
				ToAccountID:    "receiver_idem",
				Amount:         amount,
				Currency:       money.CurrencyINR,
			})
			results[idx] = tx
			errors[idx] = err
		}(i)
	}

	wg.Wait()

	// Every single retry must succeed with the exact same Transfer ID
	firstID := results[0].ID
	for i := 0; i < numWorkers; i++ {
		if errors[i] != nil {
			t.Fatalf("worker %d got error: %v", i, errors[i])
		}
		if results[i].ID != firstID {
			t.Fatalf("worker %d received different transfer ID: %s vs %s", i, results[i].ID, firstID)
		}
	}

	// Sender must have been debited exactly once, receiver credited exactly once
	sender, _ := s.GetAccount(ctx, "sender_idem")
	receiver, _ := s.GetAccount(ctx, "receiver_idem")

	if sender.Balance != 100000-amount {
		t.Fatalf("sender balance debited multiple times: %d", sender.Balance)
	}
	if receiver.Balance != amount {
		t.Fatalf("receiver balance credited multiple times: %d", receiver.Balance)
	}
}

// Acceptance Test 3: Total ledger balances remain conserved under concurrent multi-leg transfers.
func TestConcurrency_TotalConservation(t *testing.T) {
	ctx := context.Background()
	s := New()

	_ = s.CreateAccount(ctx, ledger.Account{ID: "node_a", Currency: money.CurrencyINR, Balance: 50000})
	_ = s.CreateAccount(ctx, ledger.Account{ID: "node_b", Currency: money.CurrencyINR, Balance: 50000})
	_ = s.CreateAccount(ctx, ledger.Account{ID: "node_c", Currency: money.CurrencyINR, Balance: 50000})
	initialTotal := money.Money(150000)

	var wg sync.WaitGroup
	rounds := 200

	// Concurrent circular transfers
	for i := 0; i < rounds; i++ {
		wg.Add(2)
		go func(idx int) {
			defer wg.Done()
			_, _ = s.ApplyTransfer(ctx, ledger.TransferRequest{
				IdempotencyKey: fmt.Sprintf("circ_ab_%d", idx),
				FromAccountID:  "node_a",
				ToAccountID:    "node_b",
				Amount:         100,
				Currency:       money.CurrencyINR,
			})
		}(i)
		go func(idx int) {
			defer wg.Done()
			_, _ = s.ApplyTransfer(ctx, ledger.TransferRequest{
				IdempotencyKey: fmt.Sprintf("circ_bc_%d", idx),
				FromAccountID:  "node_b",
				ToAccountID:    "node_c",
				Amount:         100,
				Currency:       money.CurrencyINR,
			})
		}(i)
	}

	wg.Wait()

	a, _ := s.GetAccount(ctx, "node_a")
	b, _ := s.GetAccount(ctx, "node_b")
	c, _ := s.GetAccount(ctx, "node_c")
	finalTotal := a.Balance + b.Balance + c.Balance

	if finalTotal != initialTotal {
		t.Fatalf("conservation violated! initial=%d, final=%d", initialTotal, finalTotal)
	}
}
