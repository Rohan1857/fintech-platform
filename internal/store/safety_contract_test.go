package store_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Rohan1857/fintech-platform/internal/ledger"
	"github.com/Rohan1857/fintech-platform/internal/store"
	"github.com/Rohan1857/fintech-platform/internal/store/memstore"
	"github.com/Rohan1857/fintech-platform/internal/store/postgres"
	"github.com/Rohan1857/fintech-platform/pkg/money"
	"github.com/jackc/pgx/v5/pgxpool"
)

func safetyStore(t *testing.T, backend string) (store.Store, *pgxpool.Pool, string) {
	t.Helper()
	var s store.Store = memstore.New()
	var pool *pgxpool.Pool
	if backend == "postgres" {
		url := os.Getenv("DATABASE_URL")
		if url == "" {
			t.Fatal("DATABASE_URL must point to a disposable test database")
		}
		var err error
		pool, err = pgxpool.New(context.Background(), url)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		s = postgres.New(pool)
	}
	prefix := fmt.Sprintf("safe_%d_", time.Now().UnixNano())
	for _, a := range []ledger.Account{
		{ID: prefix + "buyer", Currency: "INR", Balance: 300000},
		{ID: prefix + "seller", Currency: "INR"},
	} {
		if err := s.CreateAccount(context.Background(), a); err != nil {
			t.Fatal(err)
		}
	}
	return s, pool, prefix
}

func safetyHold(t *testing.T, s store.Store, prefix, key string, amount int64, ttl time.Duration) ledger.Hold {
	t.Helper()
	h, err := s.AuthorizeHold(context.Background(), ledger.AuthorizeRequest{
		IdempotencyKey: prefix + key, FromAccountID: prefix + "buyer", ToAccountID: prefix + "seller",
		Amount: money.Money(amount), Currency: "INR", ExpiresIn: ttl,
	})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func assertSafetyBalance(t *testing.T, s store.Store, buyer string, held int64) {
	t.Helper()
	a, err := s.GetAccount(context.Background(), buyer)
	if err != nil || a.Balance != 300000 || int64(a.Held) != held {
		t.Fatalf("want balance=300000 held=%d, got %+v err=%v", held, a, err)
	}
}

func TestSafety_ExpiredCapturePersistsRelease(t *testing.T) {
	for _, backend := range []string{"memory", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			s, pool, prefix := safetyStore(t, backend)
			h := safetyHold(t, s, prefix, "hold", 100000, time.Hour)
			if pool != nil {
				if _, err := pool.Exec(context.Background(), "UPDATE holds SET expires_at=$1 WHERE id=$2", time.Now().Add(-time.Hour), h.ID); err != nil {
					t.Fatal(err)
				}
			} else {
				s.(*memstore.MemStore).SetClock(func() time.Time { return h.ExpiresAt.Add(time.Second) })
			}
			_, err := s.CaptureHold(context.Background(), ledger.CaptureRequest{IdempotencyKey: prefix + "capture", HoldID: h.ID, Amount: 50000})
			if !errors.Is(err, ledger.ErrHoldExpired) {
				t.Fatalf("want expired outcome, got %v", err)
			}
			assertSafetyBalance(t, s, prefix+"buyer", 0)
			expired, ok, err := s.GetHold(context.Background(), h.ID)
			if err != nil || !ok || expired.Status != ledger.HoldExpired {
				t.Fatalf("expiry not persisted: %+v err=%v", expired, err)
			}
			if _, ok, err := s.GetTransferByKey(context.Background(), prefix+"capture"); err != nil || ok {
				t.Fatalf("expired capture posted a transfer: exists=%v err=%v", ok, err)
			}
			n, err := s.ExpireHolds(context.Background(), h.ExpiresAt.Add(2*time.Hour))
			if err != nil || n != 0 {
				t.Fatalf("released an expired hold twice: n=%d err=%v", n, err)
			}
			assertSafetyBalance(t, s, prefix+"buyer", 0)
		})
	}
}

func TestSafety_BatchExpirySumsHoldsAndPreservesLiveHold(t *testing.T) {
	for _, backend := range []string{"memory", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			s, _, prefix := safetyStore(t, backend)
			for i := 0; i < 3; i++ {
				safetyHold(t, s, prefix, fmt.Sprintf("expired_%d", i), 50000, time.Hour)
			}
			live := safetyHold(t, s, prefix, "live", 20000, 24*time.Hour)
			n, err := s.ExpireHolds(context.Background(), time.Now().Add(2*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			assertSafetyBalance(t, s, prefix+"buyer", 20000)
			if n != 3 {
				t.Fatalf("want 3 expired holds, got %d", n)
			}
			h, ok, err := s.GetHold(context.Background(), live.ID)
			if err != nil || !ok || h.Status != ledger.HoldActive {
				t.Fatalf("live hold changed: %+v err=%v", h, err)
			}
			n, err = s.ExpireHolds(context.Background(), time.Now().Add(2*time.Hour))
			if err != nil || n != 0 {
				t.Fatalf("repeat sweep: n=%d err=%v", n, err)
			}
			assertSafetyBalance(t, s, prefix+"buyer", 20000)
		})
	}
}

func TestSafety_ExpiryIncludesExactDeadline(t *testing.T) {
	for _, backend := range []string{"memory", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			s, _, prefix := safetyStore(t, backend)
			h := safetyHold(t, s, prefix, "hold", 50000, time.Hour)
			h, _, err := s.GetHold(context.Background(), h.ID)
			if err != nil {
				t.Fatal(err)
			}
			n, err := s.ExpireHolds(context.Background(), h.ExpiresAt)
			if err != nil || n != 1 {
				t.Fatalf("deadline not expired: n=%d err=%v", n, err)
			}
			assertSafetyBalance(t, s, prefix+"buyer", 0)
		})
	}
}

func TestSafety_ConcurrentSweepsReleaseOnce(t *testing.T) {
	for _, backend := range []string{"memory", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			s, _, prefix := safetyStore(t, backend)
			for i := 0; i < 3; i++ {
				safetyHold(t, s, prefix, fmt.Sprintf("hold_%d", i), 50000, time.Hour)
			}
			var wg sync.WaitGroup
			counts := make(chan int, 4)
			errs := make(chan error, 4)
			for i := 0; i < 4; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					n, err := s.ExpireHolds(context.Background(), time.Now().Add(2*time.Hour))
					counts <- n
					errs <- err
				}()
			}
			wg.Wait()
			close(counts)
			close(errs)
			for err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}
			total := 0
			for n := range counts {
				total += n
			}
			if total != 3 {
				t.Fatalf("want 3 total releases, got %d", total)
			}
			assertSafetyBalance(t, s, prefix+"buyer", 0)
		})
	}
}

func TestSafety_CaptureAndSweepCannotReleaseTwice(t *testing.T) {
	for _, backend := range []string{"memory", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			s, pool, prefix := safetyStore(t, backend)
			h := safetyHold(t, s, prefix, "hold", 100000, time.Hour)
			if pool != nil {
				if _, err := pool.Exec(context.Background(), "UPDATE holds SET expires_at=$1 WHERE id=$2", time.Now().Add(-time.Hour), h.ID); err != nil {
					t.Fatal(err)
				}
			} else {
				s.(*memstore.MemStore).SetClock(func() time.Time { return h.ExpiresAt.Add(time.Second) })
			}
			var wg sync.WaitGroup
			errs := make(chan error, 8)
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					if i%2 == 0 {
						_, err := s.ExpireHolds(context.Background(), h.ExpiresAt.Add(time.Hour))
						errs <- err
					} else {
						_, err := s.CaptureHold(context.Background(), ledger.CaptureRequest{IdempotencyKey: prefix + "capture", HoldID: h.ID, Amount: 50000})
						if errors.Is(err, ledger.ErrHoldExpired) || errors.Is(err, ledger.ErrHoldNotActive) {
							err = nil
						}
						errs <- err
					}
				}(i)
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}
			assertSafetyBalance(t, s, prefix+"buyer", 0)
			expired, _, err := s.GetHold(context.Background(), h.ID)
			if err != nil || expired.Status != ledger.HoldExpired {
				t.Fatalf("hold not expired: %+v err=%v", expired, err)
			}
			if _, ok, err := s.GetTransferByKey(context.Background(), prefix+"capture"); err != nil || ok {
				t.Fatalf("expired race created a transfer: exists=%v err=%v", ok, err)
			}
		})
	}
}

func TestSafety_AccountCreationRejectsInvalidInput(t *testing.T) {
	for _, backend := range []string{"memory", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			s, _, prefix := safetyStore(t, backend)
			for _, a := range []ledger.Account{
				{ID: prefix + "negative", Currency: "INR", Balance: -100000},
				{ID: "system_" + prefix + "fake", Currency: "INR"},
				{ID: "system_reserve_USD", Currency: "USD"},
			} {
				if err := s.CreateAccount(context.Background(), a); err == nil {
					t.Errorf("accepted invalid account: %+v", a)
				}
				if _, err := s.GetAccount(context.Background(), a.ID); !errors.Is(err, ledger.ErrAccountNotFound) {
					t.Errorf("invalid account was persisted: id=%s err=%v", a.ID, err)
				}
			}
			if err := s.CreateAccount(context.Background(), ledger.Account{ID: prefix + "zero", Currency: "INR"}); err != nil {
				t.Fatalf("zero balance must remain valid: %v", err)
			}
		})
	}
}

func TestSafety_ReserveAccountsCannotBeUsedForPublicPayments(t *testing.T) {
	for _, backend := range []string{"memory", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			s, _, prefix := safetyStore(t, backend)
			ctx := context.Background()
			if _, err := s.ApplyTransfer(ctx, ledger.TransferRequest{IdempotencyKey: prefix + "reserve_tx", FromAccountID: prefix + "buyer", ToAccountID: "system_reserve_INR", Amount: 100, Currency: "INR"}); err == nil {
				t.Error("accepted transfer to reserve")
			}
			if _, err := s.AuthorizeHold(ctx, ledger.AuthorizeRequest{IdempotencyKey: prefix + "reserve_hold", FromAccountID: prefix + "buyer", ToAccountID: "system_reserve_INR", Amount: 100, Currency: "INR"}); err == nil {
				t.Error("accepted authorization to reserve")
			}
			entries, err := s.AccountEntries(ctx, prefix+"buyer")
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := s.RefundTransfer(ctx, ledger.RefundRequest{IdempotencyKey: prefix + "reserve_refund", TransferID: entries[0].TransferID, Amount: 100}); err == nil {
				t.Error("accepted refund of internal funding transfer")
			}
			assertSafetyBalance(t, s, prefix+"buyer", 0)
		})
	}
}
