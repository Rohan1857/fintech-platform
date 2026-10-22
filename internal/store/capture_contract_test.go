package store_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Rohan1857/fintech-platform/internal/ledger"
	"github.com/Rohan1857/fintech-platform/internal/store"
	"github.com/Rohan1857/fintech-platform/internal/store/memstore"
	"github.com/Rohan1857/fintech-platform/internal/store/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestCaptureReplayIsBoundToOriginalHold(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"memory", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var s store.Store = memstore.New()
			if backend == "postgres" {
				url := os.Getenv("DATABASE_URL")
				if url == "" {
					t.Fatal("DATABASE_URL must point to a disposable test database")
				}
				pool, err := pgxpool.New(ctx, url)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(pool.Close)
				s = postgres.New(pool)
			}
			prefix := fmt.Sprintf("capture_%d_", time.Now().UnixNano())
			for _, account := range []ledger.Account{
				{ID: prefix + "buyer", Currency: "INR", Balance: 10000},
				{ID: prefix + "seller", Currency: "INR"},
			} {
				if err := s.CreateAccount(ctx, account); err != nil {
					t.Fatal(err)
				}
			}
			var holds []ledger.Hold
			for _, key := range []string{"first", "second"} {
				h, err := s.AuthorizeHold(ctx, ledger.AuthorizeRequest{
					IdempotencyKey: prefix + key, FromAccountID: prefix + "buyer", ToAccountID: prefix + "seller", Amount: 1000, Currency: "INR",
				})
				if err != nil {
					t.Fatal(err)
				}
				holds = append(holds, h)
			}
			req := ledger.CaptureRequest{IdempotencyKey: prefix + "capture", HoldID: holds[0].ID, Amount: 500}
			first, err := s.CaptureHold(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			replay, err := s.CaptureHold(ctx, req)
			if err != nil || replay.ID != first.ID {
				t.Fatalf("original replay failed: %+v err=%v", replay, err)
			}
			req.HoldID = holds[1].ID
			if _, err := s.CaptureHold(ctx, req); !errors.Is(err, ledger.ErrIdempotencyConflict) {
				t.Fatalf("same capture key accepted for another hold: %v", err)
			}
			buyer, err := s.GetAccount(ctx, prefix+"buyer")
			if err != nil || buyer.Balance != 9500 || buyer.Held != 1000 {
				t.Fatalf("capture replay mutated funds: %+v err=%v", buyer, err)
			}
		})
	}
}
