package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Rohan1857/fintech-platform/internal/api"
	"github.com/Rohan1857/fintech-platform/internal/ledger"
	"github.com/Rohan1857/fintech-platform/internal/store/postgres"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestPostgres_SystemPrivilegeRequiresExactReserveIdentity(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()
	for _, tc := range []struct {
		id, currency string
		balance      int64
	}{
		{"systemXconstraint_test", "INR", -1},
		{"system_invalid_constraint_test", "INR", 0},
		{"system_reserve_BAD", "INR", -1},
	} {
		t.Run(tc.id, func(t *testing.T) {
			tx, err := pool.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(context.Background())
			_, err = tx.Exec(context.Background(), "INSERT INTO accounts (id,currency,balance,held) VALUES ($1,$2,$3,0)", tc.id, tc.currency, tc.balance)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
				t.Fatalf("expected account constraint rejection, got %v", err)
			}
		})
	}
}

func TestPostgres_InternalFundingReplayCannotBypassAPIValidation(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()
	s := postgres.New(pool)
	id := fmt.Sprintf("replay_%d", time.Now().UnixNano())
	if err := s.CreateAccount(context.Background(), ledger.Account{ID: id, Currency: "INR", Balance: 10000}); err != nil {
		t.Fatal(err)
	}
	cfg := api.DefaultRouterConfig()
	cfg.AuthToken = "test-private-token"
	app := api.NewRouter(api.NewServer(s, nil, nil), cfg)
	body := fmt.Sprintf(`{"from_account_id":"system_reserve_INR","to_account_id":%q,"amount":10000,"currency":"INR"}`, id)
	req := httptest.NewRequest("POST", "/api/v1/transfers", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+cfg.AuthToken)
	req.Header.Set("Idempotency-Key", "opening_"+id)
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("internal entry replayed as public payment: %d %s", rec.Code, rec.Body.String())
	}
}

func TestPostgres_LegacyReserveHoldCannotBeCaptured(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()
	ctx := context.Background()
	s := postgres.New(pool)
	id := fmt.Sprintf("legacy_%d", time.Now().UnixNano())
	if err := s.CreateAccount(ctx, ledger.Account{ID: id, Currency: "INR", Balance: 10000}); err != nil {
		t.Fatal(err)
	}
	// Reproduce an internal-account authorization left by an older installation.
	if _, err := pool.Exec(ctx, "INSERT INTO holds (id,idempotency_key,from_account_id,to_account_id,amount,status) VALUES ($1,$1,'system_reserve_INR',$2,5000,'active')", id+"_hold", id); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "UPDATE accounts SET held=held+5000 WHERE id='system_reserve_INR'"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CaptureHold(ctx, ledger.CaptureRequest{IdempotencyKey: id + "_capture", HoldID: id + "_hold", Amount: 5000}); err == nil {
		t.Fatal("legacy hold moved reserve funds through public capture")
	}
	account, err := s.GetAccount(ctx, id)
	if err != nil || account.Balance != 10000 {
		t.Fatalf("reserve capture changed customer funds: %+v err=%v", account, err)
	}
}
