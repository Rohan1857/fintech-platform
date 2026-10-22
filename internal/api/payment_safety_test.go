package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Rohan1857/fintech-platform/internal/fraud"
	"github.com/Rohan1857/fintech-platform/internal/ledger"
	"github.com/Rohan1857/fintech-platform/internal/store/memstore"
)

type switchableTracker struct {
	*fraud.MemVelocityTracker
	offline bool
}

func (s *switchableTracker) IsHealthy(context.Context) bool { return !s.offline }

func paymentRequest(t *testing.T, app http.Handler, path, key string, amount int64) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"from_account_id": "buyer", "to_account_id": "seller", "amount": amount, "currency": "INR",
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-secret-token")
	req.Header.Set("Idempotency-Key", key)
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)
	return rec
}

func safetyApp(t *testing.T) (http.Handler, *memstore.MemStore, *switchableTracker) {
	t.Helper()
	s := memstore.New()
	for _, a := range []ledger.Account{
		{ID: "buyer", Currency: "INR", Balance: 100000000},
		{ID: "seller", Currency: "INR"},
	} {
		if err := s.CreateAccount(context.Background(), a); err != nil {
			t.Fatal(err)
		}
	}
	tracker := &switchableTracker{MemVelocityTracker: fraud.NewMemVelocityTracker()}
	auditor := fraud.NewMemAuditStore()
	srv := NewServer(s, fraud.NewEngine(fraud.DefaultConfig(), tracker, auditor), auditor)
	cfg := DefaultRouterConfig()
	cfg.AuthToken = "test-secret-token"
	return NewRouter(srv, cfg), s, tracker
}

func TestAPI_ReviewDoesNotMoveOrReserveFunds(t *testing.T) {
	for _, path := range []string{"/api/v1/transfers", "/api/v1/payments/authorize"} {
		t.Run(path, func(t *testing.T) {
			app, s, tracker := safetyApp(t)
			// Three recent payments to the same beneficiary trigger REVIEW
			// (beneficiary score 40 + burst score 30) under the default rules.
			for i := 0; i < 3; i++ {
				if err := tracker.RecordTransaction(context.Background(), "buyer", "seller", 1000, time.Now()); err != nil {
					t.Fatal(err)
				}
			}
			rec := paymentRequest(t, app, path, "review", 1000)
			if rec.Code != http.StatusAccepted {
				t.Fatalf("want 202 for review, got %d: %s", rec.Code, rec.Body.String())
			}
			var result struct {
				Status string `json:"status"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil || result.Status != "pending_review" {
				t.Fatalf("want pending_review assessment, got %s (err=%v)", rec.Body.String(), err)
			}
			buyer, err := s.GetAccount(context.Background(), "buyer")
			if err != nil || buyer.Balance != 100000000 || buyer.Held != 0 {
				t.Fatalf("review mutated buyer: %+v (err=%v)", buyer, err)
			}
			if _, ok, err := s.GetHoldByKey(context.Background(), "review"); err != nil || ok {
				t.Fatalf("review created a hold: exists=%v err=%v", ok, err)
			}
			if _, ok, err := s.GetTransferByKey(context.Background(), "review"); err != nil || ok {
				t.Fatalf("review created a transfer: exists=%v err=%v", ok, err)
			}
		})
	}
}

func TestAPI_CommittedReplaySurvivesRiskOutage(t *testing.T) {
	for _, tc := range []struct {
		path          string
		status        int
		balance, held int64
	}{
		{"/api/v1/transfers", http.StatusOK, 99999000, 0},
		{"/api/v1/payments/authorize", http.StatusCreated, 100000000, 1000},
	} {
		t.Run(tc.path, func(t *testing.T) {
			app, s, tracker := safetyApp(t)
			first := paymentRequest(t, app, tc.path, "committed", 1000)
			if first.Code != tc.status {
				t.Fatalf("first request: %d %s", first.Code, first.Body.String())
			}
			tracker.offline = true
			for i := 0; i < 5; i++ {
				replay := paymentRequest(t, app, tc.path, "committed", 1000)
				if replay.Code != tc.status || replay.Body.String() != first.Body.String() {
					t.Fatalf("committed replay changed: %d %s", replay.Code, replay.Body.String())
				}
			}
			conflict := paymentRequest(t, app, tc.path, "committed", 2000)
			if conflict.Code != http.StatusConflict {
				t.Fatalf("want 409 for changed payload, got %d", conflict.Code)
			}
			fresh := paymentRequest(t, app, tc.path, "new-payment", 1000)
			if fresh.Code != http.StatusForbidden {
				t.Fatalf("new payment bypassed outage: %d", fresh.Code)
			}
			buyer, err := s.GetAccount(context.Background(), "buyer")
			if err != nil || int64(buyer.Balance) != tc.balance || int64(buyer.Held) != tc.held {
				t.Fatalf("unexpected money movement: %+v err=%v", buyer, err)
			}
			count, _, err := tracker.GetVelocity(context.Background(), "buyer", fraud.DefaultConfig().VelocityWindowShort)
			if err != nil || count != 1 {
				t.Fatalf("replays counted as new attempts: count=%d err=%v", count, err)
			}
		})
	}
}
