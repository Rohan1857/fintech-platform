package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Rohan1857/fintech-platform/internal/ledger"
	"github.com/Rohan1857/fintech-platform/internal/store/memstore"
)

func TestAuthTokenRequiresExplicitPrivateValue(t *testing.T) {
	for _, tc := range []struct {
		token string
		valid bool
	}{
		{"", false}, {"   ", false}, {"slice-secret-token-2026", false},
		{"private-test-token-not-a-default", true},
	} {
		t.Run(tc.token, func(t *testing.T) {
			t.Setenv("API_AUTH_TOKEN", tc.token)
			got, err := loadAuthToken()
			if tc.valid {
				if err != nil || got != tc.token {
					t.Fatalf("explicit token rejected: err=%v", err)
				}
			} else if err == nil {
				t.Fatal("unsafe authentication configuration accepted")
			}
		})
	}
}

func TestExpiryWorkerReleasesUntouchedHoldsAndStops(t *testing.T) {
	s := memstore.New()
	now := time.Now().UTC()
	s.SetClock(func() time.Time { return now.Add(-2 * time.Hour) })
	for _, a := range []ledger.Account{{ID: "buyer", Currency: "INR", Balance: 10000}, {ID: "seller", Currency: "INR"}} {
		if err := s.CreateAccount(context.Background(), a); err != nil {
			t.Fatal(err)
		}
	}
	h, err := s.AuthorizeHold(context.Background(), ledger.AuthorizeRequest{IdempotencyKey: "expired", FromAccountID: "buyer", ToAccountID: "seller", Amount: 5000, ExpiresIn: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		runHoldExpiry(ctx, s, 5*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	deadline := time.After(2 * time.Second)
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for {
		current, _, err := s.GetHold(context.Background(), h.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.Status == ledger.HoldExpired {
			break
		}
		select {
		case <-deadline:
			t.Fatal("untouched hold was not expired")
		case <-poll.C:
		}
	}
	buyer, err := s.GetAccount(context.Background(), "buyer")
	if err != nil || buyer.Balance != 10000 || buyer.Held != 0 {
		t.Fatalf("expiry changed settled funds or retained reservation: %+v err=%v", buyer, err)
	}
	// Create another expired hold after the first pass, proving periodic work
	// continues rather than only cleaning up once at startup.
	second, err := s.AuthorizeHold(context.Background(), ledger.AuthorizeRequest{IdempotencyKey: "after-startup", FromAccountID: "buyer", ToAccountID: "seller", Amount: 2000, ExpiresIn: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	for {
		current, _, err := s.GetHold(context.Background(), second.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.Status == ledger.HoldExpired {
			break
		}
		select {
		case <-deadline:
			t.Fatal("periodic sweep did not release new expired hold")
		case <-poll.C:
		}
	}
	buyer, err = s.GetAccount(context.Background(), "buyer")
	if err != nil || buyer.Balance != 10000 || buyer.Held != 0 {
		t.Fatalf("periodic release incorrect: %+v err=%v", buyer, err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expiry worker did not stop on cancellation")
	}
}
