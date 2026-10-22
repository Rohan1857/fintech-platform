package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Rohan1857/fintech-platform/internal/ledger"
	"github.com/Rohan1857/fintech-platform/internal/store/memstore"
)

func TestAPI_InvalidAccountCreationDoesNotPersist(t *testing.T) {
	for _, body := range []string{
		`{"id":"negative","initial_balance":-100000}`,
		`{"id":"system_fake","initial_balance":0}`,
		`{"id":"system_reserve_INR","initial_balance":0}`,
	} {
		t.Run(body, func(t *testing.T) {
			s := memstore.New()
			cfg := DefaultRouterConfig()
			cfg.AuthToken = "test-secret-token"
			app := NewRouter(NewServer(s, nil, nil), cfg)
			req := httptest.NewRequest("POST", "/api/v1/accounts", bytes.NewBufferString(body))
			req.Header.Set("Authorization", "Bearer test-secret-token")
			rec := httptest.NewRecorder()
			app.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("want 400, got %d: %s", rec.Code, rec.Body.String())
			}
			var input struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal([]byte(body), &input); err != nil {
				t.Fatal(err)
			}
			if _, err := s.GetAccount(context.Background(), input.ID); !errors.Is(err, ledger.ErrAccountNotFound) {
				t.Fatalf("invalid account persisted: %v", err)
			}
		})
	}
}

func TestAPI_DefaultRouterRejectsPublishedToken(t *testing.T) {
	app := NewRouter(NewServer(memstore.New(), nil, nil), DefaultRouterConfig())
	req := httptest.NewRequest("GET", "/api/v1/accounts/missing", nil)
	req.Header.Set("Authorization", "Bearer slice-secret-token-2026")
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("public token authenticated: HTTP %d", rec.Code)
	}
}

func TestAPI_ReservedAccountRejectedBeforeRiskAssessment(t *testing.T) {
	app, _, _ := safetyApp(t)
	req := httptest.NewRequest("POST", "/api/v1/transfers", bytes.NewBufferString(`{"from_account_id":"system_reserve_INR","to_account_id":"buyer","amount":100000000,"currency":"INR"}`))
	req.Header.Set("Authorization", "Bearer test-secret-token")
	req.Header.Set("Idempotency-Key", "opening_buyer")
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("internal funding accepted as public replay: %d %s", rec.Code, rec.Body.String())
	}
}
