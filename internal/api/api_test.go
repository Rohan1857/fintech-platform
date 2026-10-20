package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/messi/fintech-platform/internal/fraud"
	"github.com/messi/fintech-platform/internal/ledger"
	"github.com/messi/fintech-platform/internal/store/memstore"
)

func setupTestApp() http.Handler {
	s := memstore.New()
	tracker := fraud.NewMemVelocityTracker()
	auditor := fraud.NewMemAuditStore()
	cfg := fraud.DefaultConfig()
	fraudEngine := fraud.NewEngine(cfg, tracker, auditor)

	srv := NewServer(s, fraudEngine, auditor)
	rCfg := DefaultRouterConfig()
	rCfg.AuthToken = "test-secret-token"
	return NewRouter(srv, rCfg)
}

func TestAPI_HealthAndAuth(t *testing.T) {
	app := setupTestApp()

	// 1. Health check is public
	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for /health, got %d", rec.Code)
	}

	// 2. Protected endpoint without token returns 401 Unauthorized
	req = httptest.NewRequest("GET", "/api/v1/accounts/acc1", nil)
	rec = httptest.NewRecorder()
	app.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without auth, got %d", rec.Code)
	}

	// 3. Protected endpoint with valid token succeeds (or returns 404 if not found)
	req = httptest.NewRequest("GET", "/api/v1/accounts/acc1", nil)
	req.Header.Set("Authorization", "Bearer test-secret-token")
	rec = httptest.NewRecorder()
	app.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for missing account, got %d", rec.Code)
	}
}

func TestAPI_AccountCreationAndTransfer(t *testing.T) {
	app := setupTestApp()

	// Create Account 1
	body1, _ := json.Marshal(map[string]any{
		"id":              "acc_alice",
		"currency":        "INR",
		"initial_balance": 10000, // 100 INR
	})
	req := httptest.NewRequest("POST", "/api/v1/accounts", bytes.NewReader(body1))
	req.Header.Set("Authorization", "Bearer test-secret-token")
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create account alice failed: %d, %s", rec.Code, rec.Body.String())
	}

	// Create Account 2
	body2, _ := json.Marshal(map[string]any{
		"id":              "acc_bob",
		"currency":        "INR",
		"initial_balance": 0,
	})
	req = httptest.NewRequest("POST", "/api/v1/accounts", bytes.NewReader(body2))
	req.Header.Set("Authorization", "Bearer test-secret-token")
	rec = httptest.NewRecorder()
	app.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create account bob failed: %d", rec.Code)
	}

	// Execute Transfer: Alice -> Bob 30 INR (3000 paise)
	transferBody, _ := json.Marshal(map[string]any{
		"from_account_id": "acc_alice",
		"to_account_id":   "acc_bob",
		"amount":          3000,
		"currency":        "INR",
	})
	req = httptest.NewRequest("POST", "/api/v1/transfers", bytes.NewReader(transferBody))
	req.Header.Set("Authorization", "Bearer test-secret-token")
	req.Header.Set("Idempotency-Key", "tx_api_001")
	rec = httptest.NewRecorder()
	app.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("transfer failed: %d, %s", rec.Code, rec.Body.String())
	}

	// Verify Bob's Balance
	req = httptest.NewRequest("GET", "/api/v1/accounts/acc_bob", nil)
	req.Header.Set("Authorization", "Bearer test-secret-token")
	rec = httptest.NewRecorder()
	app.ServeHTTP(rec, req)
	var bob ledger.Account
	_ = json.Unmarshal(rec.Body.Bytes(), &bob)
	if bob.Balance != 3000 {
		t.Fatalf("bob balance incorrect: %d", bob.Balance)
	}
}

func TestAPI_AuthCaptureAndRefundFlow(t *testing.T) {
	app := setupTestApp()

	// Seed accounts
	bodyBuyer, _ := json.Marshal(map[string]any{"id": "buyer", "currency": "INR", "initial_balance": 20000})
	bodySeller, _ := json.Marshal(map[string]any{"id": "seller", "currency": "INR", "initial_balance": 0})
	r1 := httptest.NewRequest("POST", "/api/v1/accounts", bytes.NewReader(bodyBuyer))
	r1.Header.Set("Authorization", "Bearer test-secret-token")
	app.ServeHTTP(httptest.NewRecorder(), r1)

	r2 := httptest.NewRequest("POST", "/api/v1/accounts", bytes.NewReader(bodySeller))
	r2.Header.Set("Authorization", "Bearer test-secret-token")
	app.ServeHTTP(httptest.NewRecorder(), r2)

	// 1. Authorize Hold
	authBody, _ := json.Marshal(map[string]any{
		"from_account_id": "buyer",
		"to_account_id":   "seller",
		"amount":          8000,
		"currency":        "INR",
		"expires_in_sec":  3600,
	})
	req := httptest.NewRequest("POST", "/api/v1/payments/authorize", bytes.NewReader(authBody))
	req.Header.Set("Authorization", "Bearer test-secret-token")
	req.Header.Set("Idempotency-Key", "hold_api_001")
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("auth failed: %d, %s", rec.Code, rec.Body.String())
	}

	var hold ledger.Hold
	_ = json.Unmarshal(rec.Body.Bytes(), &hold)

	// 2. Capture Partial Hold (6,000 paise)
	capBody, _ := json.Marshal(map[string]any{
		"hold_id": hold.ID,
		"amount":  6000,
	})
	req = httptest.NewRequest("POST", "/api/v1/payments/capture", bytes.NewReader(capBody))
	req.Header.Set("Authorization", "Bearer test-secret-token")
	req.Header.Set("Idempotency-Key", "cap_api_001")
	rec = httptest.NewRecorder()
	app.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("capture failed: %d, %s", rec.Code, rec.Body.String())
	}

	var capTx ledger.Transfer
	_ = json.Unmarshal(rec.Body.Bytes(), &capTx)

	// 3. Refund 2,000 paise
	refBody, _ := json.Marshal(map[string]any{
		"transfer_id": capTx.ID,
		"amount":      2000,
		"reason":      "item returned",
	})
	req = httptest.NewRequest("POST", "/api/v1/payments/refund", bytes.NewReader(refBody))
	req.Header.Set("Authorization", "Bearer test-secret-token")
	req.Header.Set("Idempotency-Key", "ref_api_001")
	rec = httptest.NewRecorder()
	app.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("refund failed: %d, %s", rec.Code, rec.Body.String())
	}

	// 4. Verify Final Balances
	req = httptest.NewRequest("GET", "/api/v1/accounts/buyer", nil)
	req.Header.Set("Authorization", "Bearer test-secret-token")
	rec = httptest.NewRecorder()
	app.ServeHTTP(rec, req)
	var buyer ledger.Account
	_ = json.Unmarshal(rec.Body.Bytes(), &buyer)
	// Initial: 20000, Captured: -6000, Refunded: +2000 -> Final: 16000
	if buyer.Balance != 16000 || buyer.Held != 0 {
		t.Fatalf("buyer balance incorrect: %d, held=%d", buyer.Balance, buyer.Held)
	}

	// 5. Run Admin Reconciliation
	req = httptest.NewRequest("POST", "/api/v1/reconciliation", nil)
	req.Header.Set("Authorization", "Bearer test-secret-token")
	rec = httptest.NewRecorder()
	app.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("reconciliation failed: %d, %s", rec.Code, rec.Body.String())
	}
}

func TestAPI_FraudRejectionEndpoint(t *testing.T) {
	app := setupTestApp()

	// Seed account with 10,000,000 paise (100,000 INR)
	body, _ := json.Marshal(map[string]any{"id": "whale", "currency": "INR", "initial_balance": 10000000})
	r1 := httptest.NewRequest("POST", "/api/v1/accounts", bytes.NewReader(body))
	r1.Header.Set("Authorization", "Bearer test-secret-token")
	app.ServeHTTP(httptest.NewRecorder(), r1)

	r2 := httptest.NewRequest("POST", "/api/v1/accounts", bytes.NewReader([]byte(`{"id":"target","currency":"INR","initial_balance":0}`)))
	r2.Header.Set("Authorization", "Bearer test-secret-token")
	app.ServeHTTP(httptest.NewRecorder(), r2)

	// Attempt transfer that exceeds Critical Value Threshold (250,000 INR = 25,000,000 paise)
	// Or multiple rapid transfers exceeding velocity threshold
	transferBody, _ := json.Marshal(map[string]any{
		"from_account_id": "whale",
		"to_account_id":   "target",
		"amount":          25000000, // 250,000 INR -> Critical value rule
		"currency":        "INR",
	})
	req := httptest.NewRequest("POST", "/api/v1/transfers", bytes.NewReader(transferBody))
	req.Header.Set("Authorization", "Bearer test-secret-token")
	req.Header.Set("Idempotency-Key", "fraud_test_key")
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)

	// Must be rejected or accepted for manual review (not approved 200!)
	if rec.Code == http.StatusOK {
		t.Fatalf("expected fraud rejection or review, but got 200 OK!")
	}
}
