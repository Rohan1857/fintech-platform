package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/messi/fintech-platform/internal/fraud"
	"github.com/messi/fintech-platform/internal/ledger"
	"github.com/messi/fintech-platform/internal/store"
	"github.com/messi/fintech-platform/pkg/money"
)

type Server struct {
	store   store.Store
	fraud   *fraud.Engine
	auditor fraud.AuditStore
}

func NewServer(store store.Store, fraud *fraud.Engine, auditor fraud.AuditStore) *Server {
	return &Server{
		store:   store,
		fraud:   fraud,
		auditor: auditor,
	}
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

// POST /api/v1/accounts
type createAccountRequest struct {
	ID             string      `json:"id"`
	Currency       string      `json:"currency"`
	InitialBalance money.Money `json:"initial_balance"`
}

func (s *Server) HandleCreateAccount(w http.ResponseWriter, r *http.Request) {
	var req createAccountRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.ID == "" {
		writeError(w, http.StatusBadRequest, "account id is required")
		return
	}
	if req.Currency == "" {
		req.Currency = money.CurrencyINR
	}

	acct := ledger.Account{
		ID:       req.ID,
		Currency: req.Currency,
		Balance:  req.InitialBalance,
		Held:     0,
	}

	if err := s.store.CreateAccount(r.Context(), acct); err != nil {
		if errors.Is(err, ledger.ErrAccountExists) {
			writeError(w, http.StatusConflict, "account already exists with different attributes")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	created, err := s.store.GetAccount(r.Context(), req.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, created)
}

// GET /api/v1/accounts/{id}
func (s *Server) HandleGetAccount(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing account id")
		return
	}

	acct, err := s.store.GetAccount(r.Context(), id)
	if err != nil {
		if errors.Is(err, ledger.ErrAccountNotFound) {
			writeError(w, http.StatusNotFound, "account not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, acct)
}

// GET /api/v1/accounts/{id}/history
func (s *Server) HandleGetHistory(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing account id")
		return
	}

	entries, err := s.store.AccountEntries(r.Context(), id)
	if err != nil {
		if errors.Is(err, ledger.ErrAccountNotFound) {
			writeError(w, http.StatusNotFound, "account not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"account_id": id,
		"entries":    entries,
	})
}

// POST /api/v1/transfers
type transferInput struct {
	FromAccountID string      `json:"from_account_id"`
	ToAccountID   string      `json:"to_account_id"`
	Amount        money.Money `json:"amount"`
	Currency      string      `json:"currency"`
}

func (s *Server) HandleTransfer(w http.ResponseWriter, r *http.Request) {
	idemKey := r.Header.Get("Idempotency-Key")
	if idemKey == "" {
		writeError(w, http.StatusBadRequest, "Idempotency-Key header is required")
		return
	}

	var req transferInput
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Amount <= 0 {
		writeError(w, http.StatusBadRequest, "amount must be positive")
		return
	}
	if req.FromAccountID == req.ToAccountID {
		writeError(w, http.StatusBadRequest, "source and destination accounts must differ")
		return
	}
	if req.Currency == "" {
		req.Currency = money.CurrencyINR
	}

	// Synchronous Fraud Evaluation
	if s.fraud != nil {
		fraudReq := fraud.EvaluationRequest{
			IdempotencyKey: idemKey,
			AccountID:      req.FromAccountID,
			BeneficiaryID:  req.ToAccountID,
			Amount:         req.Amount,
			Currency:       req.Currency,
			Timestamp:      time.Now().UTC(),
		}
		evalRes, err := s.fraud.Evaluate(r.Context(), fraudReq)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "fraud evaluation error")
			return
		}
		if evalRes.Decision == fraud.DecisionReject {
			writeJSON(w, http.StatusForbidden, map[string]any{
				"error":          "transaction rejected by fraud risk engine",
				"evaluation_id":  evalRes.EvaluationID,
				"decision":       evalRes.Decision,
				"risk_score":     evalRes.RiskScore,
				"reasons":        evalRes.Reasons,
			})
			return
		}
		if evalRes.Decision == fraud.DecisionReview {
			writeJSON(w, http.StatusAccepted, map[string]any{
				"status":         "pending_review",
				"evaluation_id":  evalRes.EvaluationID,
				"decision":       evalRes.Decision,
				"risk_score":     evalRes.RiskScore,
				"reasons":        evalRes.Reasons,
			})
			return
		}
	}

	// Apply ledger transfer
	tx, err := s.store.ApplyTransfer(r.Context(), ledger.TransferRequest{
		IdempotencyKey: idemKey,
		FromAccountID:  req.FromAccountID,
		ToAccountID:    req.ToAccountID,
		Amount:         req.Amount,
		Currency:       req.Currency,
	})
	if err != nil {
		handleLedgerError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, tx)
}

// POST /api/v1/payments/authorize
type authorizeInput struct {
	FromAccountID string      `json:"from_account_id"`
	ToAccountID   string      `json:"to_account_id"`
	Amount        money.Money `json:"amount"`
	Currency      string      `json:"currency"`
	ExpiresInSec  int64       `json:"expires_in_sec"`
}

func (s *Server) HandleAuthorize(w http.ResponseWriter, r *http.Request) {
	idemKey := r.Header.Get("Idempotency-Key")
	if idemKey == "" {
		writeError(w, http.StatusBadRequest, "Idempotency-Key header is required")
		return
	}

	var req authorizeInput
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Amount <= 0 {
		writeError(w, http.StatusBadRequest, "amount must be positive")
		return
	}
	if req.Currency == "" {
		req.Currency = money.CurrencyINR
	}

	// Synchronous Fraud Evaluation
	if s.fraud != nil {
		fraudReq := fraud.EvaluationRequest{
			IdempotencyKey: idemKey,
			AccountID:      req.FromAccountID,
			BeneficiaryID:  req.ToAccountID,
			Amount:         req.Amount,
			Currency:       req.Currency,
			Timestamp:      time.Now().UTC(),
		}
		evalRes, err := s.fraud.Evaluate(r.Context(), fraudReq)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "fraud evaluation error")
			return
		}
		if evalRes.Decision == fraud.DecisionReject {
			writeJSON(w, http.StatusForbidden, map[string]any{
				"error":         "authorization rejected by fraud risk engine",
				"evaluation_id": evalRes.EvaluationID,
				"decision":      evalRes.Decision,
				"risk_score":    evalRes.RiskScore,
				"reasons":       evalRes.Reasons,
			})
			return
		}
	}

	var expiresIn time.Duration
	if req.ExpiresInSec > 0 {
		expiresIn = time.Duration(req.ExpiresInSec) * time.Second
	}

	hold, err := s.store.AuthorizeHold(r.Context(), ledger.AuthorizeRequest{
		IdempotencyKey: idemKey,
		FromAccountID:  req.FromAccountID,
		ToAccountID:    req.ToAccountID,
		Amount:         req.Amount,
		Currency:       req.Currency,
		ExpiresIn:      expiresIn,
	})
	if err != nil {
		handleLedgerError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, hold)
}

// POST /api/v1/payments/capture
type captureInput struct {
	HoldID string      `json:"hold_id"`
	Amount money.Money `json:"amount"`
}

func (s *Server) HandleCapture(w http.ResponseWriter, r *http.Request) {
	idemKey := r.Header.Get("Idempotency-Key")
	if idemKey == "" {
		writeError(w, http.StatusBadRequest, "Idempotency-Key header is required")
		return
	}

	var req captureInput
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	tx, err := s.store.CaptureHold(r.Context(), ledger.CaptureRequest{
		IdempotencyKey: idemKey,
		HoldID:         req.HoldID,
		Amount:         req.Amount,
	})
	if err != nil {
		handleLedgerError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, tx)
}

// POST /api/v1/payments/void
type voidInput struct {
	HoldID string `json:"hold_id"`
}

func (s *Server) HandleVoid(w http.ResponseWriter, r *http.Request) {
	idemKey := r.Header.Get("Idempotency-Key")
	if idemKey == "" {
		writeError(w, http.StatusBadRequest, "Idempotency-Key header is required")
		return
	}

	var req voidInput
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	hold, err := s.store.VoidHold(r.Context(), ledger.VoidRequest{
		IdempotencyKey: idemKey,
		HoldID:         req.HoldID,
	})
	if err != nil {
		handleLedgerError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, hold)
}

// POST /api/v1/payments/refund
type refundInput struct {
	TransferID string      `json:"transfer_id"`
	Amount     money.Money `json:"amount"` // 0 indicates full refund
	Reason     string      `json:"reason"`
}

func (s *Server) HandleRefund(w http.ResponseWriter, r *http.Request) {
	idemKey := r.Header.Get("Idempotency-Key")
	if idemKey == "" {
		writeError(w, http.StatusBadRequest, "Idempotency-Key header is required")
		return
	}

	var req refundInput
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	refundRec, refundTx, err := s.store.RefundTransfer(r.Context(), ledger.RefundRequest{
		IdempotencyKey: idemKey,
		TransferID:     req.TransferID,
		Amount:         req.Amount,
		Reason:         req.Reason,
	})
	if err != nil {
		handleLedgerError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"refund":   refundRec,
		"transfer": refundTx,
	})
}

// GET /api/v1/fraud/evaluations/{id}
func (s *Server) HandleGetFraudEvaluation(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing evaluation id")
		return
	}

	if s.auditor == nil {
		writeError(w, http.StatusNotImplemented, "audit store not configured")
		return
	}

	eval, err := s.auditor.GetEvaluation(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "evaluation record not found")
		return
	}

	writeJSON(w, http.StatusOK, eval)
}

// POST /api/v1/reconciliation
func (s *Server) HandleReconcile(w http.ResponseWriter, r *http.Request) {
	rep, err := s.store.Reconcile(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	status := http.StatusOK
	if !rep.IsClean {
		status = http.StatusConflict
	}

	writeJSON(w, status, rep)
}

// GET /health
func (s *Server) HandleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":    "healthy",
		"timestamp": time.Now().UTC(),
	})
}

func handleLedgerError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ledger.ErrAccountNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, ledger.ErrInsufficientFunds):
		writeError(w, http.StatusPaymentRequired, err.Error())
	case errors.Is(err, ledger.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, ledger.ErrInvalidAmount),
		errors.Is(err, ledger.ErrSameAccount),
		errors.Is(err, ledger.ErrMissingIdempotencyKey),
		errors.Is(err, ledger.ErrCurrencyMismatch),
		errors.Is(err, ledger.ErrTooFewPostings),
		errors.Is(err, ledger.ErrZeroPosting),
		errors.Is(err, ledger.ErrDuplicateAccount),
		errors.Is(err, ledger.ErrUnbalancedPostings):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, ledger.ErrHoldNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, ledger.ErrHoldNotActive),
		errors.Is(err, ledger.ErrHoldExpired),
		errors.Is(err, ledger.ErrCaptureExceedsHold),
		errors.Is(err, ledger.ErrTransferNotFound),
		errors.Is(err, ledger.ErrTransferNotRefundable),
		errors.Is(err, ledger.ErrRefundExceedsTransfer):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}
