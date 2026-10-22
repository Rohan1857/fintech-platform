package ledger

import (
	"errors"
	"time"

	"github.com/Rohan1857/fintech-platform/pkg/money"
)

// TransferStatus represents state of a ledger transfer.
type TransferStatus string

const (
	StatusPosted   TransferStatus = "posted"
	StatusFailed   TransferStatus = "failed"
	StatusRefunded TransferStatus = "refunded"
)

// TransferType distinguishes normal transfers, captures, and refunds.
type TransferType string

const (
	TransferTypeStandard    TransferType = "standard"
	TransferTypeHoldCapture TransferType = "hold_capture"
	TransferTypeRefund      TransferType = "refund"
)

// HoldStatus represents the lifecycle of an authorization hold.
type HoldStatus string

const (
	HoldActive   HoldStatus = "active"
	HoldCaptured HoldStatus = "captured"
	HoldVoided   HoldStatus = "voided"
	HoldExpired  HoldStatus = "expired"
)

// Account is a holder of money in a single currency.
type Account struct {
	ID        string      `json:"id"`
	Currency  string      `json:"currency"`
	Balance   money.Money `json:"balance"` // Settled funds
	Held      money.Money `json:"held"`    // Reserved by active holds
	CreatedAt time.Time   `json:"created_at"`
	UpdatedAt time.Time   `json:"updated_at"`
}

// Available returns spendable balance: Balance - Held.
func (a Account) Available() money.Money {
	return a.Balance - a.Held
}

// Entry is one debit or credit leg posted against an account.
// Negative amount = debit (leaving account). Positive amount = credit (arriving).
type Entry struct {
	ID         string      `json:"id"`
	TransferID string      `json:"transfer_id"`
	AccountID  string      `json:"account_id"`
	Amount     money.Money `json:"amount"`
	CreatedAt  time.Time   `json:"created_at"`
}

// Posting is an instruction leg for a multi-leg or direct transfer.
type Posting struct {
	AccountID string      `json:"account_id"`
	Amount    money.Money `json:"amount"`
}

// Transfer records an atomic, balanced financial movement.
type Transfer struct {
	ID               string         `json:"id"`
	IdempotencyKey   string         `json:"idempotency_key"`
	FromAccountID    string         `json:"from_account_id,omitempty"`
	ToAccountID      string         `json:"to_account_id,omitempty"`
	Amount           money.Money    `json:"amount,omitempty"`
	Currency         string         `json:"currency"`
	Status           TransferStatus `json:"status"`
	Type             TransferType   `json:"type"`
	ParentTransferID string         `json:"parent_transfer_id,omitempty"`
	CreatedAt        time.Time      `json:"created_at"`
	Entries          []Entry        `json:"entries"`
}

// Hold represents an authorization hold fencing off funds without moving them yet.
type Hold struct {
	ID                string      `json:"id"`
	IdempotencyKey    string      `json:"idempotency_key"`
	FromAccountID     string      `json:"from_account_id"`
	ToAccountID       string      `json:"to_account_id"`
	Amount            money.Money `json:"amount"`
	Captured          money.Money `json:"captured"`
	Status            HoldStatus  `json:"status"`
	CreatedAt         time.Time   `json:"created_at"`
	ExpiresAt         time.Time   `json:"expires_at,omitempty"`
	CaptureTransferID string      `json:"capture_transfer_id,omitempty"`
}

// Refund tracks a refund linked to an original transfer.
type Refund struct {
	ID                 string      `json:"id"`
	IdempotencyKey     string      `json:"idempotency_key"`
	OriginalTransferID string      `json:"original_transfer_id"`
	RefundTransferID   string      `json:"refund_transfer_id"`
	Amount             money.Money `json:"amount"`
	Reason             string      `json:"reason"`
	CreatedAt          time.Time   `json:"created_at"`
}

// Request DTOs
type TransferRequest struct {
	IdempotencyKey string      `json:"idempotency_key"`
	FromAccountID  string      `json:"from_account_id"`
	ToAccountID    string      `json:"to_account_id"`
	Amount         money.Money `json:"amount"`
	Currency       string      `json:"currency"`
}

type PostRequest struct {
	IdempotencyKey string    `json:"idempotency_key"`
	Currency       string    `json:"currency"`
	Postings       []Posting `json:"postings"`
}

type AuthorizeRequest struct {
	IdempotencyKey string        `json:"idempotency_key"`
	FromAccountID  string        `json:"from_account_id"`
	ToAccountID    string        `json:"to_account_id"`
	Amount         money.Money   `json:"amount"`
	Currency       string        `json:"currency"`
	ExpiresIn      time.Duration `json:"expires_in"`
}

type CaptureRequest struct {
	IdempotencyKey string      `json:"idempotency_key"`
	HoldID         string      `json:"hold_id"`
	Amount         money.Money `json:"amount"`
}

type VoidRequest struct {
	IdempotencyKey string `json:"idempotency_key"`
	HoldID         string `json:"hold_id"`
}

type RefundRequest struct {
	IdempotencyKey string      `json:"idempotency_key"`
	TransferID     string      `json:"transfer_id"`
	Amount         money.Money `json:"amount"` // 0 indicates full refund
	Reason         string      `json:"reason"`
}

// Sentinel Domain Errors
var (
	ErrInvalidAmount         = errors.New("ledger: amount must be positive")
	ErrInvalidInitialBalance = errors.New("ledger: initial balance must be non-negative")
	ErrReservedAccount       = errors.New("ledger: system accounts are reserved for internal accounting")
	ErrSameAccount           = errors.New("ledger: source and destination must differ")
	ErrMissingIdempotencyKey = errors.New("ledger: idempotency key is required")
	ErrAccountNotFound       = errors.New("ledger: account not found")
	ErrAccountExists         = errors.New("ledger: account already exists with different attributes")
	ErrCurrencyMismatch      = errors.New("ledger: account currencies do not match")
	ErrInsufficientFunds     = errors.New("ledger: insufficient available funds")
	ErrIdempotencyConflict   = errors.New("ledger: idempotency key reused with different parameters")
	ErrTooFewPostings        = errors.New("ledger: a posting needs at least two legs")
	ErrZeroPosting           = errors.New("ledger: posting amounts must be non-zero")
	ErrDuplicateAccount      = errors.New("ledger: postings must not repeat an account")
	ErrUnbalancedPostings    = errors.New("ledger: postings must sum to zero")
	ErrHoldNotFound          = errors.New("ledger: hold not found")
	ErrHoldNotActive         = errors.New("ledger: hold is not active")
	ErrHoldExpired           = errors.New("ledger: hold has expired")
	ErrCaptureExceedsHold    = errors.New("ledger: capture amount exceeds held amount")
	ErrTransferNotFound      = errors.New("ledger: transfer not found")
	ErrTransferNotRefundable = errors.New("ledger: transfer cannot be refunded")
	ErrRefundExceedsTransfer = errors.New("ledger: refund amount exceeds transferable amount")
)
