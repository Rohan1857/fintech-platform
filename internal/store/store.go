package store

import (
	"context"
	"time"

	"github.com/messi/fintech-platform/internal/ledger"
	"github.com/messi/fintech-platform/pkg/money"
)

// ReconciliationReport summarizes double-entry consistency audit results.
type ReconciliationReport struct {
	TotalAccounts       int         `json:"total_accounts"`
	TotalTransfers      int         `json:"total_transfers"`
	TotalEntries        int         `json:"total_entries"`
	TotalBalanceMinor   money.Money `json:"total_balance_minor"`
	TotalEntriesSum     money.Money `json:"total_entries_sum"`
	UnbalancedTransfers []string    `json:"unbalanced_transfers,omitempty"`
	MismatchedAccounts  []string    `json:"mismatched_accounts,omitempty"`
	IsClean             bool        `json:"is_clean"`
}

// Store defines all storage operations for ledger, holds, refunds, and outbox.
type Store interface {
	CreateAccount(ctx context.Context, a ledger.Account) error
	GetAccount(ctx context.Context, id string) (ledger.Account, error)
	ApplyPosting(ctx context.Context, req ledger.PostRequest) (ledger.Transfer, error)
	ApplyTransfer(ctx context.Context, req ledger.TransferRequest) (ledger.Transfer, error)
	AuthorizeHold(ctx context.Context, req ledger.AuthorizeRequest) (ledger.Hold, error)
	CaptureHold(ctx context.Context, req ledger.CaptureRequest) (ledger.Transfer, error)
	VoidHold(ctx context.Context, req ledger.VoidRequest) (ledger.Hold, error)
	ExpireHolds(ctx context.Context, now time.Time) (int, error)
	RefundTransfer(ctx context.Context, req ledger.RefundRequest) (ledger.Refund, ledger.Transfer, error)
	GetTransfer(ctx context.Context, id string) (ledger.Transfer, bool, error)
	GetTransferByKey(ctx context.Context, key string) (ledger.Transfer, bool, error)
	GetHold(ctx context.Context, id string) (ledger.Hold, bool, error)
	GetHoldByKey(ctx context.Context, key string) (ledger.Hold, bool, error)
	AccountEntries(ctx context.Context, accountID string) ([]ledger.Entry, error)
	Reconcile(ctx context.Context) (ReconciliationReport, error)
}
