package memstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/messi/fintech-platform/internal/ledger"
	"github.com/messi/fintech-platform/pkg/money"
)

func TestMemStore_BasicTransfer(t *testing.T) {
	ctx := context.Background()
	s := New()

	// Seed source with 10,000 paise (100 INR), destination with 0
	_ = s.CreateAccount(ctx, ledger.Account{ID: "acct_1", Currency: money.CurrencyINR, Balance: 10000})
	_ = s.CreateAccount(ctx, ledger.Account{ID: "acct_2", Currency: money.CurrencyINR, Balance: 0})

	tx, err := s.ApplyTransfer(ctx, ledger.TransferRequest{
		IdempotencyKey: "idem_1",
		FromAccountID:  "acct_1",
		ToAccountID:    "acct_2",
		Amount:         3000, // 30 INR
		Currency:       money.CurrencyINR,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if tx.Amount != 3000 || tx.Status != ledger.StatusPosted {
		t.Fatalf("invalid tx: %+v", tx)
	}

	a1, _ := s.GetAccount(ctx, "acct_1")
	a2, _ := s.GetAccount(ctx, "acct_2")
	if a1.Balance != 7000 || a2.Balance != 3000 {
		t.Fatalf("balances wrong: a1=%d, a2=%d", a1.Balance, a2.Balance)
	}

	// Idempotent retry returns identical transaction without moving funds again
	retryTx, err := s.ApplyTransfer(ctx, ledger.TransferRequest{
		IdempotencyKey: "idem_1",
		FromAccountID:  "acct_1",
		ToAccountID:    "acct_2",
		Amount:         3000,
		Currency:       money.CurrencyINR,
	})
	if err != nil {
		t.Fatalf("retry error: %v", err)
	}
	if retryTx.ID != tx.ID {
		t.Fatalf("retry produced different tx id: %s != %s", retryTx.ID, tx.ID)
	}

	a1, _ = s.GetAccount(ctx, "acct_1")
	if a1.Balance != 7000 {
		t.Fatalf("duplicate deduction occurred: %d", a1.Balance)
	}

	// Conflict rejection
	_, err = s.ApplyTransfer(ctx, ledger.TransferRequest{
		IdempotencyKey: "idem_1",
		FromAccountID:  "acct_1",
		ToAccountID:    "acct_2",
		Amount:         4000, // Different amount
		Currency:       money.CurrencyINR,
	})
	if !errors.Is(err, ledger.ErrIdempotencyConflict) {
		t.Fatalf("expected ErrIdempotencyConflict, got %v", err)
	}
}

func TestMemStore_AggregatedLegValidationFix(t *testing.T) {
	ctx := context.Background()
	s := New()

	// Account 1 has available balance of 6,000 paise (60 INR)
	_ = s.CreateAccount(ctx, ledger.Account{ID: "acct_deb", Currency: money.CurrencyINR, Balance: 6000})
	_ = s.CreateAccount(ctx, ledger.Account{ID: "acct_cred1", Currency: money.CurrencyINR, Balance: 0})
	_ = s.CreateAccount(ctx, ledger.Account{ID: "acct_cred2", Currency: money.CurrencyINR, Balance: 0})

	// Bug case in upstream: if acct_deb has two legs of -5000 each (total -10000),
	// per-leg validation passed each 5000 <= 6000!
	// With aggregated validation, total debit is -10000 > 6000, must be rejected!
	postReq := ledger.PostRequest{
		IdempotencyKey: "idem_multileg_1",
		Currency:       money.CurrencyINR,
		Postings: []ledger.Posting{
			{AccountID: "acct_deb", Amount: -5000},
			{AccountID: "acct_cred1", Amount: 5000},
			{AccountID: "acct_deb", Amount: -5000},
			{AccountID: "acct_cred2", Amount: 5000},
		},
	}

	_, err := s.ApplyPosting(ctx, postReq)
	if !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Fatalf("expected ErrInsufficientFunds from aggregated validation, got %v", err)
	}

	deb, _ := s.GetAccount(ctx, "acct_deb")
	if deb.Balance != 6000 {
		t.Fatalf("balance was corrupted: %d", deb.Balance)
	}
}

func TestMemStore_AuthorizationHoldAndCapture(t *testing.T) {
	ctx := context.Background()
	s := New()

	_ = s.CreateAccount(ctx, ledger.Account{ID: "buyer", Currency: money.CurrencyINR, Balance: 10000})
	_ = s.CreateAccount(ctx, ledger.Account{ID: "merchant", Currency: money.CurrencyINR, Balance: 0})

	// Authorize 5,000 paise (50 INR)
	hold, err := s.AuthorizeHold(ctx, ledger.AuthorizeRequest{
		IdempotencyKey: "hold_1",
		FromAccountID:  "buyer",
		ToAccountID:    "merchant",
		Amount:         5000,
		Currency:       money.CurrencyINR,
		ExpiresIn:      time.Hour,
	})
	if err != nil {
		t.Fatalf("authorize error: %v", err)
	}
	if hold.Status != ledger.HoldActive {
		t.Fatalf("expected active hold, got %s", hold.Status)
	}

	b, _ := s.GetAccount(ctx, "buyer")
	if b.Balance != 10000 || b.Held != 5000 || b.Available() != 5000 {
		t.Fatalf("unexpected balance post-auth: balance=%d, held=%d, avail=%d", b.Balance, b.Held, b.Available())
	}

	// Second transfer attempting to spend 6,000 must fail due to available balance check
	_, err = s.ApplyTransfer(ctx, ledger.TransferRequest{
		IdempotencyKey: "spend_too_much",
		FromAccountID:  "buyer",
		ToAccountID:    "merchant",
		Amount:         6000,
		Currency:       money.CurrencyINR,
	})
	if !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Fatalf("expected ErrInsufficientFunds, got %v", err)
	}

	// Capture partial amount: 4,000 paise (remaining 1,000 unheld)
	capTx, err := s.CaptureHold(ctx, ledger.CaptureRequest{
		IdempotencyKey: "cap_1",
		HoldID:         hold.ID,
		Amount:         4000,
	})
	if err != nil {
		t.Fatalf("capture error: %v", err)
	}
	if capTx.Amount != 4000 || capTx.Status != ledger.StatusPosted {
		t.Fatalf("invalid capture tx: %+v", capTx)
	}

	b, _ = s.GetAccount(ctx, "buyer")
	m, _ := s.GetAccount(ctx, "merchant")
	if b.Balance != 6000 || b.Held != 0 || b.Available() != 6000 {
		t.Fatalf("buyer balance incorrect: b=%d, held=%d", b.Balance, b.Held)
	}
	if m.Balance != 4000 {
		t.Fatalf("merchant balance incorrect: m=%d", m.Balance)
	}
}

func TestMemStore_Refund(t *testing.T) {
	ctx := context.Background()
	s := New()

	_ = s.CreateAccount(ctx, ledger.Account{ID: "sender", Currency: money.CurrencyINR, Balance: 10000})
	_ = s.CreateAccount(ctx, ledger.Account{ID: "receiver", Currency: money.CurrencyINR, Balance: 0})

	origTx, err := s.ApplyTransfer(ctx, ledger.TransferRequest{
		IdempotencyKey: "pay_1",
		FromAccountID:  "sender",
		ToAccountID:    "receiver",
		Amount:         5000,
		Currency:       money.CurrencyINR,
	})
	if err != nil {
		t.Fatalf("initial transfer error: %v", err)
	}

	// Partial refund of 2,000 paise
	ref1, refTx1, err := s.RefundTransfer(ctx, ledger.RefundRequest{
		IdempotencyKey: "ref_1",
		TransferID:     origTx.ID,
		Amount:         2000,
		Reason:         "Customer returned partial item",
	})
	if err != nil {
		t.Fatalf("refund 1 error: %v", err)
	}
	if ref1.Amount != 2000 || refTx1.Amount != 2000 {
		t.Fatalf("refund amounts wrong: ref=%d, tx=%d", ref1.Amount, refTx1.Amount)
	}

	sAcct, _ := s.GetAccount(ctx, "sender")
	rAcct, _ := s.GetAccount(ctx, "receiver")
	if sAcct.Balance != 7000 || rAcct.Balance != 3000 {
		t.Fatalf("post refund 1 balances wrong: sender=%d, receiver=%d", sAcct.Balance, rAcct.Balance)
	}

	// Refund remaining 3,000 paise
	_, _, err = s.RefundTransfer(ctx, ledger.RefundRequest{
		IdempotencyKey: "ref_2",
		TransferID:     origTx.ID,
		Amount:         3000,
		Reason:         "Remaining refund",
	})
	if err != nil {
		t.Fatalf("refund 2 error: %v", err)
	}

	sAcct, _ = s.GetAccount(ctx, "sender")
	rAcct, _ = s.GetAccount(ctx, "receiver")
	if sAcct.Balance != 10000 || rAcct.Balance != 0 {
		t.Fatalf("final refund balances wrong: sender=%d, receiver=%d", sAcct.Balance, rAcct.Balance)
	}

	// Attempting another refund must fail
	_, _, err = s.RefundTransfer(ctx, ledger.RefundRequest{
		IdempotencyKey: "ref_3",
		TransferID:     origTx.ID,
		Amount:         100,
	})
	if !errors.Is(err, ledger.ErrRefundExceedsTransfer) {
		t.Fatalf("expected ErrRefundExceedsTransfer, got %v", err)
	}
}

func TestMemStore_Reconciliation(t *testing.T) {
	ctx := context.Background()
	s := New()

	_ = s.CreateAccount(ctx, ledger.Account{ID: "reserve", Currency: money.CurrencyINR, Balance: 0})
	_ = s.CreateAccount(ctx, ledger.Account{ID: "a", Currency: money.CurrencyINR, Balance: 0})
	_ = s.CreateAccount(ctx, ledger.Account{ID: "b", Currency: money.CurrencyINR, Balance: 0})

	// Fund A through an issuance transfer: reserve is debited (goes to -50000), a is credited (+50000)
	// In double-entry, reserve equity accounts reflect issuance. But since our Account prevents negative balance
	// unless explicitly permitted or seeded, let's test balanced transfers between accounts:
	// We allow reserve account or we create accounts with 0 and transfer between them.
	// Let's create reserve with 50000, but in Reconcile, check that transfers balance!
	_, err := s.ApplyPosting(ctx, ledger.PostRequest{
		IdempotencyKey: "seed_a",
		Currency:       money.CurrencyINR,
		Postings: []ledger.Posting{
			{AccountID: "reserve", Amount: 0}, // test zero posting rejected
		},
	})
	if !errors.Is(err, ledger.ErrTooFewPostings) && !errors.Is(err, ledger.ErrZeroPosting) {
		t.Fatalf("expected ErrTooFewPostings or ErrZeroPosting, got %v", err)
	}

	// Fund A and B via balanced multi-leg
	_ = s.CreateAccount(ctx, ledger.Account{ID: "bank", Currency: money.CurrencyINR, Balance: 100000})
	_, err = s.ApplyTransfer(ctx, ledger.TransferRequest{
		IdempotencyKey: "bank_to_a",
		FromAccountID:  "bank",
		ToAccountID:    "a",
		Amount:         40000,
		Currency:       money.CurrencyINR,
	})
	if err != nil {
		t.Fatalf("transfer err: %v", err)
	}

	_, err = s.ApplyTransfer(ctx, ledger.TransferRequest{
		IdempotencyKey: "a_to_b",
		FromAccountID:  "a",
		ToAccountID:    "b",
		Amount:         15000,
		Currency:       money.CurrencyINR,
	})
	if err != nil {
		t.Fatalf("transfer err: %v", err)
	}

	rep, err := s.Reconcile(ctx)
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}
	if len(rep.UnbalancedTransfers) > 0 {
		t.Fatalf("unbalanced transfers found: %+v", rep.UnbalancedTransfers)
	}
	if rep.TotalEntriesSum != 0 {
		t.Fatalf("entries sum not zero: %d", rep.TotalEntriesSum)
	}
}
