package memstore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sort"
	"sync"
	"time"

	"github.com/Rohan1857/fintech-platform/internal/ledger"
	"github.com/Rohan1857/fintech-platform/internal/store"
	"github.com/Rohan1857/fintech-platform/pkg/money"
)

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// MemStore is a concurrent-safe, in-memory Store implementation.
type MemStore struct {
	mu           sync.Mutex
	accounts     map[string]ledger.Account
	transfers    map[string]ledger.Transfer
	holds        map[string]ledger.Hold
	refunds      map[string]ledger.Refund
	entries      map[string][]ledger.Entry // accountID -> entries
	allEntries   map[string]ledger.Entry   // entryID -> entry
	byIdemKey    map[string]string         // idemKey -> transferID
	holdByIdem   map[string]string         // idemKey -> holdID
	refundByIdem map[string]string         // idemKey -> refundID
	refundSums   map[string]money.Money    // originalTransferID -> total refunded
	clock        func() time.Time
}

func New() *MemStore {
	return &MemStore{
		accounts:     make(map[string]ledger.Account),
		transfers:    make(map[string]ledger.Transfer),
		holds:        make(map[string]ledger.Hold),
		refunds:      make(map[string]ledger.Refund),
		entries:      make(map[string][]ledger.Entry),
		allEntries:   make(map[string]ledger.Entry),
		byIdemKey:    make(map[string]string),
		holdByIdem:   make(map[string]string),
		refundByIdem: make(map[string]string),
		refundSums:   make(map[string]money.Money),
		clock:        time.Now,
	}
}

func (s *MemStore) SetClock(clock func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clock = clock
}

func (s *MemStore) now() time.Time {
	if s.clock != nil {
		return s.clock().UTC()
	}
	return time.Now().UTC()
}

func (s *MemStore) CreateAccount(_ context.Context, a ledger.Account) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ledger.ValidateNewAccount(a); err != nil {
		return err
	}
	if a.Currency == "" {
		a.Currency = money.CurrencyINR
	}

	existing, exists := s.accounts[a.ID]
	if exists {
		if existing.Currency == a.Currency && existing.Balance == a.Balance && existing.Held == a.Held {
			return nil
		}
		return ledger.ErrAccountExists
	}

	initBalance := a.Balance
	a.Balance = 0
	a.Held = 0

	t := s.now()
	a.CreatedAt = t
	a.UpdatedAt = t
	s.accounts[a.ID] = a

	if initBalance > 0 {
		reserveID := "system_reserve_" + a.Currency
		if _, ok := s.accounts[reserveID]; !ok {
			s.accounts[reserveID] = ledger.Account{
				ID:        reserveID,
				Currency:  a.Currency,
				Balance:   0,
				Held:      0,
				CreatedAt: t,
				UpdatedAt: t,
			}
		}

		tID := newID()
		entries := []ledger.Entry{
			{ID: newID(), TransferID: tID, AccountID: reserveID, Amount: -initBalance, CreatedAt: t},
			{ID: newID(), TransferID: tID, AccountID: a.ID, Amount: initBalance, CreatedAt: t},
		}
		openTx := ledger.Transfer{
			ID:             tID,
			IdempotencyKey: "opening_" + a.ID,
			FromAccountID:  reserveID,
			ToAccountID:    a.ID,
			Amount:         initBalance,
			Currency:       a.Currency,
			Status:         ledger.StatusPosted,
			Type:           ledger.TransferTypeStandard,
			CreatedAt:      t,
			Entries:        entries,
		}

		resAcct := s.accounts[reserveID]
		resAcct.Balance -= initBalance
		s.accounts[reserveID] = resAcct

		a.Balance += initBalance
		s.accounts[a.ID] = a

		s.transfers[tID] = openTx
		s.entries[reserveID] = append(s.entries[reserveID], entries[0])
		s.entries[a.ID] = append(s.entries[a.ID], entries[1])
		s.allEntries[entries[0].ID] = entries[0]
		s.allEntries[entries[1].ID] = entries[1]
	}

	return nil
}

func (s *MemStore) GetAccount(_ context.Context, id string) (ledger.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	a, ok := s.accounts[id]
	if !ok {
		return ledger.Account{}, ledger.ErrAccountNotFound
	}
	return a, nil
}

func (s *MemStore) ApplyTransfer(ctx context.Context, req ledger.TransferRequest) (ledger.Transfer, error) {
	if req.IdempotencyKey == "" {
		return ledger.Transfer{}, ledger.ErrMissingIdempotencyKey
	}
	if req.Amount <= 0 {
		return ledger.Transfer{}, ledger.ErrInvalidAmount
	}
	if req.FromAccountID == req.ToAccountID {
		return ledger.Transfer{}, ledger.ErrSameAccount
	}

	postReq := ledger.PostRequest{
		IdempotencyKey: req.IdempotencyKey,
		Currency:       req.Currency,
		Postings: []ledger.Posting{
			{AccountID: req.FromAccountID, Amount: -req.Amount},
			{AccountID: req.ToAccountID, Amount: req.Amount},
		},
	}
	return s.ApplyPosting(ctx, postReq)
}

func (s *MemStore) ApplyPosting(_ context.Context, req ledger.PostRequest) (ledger.Transfer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.IdempotencyKey == "" {
		return ledger.Transfer{}, ledger.ErrMissingIdempotencyKey
	}

	// Idempotency check
	if id, ok := s.byIdemKey[req.IdempotencyKey]; ok {
		existing := s.transfers[id]
		if !ledger.MatchesPost(existing, req) {
			return ledger.Transfer{}, ledger.ErrIdempotencyConflict
		}
		return existing, nil
	}

	// Aggregate postings by account (CRITICAL: prevents per-leg validation vulnerability)
	aggregated, err := ledger.AggregatePostings(req.Postings)
	if err != nil {
		return ledger.Transfer{}, err
	}

	// Ensure all accounts exist and verify currencies
	currency := req.Currency
	for acctID := range aggregated {
		a, ok := s.accounts[acctID]
		if !ok {
			return ledger.Transfer{}, ledger.ErrAccountNotFound
		}
		if currency == "" {
			currency = a.Currency
		}
		if a.Currency != currency {
			return ledger.Transfer{}, ledger.ErrCurrencyMismatch
		}
	}

	// Invariant check: Check aggregated balance impacts against available balances
	for acctID, netDelta := range aggregated {
		a := s.accounts[acctID]
		newBalance := a.Balance + netDelta
		if newBalance < 0 || newBalance < a.Held {
			return ledger.Transfer{}, ledger.ErrInsufficientFunds
		}
	}

	// Construct entries and transfer
	now := s.now()
	tID := newID()
	entries := make([]ledger.Entry, len(req.Postings))
	for i, p := range req.Postings {
		entries[i] = ledger.Entry{
			ID:         newID(),
			TransferID: tID,
			AccountID:  p.AccountID,
			Amount:     p.Amount,
			CreatedAt:  now,
		}
	}

	if err := ledger.AssertBalanced(entries); err != nil {
		return ledger.Transfer{}, err
	}

	t := ledger.Transfer{
		ID:             tID,
		IdempotencyKey: req.IdempotencyKey,
		Currency:       currency,
		Status:         ledger.StatusPosted,
		Type:           ledger.TransferTypeStandard,
		CreatedAt:      now,
		Entries:        entries,
	}

	if len(req.Postings) == 2 {
		d, c := req.Postings[0], req.Postings[1]
		if d.Amount > 0 {
			d, c = c, d
		}
		t.FromAccountID = d.AccountID
		t.ToAccountID = c.AccountID
		t.Amount = c.Amount
	}

	// Commit balances and entries atomically
	for acctID, netDelta := range aggregated {
		a := s.accounts[acctID]
		a.Balance += netDelta
		a.UpdatedAt = now
		s.accounts[acctID] = a
	}

	for _, e := range entries {
		s.entries[e.AccountID] = append(s.entries[e.AccountID], e)
		s.allEntries[e.ID] = e
	}

	s.transfers[tID] = t
	s.byIdemKey[req.IdempotencyKey] = tID
	return t, nil
}

func (s *MemStore) AuthorizeHold(_ context.Context, req ledger.AuthorizeRequest) (ledger.Hold, error) {
	for _, id := range []string{req.FromAccountID, req.ToAccountID} {
		if err := ledger.ValidatePublicAccountID(id); err != nil {
			return ledger.Hold{}, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.IdempotencyKey == "" {
		return ledger.Hold{}, ledger.ErrMissingIdempotencyKey
	}
	if req.Amount <= 0 {
		return ledger.Hold{}, ledger.ErrInvalidAmount
	}
	if req.FromAccountID == req.ToAccountID {
		return ledger.Hold{}, ledger.ErrSameAccount
	}

	// Idempotency check
	if id, ok := s.holdByIdem[req.IdempotencyKey]; ok {
		existing := s.holds[id]
		if !ledger.MatchesHold(existing, req) {
			return ledger.Hold{}, ledger.ErrIdempotencyConflict
		}
		return existing, nil
	}

	fromAcct, ok := s.accounts[req.FromAccountID]
	if !ok {
		return ledger.Hold{}, ledger.ErrAccountNotFound
	}
	toAcct, ok := s.accounts[req.ToAccountID]
	if !ok {
		return ledger.Hold{}, ledger.ErrAccountNotFound
	}

	if fromAcct.Currency != toAcct.Currency {
		return ledger.Hold{}, ledger.ErrCurrencyMismatch
	}
	if req.Currency != "" && req.Currency != fromAcct.Currency {
		return ledger.Hold{}, ledger.ErrCurrencyMismatch
	}

	if fromAcct.Available() < req.Amount {
		return ledger.Hold{}, ledger.ErrInsufficientFunds
	}

	now := s.now()
	var expiresAt time.Time
	if req.ExpiresIn > 0 {
		expiresAt = now.Add(req.ExpiresIn)
	}

	hID := newID()
	h := ledger.Hold{
		ID:             hID,
		IdempotencyKey: req.IdempotencyKey,
		FromAccountID:  req.FromAccountID,
		ToAccountID:    req.ToAccountID,
		Amount:         req.Amount,
		Captured:       0,
		Status:         ledger.HoldActive,
		CreatedAt:      now,
		ExpiresAt:      expiresAt,
	}

	fromAcct.Held += req.Amount
	fromAcct.UpdatedAt = now
	s.accounts[req.FromAccountID] = fromAcct

	s.holds[hID] = h
	s.holdByIdem[req.IdempotencyKey] = hID
	return h, nil
}

func (s *MemStore) CaptureHold(_ context.Context, req ledger.CaptureRequest) (ledger.Transfer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.IdempotencyKey == "" {
		return ledger.Transfer{}, ledger.ErrMissingIdempotencyKey
	}
	if req.Amount <= 0 {
		return ledger.Transfer{}, ledger.ErrInvalidAmount
	}

	// Idempotency check on transfer
	if id, ok := s.byIdemKey[req.IdempotencyKey]; ok {
		existing := s.transfers[id]
		if existing.Type == ledger.TransferTypeHoldCapture && existing.Amount == req.Amount && s.holds[req.HoldID].CaptureTransferID == existing.ID {
			return existing, nil
		}
		return ledger.Transfer{}, ledger.ErrIdempotencyConflict
	}

	h, ok := s.holds[req.HoldID]
	if !ok {
		return ledger.Transfer{}, ledger.ErrHoldNotFound
	}
	for _, id := range []string{h.FromAccountID, h.ToAccountID} {
		if err := ledger.ValidatePublicAccountID(id); err != nil {
			return ledger.Transfer{}, err
		}
	}
	if h.Status != ledger.HoldActive {
		return ledger.Transfer{}, ledger.ErrHoldNotActive
	}
	now := s.now()
	if !h.ExpiresAt.IsZero() && !now.Before(h.ExpiresAt) {
		h.Status = ledger.HoldExpired
		fromAcct := s.accounts[h.FromAccountID]
		fromAcct.Held -= h.Amount
		fromAcct.UpdatedAt = now
		s.accounts[h.FromAccountID] = fromAcct
		s.holds[h.ID] = h
		return ledger.Transfer{}, ledger.ErrHoldExpired
	}

	if req.Amount > h.Amount {
		return ledger.Transfer{}, ledger.ErrCaptureExceedsHold
	}

	fromAcct := s.accounts[h.FromAccountID]
	toAcct := s.accounts[h.ToAccountID]

	// Release hold and deduct settled funds
	fromAcct.Held -= h.Amount
	fromAcct.Balance -= req.Amount
	toAcct.Balance += req.Amount
	fromAcct.UpdatedAt = now
	toAcct.UpdatedAt = now
	s.accounts[fromAcct.ID] = fromAcct
	s.accounts[toAcct.ID] = toAcct

	tID := newID()
	entries := []ledger.Entry{
		{ID: newID(), TransferID: tID, AccountID: fromAcct.ID, Amount: -req.Amount, CreatedAt: now},
		{ID: newID(), TransferID: tID, AccountID: toAcct.ID, Amount: req.Amount, CreatedAt: now},
	}

	t := ledger.Transfer{
		ID:             tID,
		IdempotencyKey: req.IdempotencyKey,
		FromAccountID:  fromAcct.ID,
		ToAccountID:    toAcct.ID,
		Amount:         req.Amount,
		Currency:       fromAcct.Currency,
		Status:         ledger.StatusPosted,
		Type:           ledger.TransferTypeHoldCapture,
		CreatedAt:      now,
		Entries:        entries,
	}

	h.Status = ledger.HoldCaptured
	h.Captured = req.Amount
	h.CaptureTransferID = tID
	s.holds[h.ID] = h

	s.entries[fromAcct.ID] = append(s.entries[fromAcct.ID], entries[0])
	s.entries[toAcct.ID] = append(s.entries[toAcct.ID], entries[1])
	s.allEntries[entries[0].ID] = entries[0]
	s.allEntries[entries[1].ID] = entries[1]

	s.transfers[tID] = t
	s.byIdemKey[req.IdempotencyKey] = tID
	return t, nil
}

func (s *MemStore) VoidHold(_ context.Context, req ledger.VoidRequest) (ledger.Hold, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	h, ok := s.holds[req.HoldID]
	if !ok {
		return ledger.Hold{}, ledger.ErrHoldNotFound
	}
	if h.Status != ledger.HoldActive {
		return ledger.Hold{}, ledger.ErrHoldNotActive
	}

	now := s.now()
	h.Status = ledger.HoldVoided
	fromAcct := s.accounts[h.FromAccountID]
	fromAcct.Held -= h.Amount
	fromAcct.UpdatedAt = now
	s.accounts[h.FromAccountID] = fromAcct
	s.holds[h.ID] = h
	return h, nil
}

func (s *MemStore) ExpireHolds(_ context.Context, now time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0
	for id, h := range s.holds {
		if h.Status == ledger.HoldActive && !h.ExpiresAt.IsZero() && !now.Before(h.ExpiresAt) {
			h.Status = ledger.HoldExpired
			fromAcct := s.accounts[h.FromAccountID]
			fromAcct.Held -= h.Amount
			fromAcct.UpdatedAt = now
			s.accounts[h.FromAccountID] = fromAcct
			s.holds[id] = h
			count++
		}
	}
	return count, nil
}

func (s *MemStore) RefundTransfer(_ context.Context, req ledger.RefundRequest) (ledger.Refund, ledger.Transfer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.IdempotencyKey == "" {
		return ledger.Refund{}, ledger.Transfer{}, ledger.ErrMissingIdempotencyKey
	}

	// Idempotency check
	if refID, ok := s.refundByIdem[req.IdempotencyKey]; ok {
		existingRef := s.refunds[refID]
		if !ledger.MatchesRefund(existingRef, req) {
			return ledger.Refund{}, ledger.Transfer{}, ledger.ErrIdempotencyConflict
		}
		existingTransfer := s.transfers[existingRef.RefundTransferID]
		return existingRef, existingTransfer, nil
	}

	orig, ok := s.transfers[req.TransferID]
	if !ok {
		return ledger.Refund{}, ledger.Transfer{}, ledger.ErrTransferNotFound
	}
	for _, id := range []string{orig.FromAccountID, orig.ToAccountID} {
		if err := ledger.ValidatePublicAccountID(id); err != nil {
			return ledger.Refund{}, ledger.Transfer{}, err
		}
	}
	if orig.Status == ledger.StatusRefunded || s.refundSums[orig.ID] >= orig.Amount {
		return ledger.Refund{}, ledger.Transfer{}, ledger.ErrRefundExceedsTransfer
	}
	if orig.Status != ledger.StatusPosted {
		return ledger.Refund{}, ledger.Transfer{}, ledger.ErrTransferNotRefundable
	}

	refundAmt := req.Amount
	if refundAmt == 0 {
		refundAmt = orig.Amount
	}
	if refundAmt <= 0 {
		return ledger.Refund{}, ledger.Transfer{}, ledger.ErrInvalidAmount
	}

	alreadyRefunded := s.refundSums[orig.ID]
	if alreadyRefunded+refundAmt > orig.Amount {
		return ledger.Refund{}, ledger.Transfer{}, ledger.ErrRefundExceedsTransfer
	}

	// Check that recipient has sufficient funds to be debited
	recipientAcct, ok := s.accounts[orig.ToAccountID]
	if !ok {
		return ledger.Refund{}, ledger.Transfer{}, ledger.ErrAccountNotFound
	}
	senderAcct, ok := s.accounts[orig.FromAccountID]
	if !ok {
		return ledger.Refund{}, ledger.Transfer{}, ledger.ErrAccountNotFound
	}

	if recipientAcct.Available() < refundAmt {
		return ledger.Refund{}, ledger.Transfer{}, ledger.ErrInsufficientFunds
	}

	now := s.now()
	// Apply reverse balance move
	recipientAcct.Balance -= refundAmt
	senderAcct.Balance += refundAmt
	recipientAcct.UpdatedAt = now
	senderAcct.UpdatedAt = now
	s.accounts[recipientAcct.ID] = recipientAcct
	s.accounts[senderAcct.ID] = senderAcct

	refTransferID := newID()
	entries := []ledger.Entry{
		{ID: newID(), TransferID: refTransferID, AccountID: recipientAcct.ID, Amount: -refundAmt, CreatedAt: now},
		{ID: newID(), TransferID: refTransferID, AccountID: senderAcct.ID, Amount: refundAmt, CreatedAt: now},
	}

	refundTransfer := ledger.Transfer{
		ID:               refTransferID,
		IdempotencyKey:   "ref_tx_" + req.IdempotencyKey,
		FromAccountID:    recipientAcct.ID,
		ToAccountID:      senderAcct.ID,
		Amount:           refundAmt,
		Currency:         orig.Currency,
		Status:           ledger.StatusPosted,
		Type:             ledger.TransferTypeRefund,
		ParentTransferID: orig.ID,
		CreatedAt:        now,
		Entries:          entries,
	}

	refID := newID()
	refundRec := ledger.Refund{
		ID:                 refID,
		IdempotencyKey:     req.IdempotencyKey,
		OriginalTransferID: orig.ID,
		RefundTransferID:   refTransferID,
		Amount:             refundAmt,
		Reason:             req.Reason,
		CreatedAt:          now,
	}

	s.refundSums[orig.ID] += refundAmt
	if s.refundSums[orig.ID] == orig.Amount {
		orig.Status = ledger.StatusRefunded
		s.transfers[orig.ID] = orig
	}

	s.entries[recipientAcct.ID] = append(s.entries[recipientAcct.ID], entries[0])
	s.entries[senderAcct.ID] = append(s.entries[senderAcct.ID], entries[1])
	s.allEntries[entries[0].ID] = entries[0]
	s.allEntries[entries[1].ID] = entries[1]

	s.transfers[refTransferID] = refundTransfer
	s.refunds[refID] = refundRec
	s.refundByIdem[req.IdempotencyKey] = refID
	return refundRec, refundTransfer, nil
}

func (s *MemStore) GetTransfer(_ context.Context, id string) (ledger.Transfer, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.transfers[id]
	return t, ok, nil
}

func (s *MemStore) GetTransferByKey(_ context.Context, key string) (ledger.Transfer, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.byIdemKey[key]
	if !ok {
		return ledger.Transfer{}, false, nil
	}
	return s.transfers[id], true, nil
}

func (s *MemStore) GetHold(_ context.Context, id string) (ledger.Hold, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.holds[id]
	return h, ok, nil
}

func (s *MemStore) GetHoldByKey(_ context.Context, key string) (ledger.Hold, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.holdByIdem[key]
	if !ok {
		return ledger.Hold{}, false, nil
	}
	return s.holds[id], true, nil
}

func (s *MemStore) AccountEntries(_ context.Context, accountID string) ([]ledger.Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.accounts[accountID]; !ok {
		return nil, ledger.ErrAccountNotFound
	}

	items := s.entries[accountID]
	out := make([]ledger.Entry, len(items))
	copy(out, items)
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out, nil
}

func (s *MemStore) Reconcile(_ context.Context) (store.ReconciliationReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	report := store.ReconciliationReport{
		TotalAccounts:  len(s.accounts),
		TotalTransfers: len(s.transfers),
		TotalEntries:   len(s.allEntries),
		IsClean:        true,
	}

	// 1. Verify every transfer sums to 0
	for _, t := range s.transfers {
		var sum money.Money
		for _, e := range t.Entries {
			sum += e.Amount
		}
		if sum != 0 || len(t.Entries) < 2 {
			report.UnbalancedTransfers = append(report.UnbalancedTransfers, t.ID)
			report.IsClean = false
		}
	}

	// 2. Verify account balances match entry sums
	accountEntrySums := make(map[string]money.Money, len(s.accounts))
	var totalEntriesSum money.Money
	for _, e := range s.allEntries {
		accountEntrySums[e.AccountID] += e.Amount
		totalEntriesSum += e.Amount
	}
	report.TotalEntriesSum = totalEntriesSum
	if totalEntriesSum != 0 {
		report.IsClean = false
	}

	var totalAccountBalance money.Money
	for id, a := range s.accounts {
		totalAccountBalance += a.Balance
		if a.Balance != accountEntrySums[id] {
			report.MismatchedAccounts = append(report.MismatchedAccounts, id)
			report.IsClean = false
		}
	}
	report.TotalBalanceMinor = totalAccountBalance

	return report, nil
}
