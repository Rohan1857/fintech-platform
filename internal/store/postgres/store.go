package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/messi/fintech-platform/internal/ledger"
	"github.com/messi/fintech-platform/internal/store"
	"github.com/messi/fintech-platform/pkg/money"
)

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Store is a PostgreSQL implementation of store.Store using pgx/v5 connection pool.
type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) inTx(ctx context.Context, fn func(pgx.Tx) error) (err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func isCheckViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23514"
}

func (s *Store) CreateAccount(ctx context.Context, a ledger.Account) error {
	if a.ID == "" {
		return fmt.Errorf("account id required")
	}
	if a.Currency == "" {
		a.Currency = money.CurrencyINR
	}

	initBalance := a.Balance

	return s.inTx(ctx, func(tx pgx.Tx) error {
		var existingCur string
		var existingBal, existingHeld int64
		err := tx.QueryRow(ctx, `SELECT currency, balance, held FROM accounts WHERE id = $1`, a.ID).
			Scan(&existingCur, &existingBal, &existingHeld)
		if err == nil {
			if existingCur != a.Currency || existingBal != int64(initBalance) || existingHeld != int64(a.Held) {
				return ledger.ErrAccountExists
			}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		now := time.Now().UTC()
		_, err = tx.Exec(ctx, `
			INSERT INTO accounts (id, currency, balance, held, created_at, updated_at)
			VALUES ($1, $2, 0, 0, $3, $3)`,
			a.ID, a.Currency, now,
		)
		if err != nil {
			if isCheckViolation(err) {
				return ledger.ErrInsufficientFunds
			}
			return err
		}

		if initBalance > 0 {
			reserveID := "system_reserve_" + a.Currency
			_, err = tx.Exec(ctx, `
				INSERT INTO accounts (id, currency, balance, held, created_at, updated_at)
				VALUES ($1, $2, 0, 0, $3, $3)
				ON CONFLICT (id) DO NOTHING`,
				reserveID, a.Currency, now,
			)
			if err != nil {
				return err
			}

			tID := newID()
			entries := []ledger.Entry{
				{ID: newID(), TransferID: tID, AccountID: reserveID, Amount: -initBalance, CreatedAt: now},
				{ID: newID(), TransferID: tID, AccountID: a.ID, Amount: initBalance, CreatedAt: now},
			}

			amtVal := int64(initBalance)
			_, err = tx.Exec(ctx, `
				INSERT INTO transfers (id, idempotency_key, from_account_id, to_account_id, amount, currency, status, type, created_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
				tID, "opening_"+a.ID, reserveID, a.ID, amtVal, a.Currency, string(ledger.StatusPosted), string(ledger.TransferTypeStandard), now,
			)
			if err != nil {
				return err
			}

			for _, e := range entries {
				_, err = tx.Exec(ctx, `
					INSERT INTO entries (id, transfer_id, account_id, amount, created_at)
					VALUES ($1, $2, $3, $4, $5)`,
					e.ID, e.TransferID, e.AccountID, int64(e.Amount), e.CreatedAt,
				)
				if err != nil {
					return err
				}
			}

			_, err = tx.Exec(ctx, `
				UPDATE accounts SET balance = balance - $1, updated_at = $2 WHERE id = $3`,
				amtVal, now, reserveID,
			)
			if err != nil {
				return err
			}

			_, err = tx.Exec(ctx, `
				UPDATE accounts SET balance = balance + $1, updated_at = $2 WHERE id = $3`,
				amtVal, now, a.ID,
			)
			if err != nil {
				return err
			}
		}

		if a.Held > 0 {
			_, err = tx.Exec(ctx, `
				UPDATE accounts SET held = $1, updated_at = $2 WHERE id = $3`,
				int64(a.Held), now, a.ID,
			)
			if err != nil {
				if isCheckViolation(err) {
					return ledger.ErrInsufficientFunds
				}
				return err
			}
		}

		return nil
	})
}

func (s *Store) GetAccount(ctx context.Context, id string) (ledger.Account, error) {
	var a ledger.Account
	var balance, held int64
	err := s.pool.QueryRow(ctx, `
		SELECT id, currency, balance, held, created_at, updated_at
		FROM accounts WHERE id = $1`, id).Scan(
		&a.ID, &a.Currency, &balance, &held, &a.CreatedAt, &a.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ledger.Account{}, ledger.ErrAccountNotFound
		}
		return ledger.Account{}, err
	}
	a.Balance = money.Money(balance)
	a.Held = money.Money(held)
	return a, nil
}

// lockAccounts locks accounts in deterministic sorted order to prevent deadlocks.
func lockAccounts(ctx context.Context, tx pgx.Tx, ids []string) (map[string]ledger.Account, error) {
	uniqueMap := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		uniqueMap[id] = struct{}{}
	}
	sorted := make([]string, 0, len(uniqueMap))
	for id := range uniqueMap {
		sorted = append(sorted, id)
	}
	sort.Strings(sorted)

	accounts := make(map[string]ledger.Account, len(sorted))
	for _, id := range sorted {
		var a ledger.Account
		var balance, held int64
		err := tx.QueryRow(ctx, `
			SELECT id, currency, balance, held, created_at, updated_at
			FROM accounts WHERE id = $1 FOR UPDATE`, id).Scan(
			&a.ID, &a.Currency, &balance, &held, &a.CreatedAt, &a.UpdatedAt,
		)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, ledger.ErrAccountNotFound
			}
			return nil, err
		}
		a.Balance = money.Money(balance)
		a.Held = money.Money(held)
		accounts[id] = a
	}
	return accounts, nil
}

func (s *Store) ApplyTransfer(ctx context.Context, req ledger.TransferRequest) (ledger.Transfer, error) {
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

func (s *Store) ApplyPosting(ctx context.Context, req ledger.PostRequest) (ledger.Transfer, error) {
	if req.IdempotencyKey == "" {
		return ledger.Transfer{}, ledger.ErrMissingIdempotencyKey
	}

	// 1. Aggregate postings by account upfront: solves per-leg validation vulnerability
	aggregated, err := ledger.AggregatePostings(req.Postings)
	if err != nil {
		return ledger.Transfer{}, err
	}

	var result ledger.Transfer
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		// 2. Lock accounts in deterministic sorted order
		acctIDs := make([]string, 0, len(aggregated))
		for id := range aggregated {
			acctIDs = append(acctIDs, id)
		}
		accounts, err := lockAccounts(ctx, tx, acctIDs)
		if err != nil {
			return err
		}

		// 3. Check idempotency inside locked transaction
		existing, ok, err := s.readTransferByKeyTx(ctx, tx, req.IdempotencyKey)
		if err != nil {
			return err
		}
		if ok {
			if !ledger.MatchesPost(existing, req) {
				return ledger.ErrIdempotencyConflict
			}
			result = existing
			return nil
		}

		// 4. Validate currencies
		currency := req.Currency
		for acctID := range aggregated {
			a := accounts[acctID]
			if currency == "" {
				currency = a.Currency
			}
			if a.Currency != currency {
				return ledger.ErrCurrencyMismatch
			}
		}

		// 5. Invariant check: Check aggregated balance impacts against available balance
		for acctID, netDelta := range aggregated {
			a := accounts[acctID]
			newBalance := a.Balance + netDelta
			if newBalance < 0 || newBalance < a.Held {
				return ledger.ErrInsufficientFunds
			}
		}

		now := time.Now().UTC()
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
			return err
		}

		var fromID, toID *string
		var amt *int64
		if len(req.Postings) == 2 {
			d, c := req.Postings[0], req.Postings[1]
			if d.Amount > 0 {
				d, c = c, d
			}
			fromID = &d.AccountID
			toID = &c.AccountID
			val := int64(c.Amount)
			amt = &val
		}

		// Insert transfer row
		_, err = tx.Exec(ctx, `
			INSERT INTO transfers (id, idempotency_key, from_account_id, to_account_id, amount, currency, status, type, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			tID, req.IdempotencyKey, fromID, toID, amt, currency, string(ledger.StatusPosted), string(ledger.TransferTypeStandard), now,
		)
		if err != nil {
			if isUniqueViolation(err) {
				return ledger.ErrIdempotencyConflict
			}
			return err
		}

		// Insert entry rows
		for _, e := range entries {
			_, err = tx.Exec(ctx, `
				INSERT INTO entries (id, transfer_id, account_id, amount, created_at)
				VALUES ($1, $2, $3, $4, $5)`,
				e.ID, e.TransferID, e.AccountID, int64(e.Amount), e.CreatedAt,
			)
			if err != nil {
				return err
			}
		}

		// Update account balances atomically
		for acctID, netDelta := range aggregated {
			_, err = tx.Exec(ctx, `
				UPDATE accounts SET balance = balance + $1, updated_at = $2
				WHERE id = $3`,
				int64(netDelta), now, acctID,
			)
			if err != nil {
				if isCheckViolation(err) {
					return ledger.ErrInsufficientFunds
				}
				return err
			}
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
		if fromID != nil {
			t.FromAccountID = *fromID
			t.ToAccountID = *toID
			t.Amount = money.Money(*amt)
		}

		// Write to outbox atomically within same transaction
		outboxPayload, _ := json.Marshal(map[string]any{
			"transfer_id":     t.ID,
			"idempotency_key": t.IdempotencyKey,
			"from_account_id": t.FromAccountID,
			"to_account_id":   t.ToAccountID,
			"amount":          t.Amount,
			"currency":        t.Currency,
			"status":          t.Status,
			"created_at":      t.CreatedAt,
		})
		_, err = tx.Exec(ctx, `
			INSERT INTO outbox_events (id, event_type, aggregate_type, aggregate_id, payload, created_at)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			newID(), "payment.completed", "transfer", t.ID, outboxPayload, now,
		)
		if err != nil {
			return err
		}

		result = t
		return nil
	})

	return result, err
}

func (s *Store) AuthorizeHold(ctx context.Context, req ledger.AuthorizeRequest) (ledger.Hold, error) {
	if req.IdempotencyKey == "" {
		return ledger.Hold{}, ledger.ErrMissingIdempotencyKey
	}
	if req.Amount <= 0 {
		return ledger.Hold{}, ledger.ErrInvalidAmount
	}
	if req.FromAccountID == req.ToAccountID {
		return ledger.Hold{}, ledger.ErrSameAccount
	}

	var result ledger.Hold
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		accounts, err := lockAccounts(ctx, tx, []string{req.FromAccountID, req.ToAccountID})
		if err != nil {
			return err
		}

		existing, ok, err := s.readHoldByKeyTx(ctx, tx, req.IdempotencyKey)
		if err != nil {
			return err
		}
		if ok {
			if !ledger.MatchesHold(existing, req) {
				return ledger.ErrIdempotencyConflict
			}
			result = existing
			return nil
		}

		fromAcct := accounts[req.FromAccountID]
		toAcct := accounts[req.ToAccountID]
		if fromAcct.Currency != toAcct.Currency {
			return ledger.ErrCurrencyMismatch
		}
		if req.Currency != "" && req.Currency != fromAcct.Currency {
			return ledger.ErrCurrencyMismatch
		}
		if fromAcct.Available() < req.Amount {
			return ledger.ErrInsufficientFunds
		}

		now := time.Now().UTC()
		var expiresAt *time.Time
		if req.ExpiresIn > 0 {
			exp := now.Add(req.ExpiresIn)
			expiresAt = &exp
		}

		hID := newID()
		_, err = tx.Exec(ctx, `
			INSERT INTO holds (id, idempotency_key, from_account_id, to_account_id, amount, captured, status, created_at, expires_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			hID, req.IdempotencyKey, req.FromAccountID, req.ToAccountID, int64(req.Amount), 0, string(ledger.HoldActive), now, expiresAt,
		)
		if err != nil {
			return err
		}

		_, err = tx.Exec(ctx, `
			UPDATE accounts SET held = held + $1, updated_at = $2
			WHERE id = $3`,
			int64(req.Amount), now, req.FromAccountID,
		)
		if err != nil {
			if isCheckViolation(err) {
				return ledger.ErrInsufficientFunds
			}
			return err
		}

		h := ledger.Hold{
			ID:             hID,
			IdempotencyKey: req.IdempotencyKey,
			FromAccountID:  req.FromAccountID,
			ToAccountID:    req.ToAccountID,
			Amount:         req.Amount,
			Captured:       0,
			Status:         ledger.HoldActive,
			CreatedAt:      now,
		}
		if expiresAt != nil {
			h.ExpiresAt = *expiresAt
		}

		outboxPayload, _ := json.Marshal(map[string]any{
			"hold_id":         h.ID,
			"idempotency_key": h.IdempotencyKey,
			"from_account_id": h.FromAccountID,
			"to_account_id":   h.ToAccountID,
			"amount":          h.Amount,
			"currency":        fromAcct.Currency,
			"status":          h.Status,
			"created_at":      h.CreatedAt,
			"expires_at":      h.ExpiresAt,
		})
		_, err = tx.Exec(ctx, `
			INSERT INTO outbox_events (id, event_type, aggregate_type, aggregate_id, payload, created_at)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			newID(), "payment.created", "hold", h.ID, outboxPayload, now,
		)
		if err != nil {
			return err
		}

		result = h
		return nil
	})

	return result, err
}

func (s *Store) CaptureHold(ctx context.Context, req ledger.CaptureRequest) (ledger.Transfer, error) {
	if req.IdempotencyKey == "" {
		return ledger.Transfer{}, ledger.ErrMissingIdempotencyKey
	}
	if req.Amount <= 0 {
		return ledger.Transfer{}, ledger.ErrInvalidAmount
	}

	var result ledger.Transfer
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		existing, ok, err := s.readTransferByKeyTx(ctx, tx, req.IdempotencyKey)
		if err != nil {
			return err
		}
		if ok {
			if existing.Type == ledger.TransferTypeHoldCapture && existing.Amount == req.Amount {
				result = existing
				return nil
			}
			return ledger.ErrIdempotencyConflict
		}

		// Lock hold row
		var h ledger.Hold
		var amount, captured int64
		var expiresAt *time.Time
		err = tx.QueryRow(ctx, `
			SELECT id, idempotency_key, from_account_id, to_account_id, amount, captured, status, created_at, expires_at
			FROM holds WHERE id = $1 FOR UPDATE`, req.HoldID).Scan(
			&h.ID, &h.IdempotencyKey, &h.FromAccountID, &h.ToAccountID, &amount, &captured, &h.Status, &h.CreatedAt, &expiresAt,
		)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ledger.ErrHoldNotFound
			}
			return err
		}
		h.Amount = money.Money(amount)
		h.Captured = money.Money(captured)
		if expiresAt != nil {
			h.ExpiresAt = *expiresAt
		}

		if h.Status != ledger.HoldActive {
			return ledger.ErrHoldNotActive
		}

		now := time.Now().UTC()
		if !h.ExpiresAt.IsZero() && now.After(h.ExpiresAt) {
			_, _ = tx.Exec(ctx, `UPDATE holds SET status = 'expired' WHERE id = $1`, h.ID)
			_, _ = tx.Exec(ctx, `UPDATE accounts SET held = held - $1, updated_at = $2 WHERE id = $3`,
				int64(h.Amount), now, h.FromAccountID)
			return ledger.ErrHoldExpired
		}

		if req.Amount > h.Amount {
			return ledger.ErrCaptureExceedsHold
		}

		accounts, err := lockAccounts(ctx, tx, []string{h.FromAccountID, h.ToAccountID})
		if err != nil {
			return err
		}
		fromAcct := accounts[h.FromAccountID]

		tID := newID()
		entries := []ledger.Entry{
			{ID: newID(), TransferID: tID, AccountID: h.FromAccountID, Amount: -req.Amount, CreatedAt: now},
			{ID: newID(), TransferID: tID, AccountID: h.ToAccountID, Amount: req.Amount, CreatedAt: now},
		}

		amtVal := int64(req.Amount)
		_, err = tx.Exec(ctx, `
			INSERT INTO transfers (id, idempotency_key, from_account_id, to_account_id, amount, currency, status, type, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			tID, req.IdempotencyKey, h.FromAccountID, h.ToAccountID, amtVal, fromAcct.Currency, string(ledger.StatusPosted), string(ledger.TransferTypeHoldCapture), now,
		)
		if err != nil {
			return err
		}

		for _, e := range entries {
			_, err = tx.Exec(ctx, `
				INSERT INTO entries (id, transfer_id, account_id, amount, created_at)
				VALUES ($1, $2, $3, $4, $5)`,
				e.ID, e.TransferID, e.AccountID, int64(e.Amount), e.CreatedAt,
			)
			if err != nil {
				return err
			}
		}

		// Update buyer: release full hold amount, deduct captured balance
		_, err = tx.Exec(ctx, `
			UPDATE accounts SET balance = balance - $1, held = held - $2, updated_at = $3
			WHERE id = $4`,
			int64(req.Amount), int64(h.Amount), now, h.FromAccountID,
		)
		if err != nil {
			return err
		}

		// Update merchant: credit captured balance
		_, err = tx.Exec(ctx, `
			UPDATE accounts SET balance = balance + $1, updated_at = $2
			WHERE id = $3`,
			int64(req.Amount), now, h.ToAccountID,
		)
		if err != nil {
			return err
		}

		// Update hold record
		_, err = tx.Exec(ctx, `
			UPDATE holds SET status = 'captured', captured = $1, capture_transfer_id = $2
			WHERE id = $3`,
			amtVal, tID, h.ID,
		)
		if err != nil {
			return err
		}

		t := ledger.Transfer{
			ID:             tID,
			IdempotencyKey: req.IdempotencyKey,
			FromAccountID:  h.FromAccountID,
			ToAccountID:    h.ToAccountID,
			Amount:         req.Amount,
			Currency:       fromAcct.Currency,
			Status:         ledger.StatusPosted,
			Type:           ledger.TransferTypeHoldCapture,
			CreatedAt:      now,
			Entries:        entries,
		}

		outboxPayload, _ := json.Marshal(map[string]any{
			"transfer_id":     t.ID,
			"hold_id":         h.ID,
			"idempotency_key": t.IdempotencyKey,
			"amount":          t.Amount,
			"currency":        t.Currency,
			"status":          t.Status,
			"created_at":      t.CreatedAt,
		})
		_, err = tx.Exec(ctx, `
			INSERT INTO outbox_events (id, event_type, aggregate_type, aggregate_id, payload, created_at)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			newID(), "payment.completed", "transfer", t.ID, outboxPayload, now,
		)
		if err != nil {
			return err
		}

		result = t
		return nil
	})

	return result, err
}

func (s *Store) VoidHold(ctx context.Context, req ledger.VoidRequest) (ledger.Hold, error) {
	if req.IdempotencyKey == "" {
		return ledger.Hold{}, ledger.ErrMissingIdempotencyKey
	}

	var result ledger.Hold
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var h ledger.Hold
		var amount, captured int64
		err := tx.QueryRow(ctx, `
			SELECT id, idempotency_key, from_account_id, to_account_id, amount, captured, status, created_at
			FROM holds WHERE id = $1 FOR UPDATE`, req.HoldID).Scan(
			&h.ID, &h.IdempotencyKey, &h.FromAccountID, &h.ToAccountID, &amount, &captured, &h.Status, &h.CreatedAt,
		)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ledger.ErrHoldNotFound
			}
			return err
		}
		h.Amount = money.Money(amount)
		h.Captured = money.Money(captured)

		if h.Status != ledger.HoldActive {
			return ledger.ErrHoldNotActive
		}

		now := time.Now().UTC()
		_, err = tx.Exec(ctx, `
			UPDATE accounts SET held = held - $1, updated_at = $2
			WHERE id = $3`,
			int64(h.Amount), now, h.FromAccountID,
		)
		if err != nil {
			return err
		}

		_, err = tx.Exec(ctx, `UPDATE holds SET status = 'voided' WHERE id = $1`, h.ID)
		if err != nil {
			return err
		}
		h.Status = ledger.HoldVoided

		outboxPayload, _ := json.Marshal(map[string]any{
			"hold_id":    h.ID,
			"status":     h.Status,
			"voided_at":  now,
			"amount":     h.Amount,
			"account_id": h.FromAccountID,
		})
		_, err = tx.Exec(ctx, `
			INSERT INTO outbox_events (id, event_type, aggregate_type, aggregate_id, payload, created_at)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			newID(), "payment.failed", "hold", h.ID, outboxPayload, now,
		)
		if err != nil {
			return err
		}

		result = h
		return nil
	})

	return result, err
}

func (s *Store) ExpireHolds(ctx context.Context, now time.Time) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		WITH expired AS (
			UPDATE holds
			SET status = 'expired'
			WHERE status = 'active' AND expires_at IS NOT NULL AND expires_at <= $1
			RETURNING from_account_id, amount
		)
		UPDATE accounts
		SET held = accounts.held - expired.amount, updated_at = $1
		FROM expired
		WHERE accounts.id = expired.from_account_id`,
		now,
	)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (s *Store) RefundTransfer(ctx context.Context, req ledger.RefundRequest) (ledger.Refund, ledger.Transfer, error) {
	if req.IdempotencyKey == "" {
		return ledger.Refund{}, ledger.Transfer{}, ledger.ErrMissingIdempotencyKey
	}

	var refundRes ledger.Refund
	var transferRes ledger.Transfer

	err := s.inTx(ctx, func(tx pgx.Tx) error {
		// Idempotency check on refund
		existingRef, ok, err := s.readRefundByKeyTx(ctx, tx, req.IdempotencyKey)
		if err != nil {
			return err
		}
		if ok {
			if !ledger.MatchesRefund(existingRef, req) {
				return ledger.ErrIdempotencyConflict
			}
			refundRes = existingRef
			existingTx, _, rerr := s.readTransferByIDTx(ctx, tx, existingRef.RefundTransferID)
			if rerr != nil {
				return rerr
			}
			transferRes = existingTx
			return nil
		}

		// Read and lock original transfer
		orig, ok, err := s.readTransferByIDTx(ctx, tx, req.TransferID)
		if err != nil {
			return err
		}
		if !ok {
			return ledger.ErrTransferNotFound
		}

		var totalAlreadyRefunded int64
		err = tx.QueryRow(ctx, `
			SELECT COALESCE(SUM(amount), 0) FROM refunds WHERE original_transfer_id = $1`, orig.ID).Scan(&totalAlreadyRefunded)
		if err != nil {
			return err
		}

		if orig.Status == ledger.StatusRefunded || money.Money(totalAlreadyRefunded) >= orig.Amount {
			return ledger.ErrRefundExceedsTransfer
		}
		if orig.Status != ledger.StatusPosted {
			return ledger.ErrTransferNotRefundable
		}

		refundAmt := req.Amount
		if refundAmt == 0 {
			refundAmt = orig.Amount
		}
		if refundAmt <= 0 {
			return ledger.ErrInvalidAmount
		}

		if money.Money(totalAlreadyRefunded)+refundAmt > orig.Amount {
			return ledger.ErrRefundExceedsTransfer
		}

		// Lock recipient (who will be debited) and sender (who will be credited)
		accounts, err := lockAccounts(ctx, tx, []string{orig.ToAccountID, orig.FromAccountID})
		if err != nil {
			return err
		}

		recipientAcct := accounts[orig.ToAccountID]
		if recipientAcct.Available() < refundAmt {
			return ledger.ErrInsufficientFunds
		}

		now := time.Now().UTC()
		refTxID := newID()
		entries := []ledger.Entry{
			{ID: newID(), TransferID: refTxID, AccountID: orig.ToAccountID, Amount: -refundAmt, CreatedAt: now},
			{ID: newID(), TransferID: refTxID, AccountID: orig.FromAccountID, Amount: refundAmt, CreatedAt: now},
		}

		amtVal := int64(refundAmt)
		_, err = tx.Exec(ctx, `
			INSERT INTO transfers (id, idempotency_key, from_account_id, to_account_id, amount, currency, status, type, parent_transfer_id, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			refTxID, "ref_tx_"+req.IdempotencyKey, orig.ToAccountID, orig.FromAccountID, amtVal, orig.Currency, string(ledger.StatusPosted), string(ledger.TransferTypeRefund), orig.ID, now,
		)
		if err != nil {
			return err
		}

		for _, e := range entries {
			_, err = tx.Exec(ctx, `
				INSERT INTO entries (id, transfer_id, account_id, amount, created_at)
				VALUES ($1, $2, $3, $4, $5)`,
				e.ID, e.TransferID, e.AccountID, int64(e.Amount), e.CreatedAt,
			)
			if err != nil {
				return err
			}
		}

		// Reverse balances
		_, err = tx.Exec(ctx, `UPDATE accounts SET balance = balance - $1, updated_at = $2 WHERE id = $3`,
			amtVal, now, orig.ToAccountID)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE accounts SET balance = balance + $1, updated_at = $2 WHERE id = $3`,
			amtVal, now, orig.FromAccountID)
		if err != nil {
			return err
		}

		refID := newID()
		_, err = tx.Exec(ctx, `
			INSERT INTO refunds (id, idempotency_key, original_transfer_id, refund_transfer_id, amount, reason, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			refID, req.IdempotencyKey, orig.ID, refTxID, amtVal, req.Reason, now,
		)
		if err != nil {
			return err
		}

		if money.Money(totalAlreadyRefunded)+refundAmt == orig.Amount {
			_, _ = tx.Exec(ctx, `UPDATE transfers SET status = 'refunded' WHERE id = $1`, orig.ID)
		}

		refundRes = ledger.Refund{
			ID:                 refID,
			IdempotencyKey:     req.IdempotencyKey,
			OriginalTransferID: orig.ID,
			RefundTransferID:   refTxID,
			Amount:             refundAmt,
			Reason:             req.Reason,
			CreatedAt:          now,
		}

		transferRes = ledger.Transfer{
			ID:               refTxID,
			IdempotencyKey:   "ref_tx_" + req.IdempotencyKey,
			FromAccountID:    orig.ToAccountID,
			ToAccountID:      orig.FromAccountID,
			Amount:           refundAmt,
			Currency:         orig.Currency,
			Status:           ledger.StatusPosted,
			Type:             ledger.TransferTypeRefund,
			ParentTransferID: orig.ID,
			CreatedAt:        now,
			Entries:          entries,
		}

		outboxPayload, _ := json.Marshal(map[string]any{
			"refund_id":            refID,
			"original_transfer_id": orig.ID,
			"refund_transfer_id":   refTxID,
			"amount":               refundAmt,
			"currency":             orig.Currency,
			"created_at":           now,
		})
		_, err = tx.Exec(ctx, `
			INSERT INTO outbox_events (id, event_type, aggregate_type, aggregate_id, payload, created_at)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			newID(), "payment.refunded", "refund", refID, outboxPayload, now,
		)
		if err != nil {
			return err
		}

		return nil
	})

	return refundRes, transferRes, err
}

func (s *Store) GetTransfer(ctx context.Context, id string) (ledger.Transfer, bool, error) {
	return s.readTransferByIDTx(ctx, s.pool, id)
}

func (s *Store) GetTransferByKey(ctx context.Context, key string) (ledger.Transfer, bool, error) {
	return s.readTransferByKeyTx(ctx, s.pool, key)
}

func (s *Store) GetHold(ctx context.Context, id string) (ledger.Hold, bool, error) {
	var h ledger.Hold
	var amount, captured int64
	var expiresAt *time.Time
	var captureTransferID *string
	err := s.pool.QueryRow(ctx, `
		SELECT id, idempotency_key, from_account_id, to_account_id, amount, captured, status, capture_transfer_id, created_at, expires_at
		FROM holds WHERE id = $1`, id).Scan(
		&h.ID, &h.IdempotencyKey, &h.FromAccountID, &h.ToAccountID, &amount, &captured, &h.Status, &captureTransferID, &h.CreatedAt, &expiresAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ledger.Hold{}, false, nil
		}
		return ledger.Hold{}, false, err
	}
	h.Amount = money.Money(amount)
	h.Captured = money.Money(captured)
	if captureTransferID != nil {
		h.CaptureTransferID = *captureTransferID
	}
	if expiresAt != nil {
		h.ExpiresAt = *expiresAt
	}
	return h, true, nil
}

func (s *Store) GetHoldByKey(ctx context.Context, key string) (ledger.Hold, bool, error) {
	return s.readHoldByKeyTx(ctx, s.pool, key)
}

func (s *Store) AccountEntries(ctx context.Context, accountID string) ([]ledger.Entry, error) {
	if _, err := s.GetAccount(ctx, accountID); err != nil {
		return nil, err
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id, transfer_id, account_id, amount, created_at
		FROM entries WHERE account_id = $1
		ORDER BY created_at ASC, id ASC`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []ledger.Entry
	for rows.Next() {
		var e ledger.Entry
		var amt int64
		if err := rows.Scan(&e.ID, &e.TransferID, &e.AccountID, &amt, &e.CreatedAt); err != nil {
			return nil, err
		}
		e.Amount = money.Money(amt)
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

func (s *Store) Reconcile(ctx context.Context) (store.ReconciliationReport, error) {
	var report store.ReconciliationReport
	report.IsClean = true

	// Count accounts
	_ = s.pool.QueryRow(ctx, `SELECT COUNT(*), COALESCE(SUM(balance), 0) FROM accounts`).
		Scan(&report.TotalAccounts, (*int64)(&report.TotalBalanceMinor))

	// Count transfers
	_ = s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM transfers`).
		Scan(&report.TotalTransfers)

	// Count entries and sum
	_ = s.pool.QueryRow(ctx, `SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM entries`).
		Scan(&report.TotalEntries, (*int64)(&report.TotalEntriesSum))

	if report.TotalEntriesSum != 0 {
		report.IsClean = false
	}

	// 1. Find any transfer where sum of entries != 0
	rows, err := s.pool.Query(ctx, `
		SELECT transfer_id
		FROM entries
		GROUP BY transfer_id
		HAVING SUM(amount) != 0 OR COUNT(*) < 2`)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var tid string
			if err := rows.Scan(&tid); err == nil {
				report.UnbalancedTransfers = append(report.UnbalancedTransfers, tid)
				report.IsClean = false
			}
		}
	}

	// 2. Find any account where balance != sum of entries
	acctRows, err := s.pool.Query(ctx, `
		SELECT a.id
		FROM accounts a
		LEFT JOIN entries e ON a.id = e.account_id
		GROUP BY a.id, a.balance
		HAVING a.balance != COALESCE(SUM(e.amount), 0)`)
	if err == nil {
		defer acctRows.Close()
		for acctRows.Next() {
			var aid string
			if err := acctRows.Scan(&aid); err == nil {
				report.MismatchedAccounts = append(report.MismatchedAccounts, aid)
				report.IsClean = false
			}
		}
	}

	return report, nil
}

// Helpers
type dbQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func (s *Store) readTransferByKeyTx(ctx context.Context, q dbQuerier, key string) (ledger.Transfer, bool, error) {
	var t ledger.Transfer
	var fromID, toID, parentID *string
	var amt *int64
	err := q.QueryRow(ctx, `
		SELECT id, idempotency_key, from_account_id, to_account_id, amount, currency, status, type, parent_transfer_id, created_at
		FROM transfers WHERE idempotency_key = $1`, key).Scan(
		&t.ID, &t.IdempotencyKey, &fromID, &toID, &amt, &t.Currency, &t.Status, &t.Type, &parentID, &t.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ledger.Transfer{}, false, nil
		}
		return ledger.Transfer{}, false, err
	}
	if fromID != nil {
		t.FromAccountID = *fromID
	}
	if toID != nil {
		t.ToAccountID = *toID
	}
	if amt != nil {
		t.Amount = money.Money(*amt)
	}
	if parentID != nil {
		t.ParentTransferID = *parentID
	}

	// Read entries
	rows, err := q.Query(ctx, `SELECT id, transfer_id, account_id, amount, created_at FROM entries WHERE transfer_id = $1 ORDER BY id`, t.ID)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var e ledger.Entry
			var eAmt int64
			_ = rows.Scan(&e.ID, &e.TransferID, &e.AccountID, &eAmt, &e.CreatedAt)
			e.Amount = money.Money(eAmt)
			t.Entries = append(t.Entries, e)
		}
	}

	return t, true, nil
}

func (s *Store) readTransferByIDTx(ctx context.Context, q dbQuerier, id string) (ledger.Transfer, bool, error) {
	var t ledger.Transfer
	var fromID, toID, parentID *string
	var amt *int64
	err := q.QueryRow(ctx, `
		SELECT id, idempotency_key, from_account_id, to_account_id, amount, currency, status, type, parent_transfer_id, created_at
		FROM transfers WHERE id = $1`, id).Scan(
		&t.ID, &t.IdempotencyKey, &fromID, &toID, &amt, &t.Currency, &t.Status, &t.Type, &parentID, &t.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ledger.Transfer{}, false, nil
		}
		return ledger.Transfer{}, false, err
	}
	if fromID != nil {
		t.FromAccountID = *fromID
	}
	if toID != nil {
		t.ToAccountID = *toID
	}
	if amt != nil {
		t.Amount = money.Money(*amt)
	}
	if parentID != nil {
		t.ParentTransferID = *parentID
	}

	rows, err := q.Query(ctx, `SELECT id, transfer_id, account_id, amount, created_at FROM entries WHERE transfer_id = $1 ORDER BY id`, t.ID)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var e ledger.Entry
			var eAmt int64
			_ = rows.Scan(&e.ID, &e.TransferID, &e.AccountID, &eAmt, &e.CreatedAt)
			e.Amount = money.Money(eAmt)
			t.Entries = append(t.Entries, e)
		}
	}

	return t, true, nil
}

func (s *Store) readHoldByKeyTx(ctx context.Context, q dbQuerier, key string) (ledger.Hold, bool, error) {
	var h ledger.Hold
	var amount, captured int64
	var expiresAt *time.Time
	var capTransferID *string
	err := q.QueryRow(ctx, `
		SELECT id, idempotency_key, from_account_id, to_account_id, amount, captured, status, capture_transfer_id, created_at, expires_at
		FROM holds WHERE idempotency_key = $1`, key).Scan(
		&h.ID, &h.IdempotencyKey, &h.FromAccountID, &h.ToAccountID, &amount, &captured, &h.Status, &capTransferID, &h.CreatedAt, &expiresAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ledger.Hold{}, false, nil
		}
		return ledger.Hold{}, false, err
	}
	h.Amount = money.Money(amount)
	h.Captured = money.Money(captured)
	if capTransferID != nil {
		h.CaptureTransferID = *capTransferID
	}
	if expiresAt != nil {
		h.ExpiresAt = *expiresAt
	}
	return h, true, nil
}

func (s *Store) readRefundByKeyTx(ctx context.Context, q dbQuerier, key string) (ledger.Refund, bool, error) {
	var r ledger.Refund
	var amount int64
	var reason *string
	err := q.QueryRow(ctx, `
		SELECT id, idempotency_key, original_transfer_id, refund_transfer_id, amount, reason, created_at
		FROM refunds WHERE idempotency_key = $1`, key).Scan(
		&r.ID, &r.IdempotencyKey, &r.OriginalTransferID, &r.RefundTransferID, &amount, &reason, &r.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ledger.Refund{}, false, nil
		}
		return ledger.Refund{}, false, err
	}
	r.Amount = money.Money(amount)
	if reason != nil {
		r.Reason = *reason
	}
	return r, true, nil
}
