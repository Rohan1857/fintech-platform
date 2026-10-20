package ledger

import (
	"fmt"

	"github.com/messi/fintech-platform/pkg/money"
)

// AssertBalanced verifies the fundamental double-entry invariant:
// 1. Must have at least two legs.
// 2. Sum of all signed amounts must equal exactly 0.
func AssertBalanced(entries []Entry) error {
	if len(entries) < 2 {
		return ErrTooFewPostings
	}
	var sum money.Money
	for _, e := range entries {
		sum += e.Amount
	}
	if sum != 0 {
		return fmt.Errorf("%w: sum=%d", ErrUnbalancedPostings, sum)
	}
	return nil
}

// AggregatePostings sums all posting amounts grouped by account ID.
// This resolves the per-leg validation vulnerability by aggregating net balance impact.
func AggregatePostings(postings []Posting) (map[string]money.Money, error) {
	if len(postings) < 2 {
		return nil, ErrTooFewPostings
	}
	aggregated := make(map[string]money.Money, len(postings))
	var total money.Money
	for _, p := range postings {
		if p.Amount == 0 {
			return nil, ErrZeroPosting
		}
		if p.AccountID == "" {
			return nil, ErrAccountNotFound
		}
		aggregated[p.AccountID] += p.Amount
		total += p.Amount
	}
	if total != 0 {
		return nil, fmt.Errorf("%w: sum=%d", ErrUnbalancedPostings, total)
	}
	return aggregated, nil
}

// MatchesPost checks if an existing transfer strictly matches the idempotency payload.
func MatchesPost(existing Transfer, req PostRequest) bool {
	if req.Currency != "" && existing.Currency != req.Currency {
		return false
	}
	if len(existing.Entries) != len(req.Postings) {
		return false
	}
	byAccount := make(map[string]money.Money, len(existing.Entries))
	for _, e := range existing.Entries {
		byAccount[e.AccountID] += e.Amount
	}
	reqByAccount := make(map[string]money.Money, len(req.Postings))
	for _, p := range req.Postings {
		reqByAccount[p.AccountID] += p.Amount
	}
	if len(byAccount) != len(reqByAccount) {
		return false
	}
	for acct, amt := range reqByAccount {
		if byAccount[acct] != amt {
			return false
		}
	}
	return true
}

// MatchesTransfer verifies idempotent replay of a 2-legged transfer.
func MatchesTransfer(existing Transfer, req TransferRequest) bool {
	if req.Currency != "" && existing.Currency != req.Currency {
		return false
	}
	if existing.FromAccountID != req.FromAccountID || existing.ToAccountID != req.ToAccountID {
		return false
	}
	return existing.Amount == req.Amount
}

// MatchesHold verifies idempotent replay of an authorization hold request.
func MatchesHold(existing Hold, req AuthorizeRequest) bool {
	return existing.FromAccountID == req.FromAccountID &&
		existing.ToAccountID == req.ToAccountID &&
		existing.Amount == req.Amount
}

// MatchesRefund verifies idempotent replay of a refund request.
func MatchesRefund(existing Refund, req RefundRequest) bool {
	return existing.OriginalTransferID == req.TransferID &&
		(req.Amount == 0 || existing.Amount == req.Amount)
}
