package events

import (
	"encoding/json"
	"time"
)

// Standard domain event types
const (
	EventPaymentCreated   = "payment.created"
	EventPaymentCompleted = "payment.completed"
	EventPaymentFailed    = "payment.failed"
	EventPaymentRefunded  = "payment.refunded"
	EventFraudFlagged     = "fraud.flagged"
)

// Event is the standardized envelope for asynchronous distributed messaging.
type Event struct {
	ID            string          `json:"id"`
	Type          string          `json:"type"`
	CorrelationID string          `json:"correlation_id"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	Payload       json.RawMessage `json:"payload"`
	CreatedAt     time.Time       `json:"created_at"`
}

// PaymentEventPayload represents payload for payment events.
type PaymentEventPayload struct {
	TransferID     string    `json:"transfer_id,omitempty"`
	HoldID         string    `json:"hold_id,omitempty"`
	RefundID       string    `json:"refund_id,omitempty"`
	IdempotencyKey string    `json:"idempotency_key"`
	FromAccountID  string    `json:"from_account_id,omitempty"`
	ToAccountID    string    `json:"to_account_id,omitempty"`
	Amount         int64     `json:"amount"`
	Currency       string    `json:"currency"`
	Status         string    `json:"status"`
	Reason         string    `json:"reason,omitempty"`
	Timestamp      time.Time `json:"timestamp"`
}

// FraudFlaggedPayload represents payload for fraud alert events.
type FraudFlaggedPayload struct {
	EvaluationID  string    `json:"evaluation_id"`
	AccountID     string    `json:"account_id"`
	BeneficiaryID string    `json:"beneficiary_id"`
	Amount        int64     `json:"amount"`
	Currency      string    `json:"currency"`
	Score         float64   `json:"score"`
	Decision      string    `json:"decision"`
	Reasons       []string  `json:"reasons"`
	Timestamp     time.Time `json:"timestamp"`
}
