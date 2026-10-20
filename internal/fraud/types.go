package fraud

import (
	"context"
	"time"

	"github.com/messi/fintech-platform/pkg/money"
)

// Decision defines the outcome of a fraud evaluation.
type Decision string

const (
	DecisionApprove Decision = "APPROVE"
	DecisionReview  Decision = "REVIEW"
	DecisionReject  Decision = "REJECT"
)

// EvaluationRequest supplies transaction attributes for real-time risk assessment.
type EvaluationRequest struct {
	IdempotencyKey string      `json:"idempotency_key"`
	PaymentID      string      `json:"payment_id,omitempty"`
	AccountID      string      `json:"account_id"`
	BeneficiaryID  string      `json:"beneficiary_id"`
	Amount         money.Money `json:"amount"`
	Currency       string      `json:"currency"`
	DeviceID       string      `json:"device_id,omitempty"`
	IPAddress      string      `json:"ip_address,omitempty"`
	Timestamp      time.Time   `json:"timestamp"`
}

// RuleHit details a single rule triggered during evaluation.
type RuleHit struct {
	RuleID      string   `json:"rule_id"`
	RuleName    string   `json:"rule_name"`
	Score       float64  `json:"score"`
	Reason      string   `json:"reason"`
	HardOutcome Decision `json:"hard_outcome,omitempty"`
}

// EvaluationResult is the actionable output from the fraud engine.
type EvaluationResult struct {
	EvaluationID   string    `json:"evaluation_id"`
	Decision       Decision  `json:"decision"`
	RiskScore      float64   `json:"risk_score"`
	Reasons        []string  `json:"reasons"`
	TriggeredRules []RuleHit `json:"triggered_rules"`
	EvaluatedAt    time.Time `json:"evaluated_at"`
}

// Config configures fraud engine thresholds and failure policy.
type Config struct {
	ReviewThreshold             float64       `json:"review_threshold"`               // e.g. 50.0
	RejectThreshold             float64       `json:"reject_threshold"`               // e.g. 80.0
	HighValueThresholdMinor     money.Money   `json:"high_value_threshold_minor"`     // e.g. 5,000,000 paise (50,000 INR)
	CriticalValueThresholdMinor money.Money   `json:"critical_value_threshold_minor"` // e.g. 20,000,000 paise (200,000 INR)
	VelocityWindowShort         time.Duration `json:"velocity_window_short"`          // e.g. 1 minute
	VelocityWindowMedium        time.Duration `json:"velocity_window_medium"`         // e.g. 5 minutes
	VelocityMaxCountShort       int64         `json:"velocity_max_count_short"`       // e.g. 3 transactions
	VelocityMaxCountMedium      int64         `json:"velocity_max_count_medium"`      // e.g. 5 transactions
	VelocityMaxAmountMedium     money.Money   `json:"velocity_max_amount_medium"`     // e.g. 20,000,000 paise
	BeneficiaryMaxCountShort    int64         `json:"beneficiary_max_count_short"`    // e.g. 3 rapid payments
	FailClosed                  bool          `json:"fail_closed"`                    // True: Reject/Review on error. Never silently approve.
	FallbackDecisionOnError     Decision      `json:"fallback_decision_on_error"`     // REJECT or REVIEW
}

// DefaultConfig returns balanced fintech fraud parameters.
func DefaultConfig() Config {
	return Config{
		ReviewThreshold:             50.0,
		RejectThreshold:             80.0,
		HighValueThresholdMinor:     5000000,  // 50,000 INR
		CriticalValueThresholdMinor: 25000000, // 250,000 INR
		VelocityWindowShort:         1 * time.Minute,
		VelocityWindowMedium:        5 * time.Minute,
		VelocityMaxCountShort:       4,
		VelocityMaxCountMedium:      8,
		VelocityMaxAmountMedium:     20000000, // 200,000 INR
		BeneficiaryMaxCountShort:    3,
		FailClosed:                  true,
		FallbackDecisionOnError:     DecisionReject,
	}
}

// VelocityTracker records and queries transaction rates using sliding windows.
type VelocityTracker interface {
	RecordTransaction(ctx context.Context, accountID, beneficiaryID string, amount money.Money, t time.Time) error
	GetVelocity(ctx context.Context, accountID string, window time.Duration) (count int64, sum money.Money, err error)
	GetBeneficiaryFrequency(ctx context.Context, accountID, beneficiaryID string, window time.Duration) (count int64, err error)
	IsHealthy(ctx context.Context) bool
}
