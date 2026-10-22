package fraud

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Rohan1857/fintech-platform/pkg/money"
)

// Keep real in-memory reads and health checks, but reproduce a dependency
// that becomes read-only between evaluation and recording the attempt.
type readOnlyTracker struct{ *MemVelocityTracker }

func (r *readOnlyTracker) RecordTransaction(context.Context, string, string, money.Money, time.Time) error {
	return errors.New("velocity store is read-only")
}

func TestFraudEngine_RecordFailureFailsClosed(t *testing.T) {
	for _, decision := range []Decision{DecisionReject, DecisionReview} {
		t.Run(string(decision), func(t *testing.T) {
			auditor := NewMemAuditStore()
			cfg := DefaultConfig()
			cfg.FallbackDecisionOnError = decision
			engine := NewEngine(cfg, &readOnlyTracker{NewMemVelocityTracker()}, auditor)
			result, err := engine.Evaluate(context.Background(), EvaluationRequest{
				AccountID: "buyer", BeneficiaryID: "seller", Amount: 100, Currency: "INR",
			})
			if err != nil {
				t.Fatal(err)
			}
			if result.Decision != decision || result.RiskScore != 100 {
				t.Fatalf("write failure silently approved: %+v", result)
			}
			persisted, err := auditor.GetEvaluation(context.Background(), result.EvaluationID)
			if err != nil || persisted.Decision != decision {
				t.Fatalf("fail-closed assessment not recorded: %+v err=%v", persisted, err)
			}
		})
	}
}
