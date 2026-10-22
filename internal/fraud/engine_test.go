package fraud

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/Rohan1857/fintech-platform/pkg/money"
)

func TestFraudEngine_LowRiskApproved(t *testing.T) {
	ctx := context.Background()
	tracker := NewMemVelocityTracker()
	auditor := NewMemAuditStore()
	cfg := DefaultConfig()
	engine := NewEngine(cfg, tracker, auditor)

	res, err := engine.Evaluate(ctx, EvaluationRequest{
		IdempotencyKey: "idem_tx_1",
		AccountID:      "user_100",
		BeneficiaryID:  "merchant_999",
		Amount:         100000, // 1,000 INR (well below 50,000 INR high-value threshold)
		Currency:       money.CurrencyINR,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Decision != DecisionApprove {
		t.Fatalf("expected APPROVE, got %s (score=%f, reasons=%v)", res.Decision, res.RiskScore, res.Reasons)
	}
	if res.RiskScore != 0 {
		t.Fatalf("expected 0 risk score for benign transaction, got %f", res.RiskScore)
	}

	// Verify audit record was stored
	audit, err := auditor.GetEvaluation(ctx, res.EvaluationID)
	if err != nil {
		t.Fatalf("audit not found: %v", err)
	}
	if audit.Decision != DecisionApprove {
		t.Fatalf("audit decision mismatch: %s", audit.Decision)
	}
}

func TestFraudEngine_HighValueThresholds(t *testing.T) {
	ctx := context.Background()
	tracker := NewMemVelocityTracker()
	auditor := NewMemAuditStore()
	cfg := DefaultConfig()
	engine := NewEngine(cfg, tracker, auditor)

	// Case A: Just below high value threshold (49,999 INR = 4,999,900 paise)
	resBelow, _ := engine.Evaluate(ctx, EvaluationRequest{
		IdempotencyKey: "below_hv",
		AccountID:      "user_200",
		BeneficiaryID:  "merchant_1",
		Amount:         4999900,
		Currency:       money.CurrencyINR,
	})
	if resBelow.Decision != DecisionApprove || resBelow.RiskScore != 0 {
		t.Fatalf("expected APPROVE with score 0, got %s (score=%f)", resBelow.Decision, resBelow.RiskScore)
	}

	// Case B: At high value threshold (50,000 INR = 5,000,000 paise) -> score 40
	resAt, _ := engine.Evaluate(ctx, EvaluationRequest{
		IdempotencyKey: "at_hv",
		AccountID:      "user_200",
		BeneficiaryID:  "merchant_1",
		Amount:         5000000,
		Currency:       money.CurrencyINR,
	})
	if resAt.RiskScore < 40 {
		t.Fatalf("expected score >= 40 for high value, got %f", resAt.RiskScore)
	}

	// Case C: Critical value threshold (250,000 INR) -> hard review or reject
	resCrit, _ := engine.Evaluate(ctx, EvaluationRequest{
		IdempotencyKey: "crit_hv",
		AccountID:      "user_201",
		BeneficiaryID:  "merchant_2",
		Amount:         25000000,
		Currency:       money.CurrencyINR,
	})
	if resCrit.Decision == DecisionApprove {
		t.Fatalf("critical value transaction was approved! Expected REVIEW or REJECT, got %s", resCrit.Decision)
	}
}

func TestFraudEngine_RepeatedBeneficiaryTransfers(t *testing.T) {
	ctx := context.Background()
	tracker := NewMemVelocityTracker()
	auditor := NewMemAuditStore()
	cfg := DefaultConfig()
	cfg.BeneficiaryMaxCountShort = 3
	engine := NewEngine(cfg, tracker, auditor)

	accountID := "user_repeat"
	beneficiaryID := "suspicious_beneficiary"

	// Send 3 rapid transfers
	for i := 1; i <= 3; i++ {
		res, err := engine.Evaluate(ctx, EvaluationRequest{
			IdempotencyKey: fmt.Sprintf("bene_tx_%d", i),
			AccountID:      accountID,
			BeneficiaryID:  beneficiaryID,
			Amount:         10000, // 100 INR
			Currency:       money.CurrencyINR,
		})
		if err != nil {
			t.Fatalf("evaluation %d failed: %v", i, err)
		}
		if i < 3 && res.Decision != DecisionApprove {
			t.Fatalf("early tx %d unexpectedly flagged: %s", i, res.Decision)
		}
	}

	// 4th transfer to same beneficiary triggers rapid repeat rule
	res4, _ := engine.Evaluate(ctx, EvaluationRequest{
		IdempotencyKey: "bene_tx_4",
		AccountID:      accountID,
		BeneficiaryID:  beneficiaryID,
		Amount:         10000,
		Currency:       money.CurrencyINR,
	})

	var foundRepeatRule bool
	for _, rule := range res4.TriggeredRules {
		if rule.RuleID == "RULE-BENE-001" {
			foundRepeatRule = true
			break
		}
	}
	if !foundRepeatRule {
		t.Fatalf("expected RULE-BENE-001 to trigger for rapid repeat payments: %+v", res4.TriggeredRules)
	}
}

func TestFraudEngine_SafeFailClosedPolicy(t *testing.T) {
	ctx := context.Background()
	tracker := NewMemVelocityTracker()
	auditor := NewMemAuditStore()
	cfg := DefaultConfig()
	cfg.FailClosed = true
	cfg.FallbackDecisionOnError = DecisionReject
	engine := NewEngine(cfg, tracker, auditor)

	// Simulate outage in velocity tracker (e.g. Redis crash or network timeout)
	tracker.SetHealthy(false)

	res, err := engine.Evaluate(ctx, EvaluationRequest{
		IdempotencyKey: "fail_closed_test",
		AccountID:      "user_normal",
		BeneficiaryID:  "bene_normal",
		Amount:         500, // tiny 5 INR payment
		Currency:       money.CurrencyINR,
	})
	if err != nil {
		t.Fatalf("unexpected engine error: %v", err)
	}

	// In fail-closed mode, MUST REJECT when security checks cannot be performed!
	if res.Decision == DecisionApprove {
		t.Fatalf("CRITICAL SECURITY FLAW: Silently approved payment while fraud engine dependency was offline!")
	}
	if res.Decision != DecisionReject {
		t.Fatalf("expected REJECT under fail-closed, got %s", res.Decision)
	}
	if res.RiskScore != 100.0 {
		t.Fatalf("expected max risk score under fail-closed, got %f", res.RiskScore)
	}
}

func TestFraudEngine_ConcurrentVelocityUpdates(t *testing.T) {
	ctx := context.Background()
	tracker := NewMemVelocityTracker()
	auditor := NewMemAuditStore()
	cfg := DefaultConfig()
	engine := NewEngine(cfg, tracker, auditor)

	var wg sync.WaitGroup
	workers := 50

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, _ = engine.Evaluate(ctx, EvaluationRequest{
				IdempotencyKey: fmt.Sprintf("concurrent_eval_%d", idx),
				AccountID:      fmt.Sprintf("user_concurrent_%d", idx%5),
				BeneficiaryID:  "merchant_shared",
				Amount:         1000,
				Currency:       money.CurrencyINR,
			})
		}(i)
	}

	wg.Wait()
}
