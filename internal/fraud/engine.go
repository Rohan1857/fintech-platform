package fraud

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// AuditStore abstracts persistence for risk evaluations.
type AuditStore interface {
	SaveEvaluation(ctx context.Context, res EvaluationResult, req EvaluationRequest) error
	GetEvaluation(ctx context.Context, id string) (EvaluationResult, error)
}

// MemAuditStore stores evaluations in memory for testing and local mode.
type MemAuditStore struct {
	mu          sync.RWMutex
	evaluations map[string]EvaluationResult
}

func NewMemAuditStore() *MemAuditStore {
	return &MemAuditStore{evaluations: make(map[string]EvaluationResult)}
}

func (m *MemAuditStore) SaveEvaluation(_ context.Context, res EvaluationResult, _ EvaluationRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.evaluations[res.EvaluationID] = res
	return nil
}

func (m *MemAuditStore) GetEvaluation(_ context.Context, id string) (EvaluationResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.evaluations[id]
	if !ok {
		return EvaluationResult{}, fmt.Errorf("evaluation not found: %s", id)
	}
	return e, nil
}

// Engine performs synchronous pre-flight fraud analysis.
type Engine struct {
	cfg     Config
	tracker VelocityTracker
	auditor AuditStore
}

func NewEngine(cfg Config, tracker VelocityTracker, auditor AuditStore) *Engine {
	return &Engine{
		cfg:     cfg,
		tracker: tracker,
		auditor: auditor,
	}
}

// Evaluate performs synchronous multi-factor risk assessment before funds move.
func (e *Engine) Evaluate(ctx context.Context, req EvaluationRequest) (EvaluationResult, error) {
	now := time.Now().UTC()
	if req.Timestamp.IsZero() {
		req.Timestamp = now
	}

	evalID := newID()
	var score float64
	var reasons []string
	var triggered []RuleHit

	// Safe Failure Policy: Check dependency health before proceeding
	if !e.tracker.IsHealthy(ctx) {
		if e.cfg.FailClosed {
			res := EvaluationResult{
				EvaluationID: evalID,
				Decision:     e.cfg.FallbackDecisionOnError,
				RiskScore:    100.0,
				Reasons:      []string{"security dependency unavailable: fail-closed policy activated"},
				TriggeredRules: []RuleHit{
					{
						RuleID:      "SEC-ERR-001",
						RuleName:    "DependencyHealthCheck",
						Score:       100.0,
						Reason:      "velocity tracking dependency offline; transaction halted",
						HardOutcome: e.cfg.FallbackDecisionOnError,
					},
				},
				EvaluatedAt: now,
			}
			if e.auditor != nil {
				_ = e.auditor.SaveEvaluation(ctx, res, req)
			}
			return res, nil
		}
	}

	// 1. High-Value and Critical-Value Detection
	if req.Amount >= e.cfg.CriticalValueThresholdMinor {
		hit := RuleHit{
			RuleID:      "RULE-HV-002",
			RuleName:    "CriticalValueTransaction",
			Score:       60.0,
			Reason:      fmt.Sprintf("transaction amount (%s) exceeds critical limit", req.Amount.ToMajorString()),
			HardOutcome: DecisionReview,
		}
		score += hit.Score
		reasons = append(reasons, hit.Reason)
		triggered = append(triggered, hit)
	} else if req.Amount >= e.cfg.HighValueThresholdMinor {
		hit := RuleHit{
			RuleID:   "RULE-HV-001",
			RuleName: "HighValueTransaction",
			Score:    40.0,
			Reason:   fmt.Sprintf("transaction amount (%s) exceeds high-value threshold", req.Amount.ToMajorString()),
		}
		score += hit.Score
		reasons = append(reasons, hit.Reason)
		triggered = append(triggered, hit)
	}

	// 2. Short-Window Velocity Check
	shortCount, _, err := e.tracker.GetVelocity(ctx, req.AccountID, e.cfg.VelocityWindowShort)
	if err != nil {
		if e.cfg.FailClosed {
			return e.buildFailClosedResult(ctx, evalID, req, "failed to query short-window velocity", now)
		}
	} else if shortCount >= e.cfg.VelocityMaxCountShort {
		hit := RuleHit{
			RuleID:   "RULE-VEL-001",
			RuleName: "ShortWindowVelocityExceeded",
			Score:    45.0,
			Reason:   fmt.Sprintf("%d transactions in past %v exceeds maximum (%d)", shortCount, e.cfg.VelocityWindowShort, e.cfg.VelocityMaxCountShort),
		}
		score += hit.Score
		reasons = append(reasons, hit.Reason)
		triggered = append(triggered, hit)
	}

	// 3. Medium-Window Aggregate Amount & Count Velocity
	medCount, medAmount, err := e.tracker.GetVelocity(ctx, req.AccountID, e.cfg.VelocityWindowMedium)
	if err != nil {
		if e.cfg.FailClosed {
			return e.buildFailClosedResult(ctx, evalID, req, "failed to query medium-window velocity", now)
		}
	} else {
		if medCount >= e.cfg.VelocityMaxCountMedium {
			hit := RuleHit{
				RuleID:   "RULE-VEL-002",
				RuleName: "MediumWindowCountExceeded",
				Score:    35.0,
				Reason:   fmt.Sprintf("%d transactions in past %v exceeds limit", medCount, e.cfg.VelocityWindowMedium),
			}
			score += hit.Score
			reasons = append(reasons, hit.Reason)
			triggered = append(triggered, hit)
		}
		if medAmount+req.Amount > e.cfg.VelocityMaxAmountMedium {
			hit := RuleHit{
				RuleID:   "RULE-VEL-003",
				RuleName: "MediumWindowCumulativeAmountExceeded",
				Score:    50.0,
				Reason:   fmt.Sprintf("cumulative amount (%s) in %v exceeds maximum (%s)", (medAmount + req.Amount).ToMajorString(), e.cfg.VelocityWindowMedium, e.cfg.VelocityMaxAmountMedium.ToMajorString()),
			}
			score += hit.Score
			reasons = append(reasons, hit.Reason)
			triggered = append(triggered, hit)
		}
	}

	// 4. Repeated Payments to Same Beneficiary
	if req.BeneficiaryID != "" {
		beneCount, err := e.tracker.GetBeneficiaryFrequency(ctx, req.AccountID, req.BeneficiaryID, e.cfg.VelocityWindowShort)
		if err != nil {
			if e.cfg.FailClosed {
				return e.buildFailClosedResult(ctx, evalID, req, "failed to query beneficiary frequency", now)
			}
		} else if beneCount >= e.cfg.BeneficiaryMaxCountShort {
			hit := RuleHit{
				RuleID:   "RULE-BENE-001",
				RuleName: "RapidBeneficiaryRepeatTransfer",
				Score:    40.0,
				Reason:   fmt.Sprintf("%d rapid payments sent to beneficiary %s within %v", beneCount, req.BeneficiaryID, e.cfg.VelocityWindowShort),
			}
			score += hit.Score
			reasons = append(reasons, hit.Reason)
			triggered = append(triggered, hit)
		}
	}

	// 5. Frequency Anomaly Detection (Rate Spike: 1-minute rate significantly exceeding medium rate)
	if medCount > 0 && shortCount > 0 {
		avgShortPerMin := float64(shortCount)
		avgMedPerMin := float64(medCount) / (e.cfg.VelocityWindowMedium.Minutes())
		if avgMedPerMin > 0 && avgShortPerMin >= 3.0*avgMedPerMin && shortCount >= 3 {
			hit := RuleHit{
				RuleID:   "RULE-FREQ-001",
				RuleName: "SuddenFrequencyBurstAnomaly",
				Score:    30.0,
				Reason:   fmt.Sprintf("transaction frequency rate (%.1f/min) is 3x higher than average (%.1f/min)", avgShortPerMin, avgMedPerMin),
			}
			score += hit.Score
			reasons = append(reasons, hit.Reason)
			triggered = append(triggered, hit)
		}
	}

	// Determine final decision based on score thresholds and hard rule outcomes
	decision := DecisionApprove
	for _, t := range triggered {
		if t.HardOutcome == DecisionReject {
			decision = DecisionReject
			break
		}
		if t.HardOutcome == DecisionReview && decision != DecisionReject {
			decision = DecisionReview
		}
	}

	if decision != DecisionReject {
		if score >= e.cfg.RejectThreshold {
			decision = DecisionReject
		} else if score >= e.cfg.ReviewThreshold {
			decision = DecisionReview
		}
	}

	result := EvaluationResult{
		EvaluationID:   evalID,
		Decision:       decision,
		RiskScore:      score,
		Reasons:        reasons,
		TriggeredRules: triggered,
		EvaluatedAt:    now,
	}

	// If transaction approved or reviewed, update velocity tracker synchronously
	if decision == DecisionApprove || decision == DecisionReview {
		if err := e.tracker.RecordTransaction(ctx, req.AccountID, req.BeneficiaryID, req.Amount, now); err != nil && e.cfg.FailClosed {
			return e.buildFailClosedResult(ctx, evalID, req, "failed to record transaction velocity", now)
		}
	}

	// Persist audit record
	if e.auditor != nil {
		_ = e.auditor.SaveEvaluation(ctx, result, req)
	}

	return result, nil
}

func (e *Engine) buildFailClosedResult(ctx context.Context, evalID string, req EvaluationRequest, detail string, now time.Time) (EvaluationResult, error) {
	res := EvaluationResult{
		EvaluationID: evalID,
		Decision:     e.cfg.FallbackDecisionOnError,
		RiskScore:    100.0,
		Reasons:      []string{fmt.Sprintf("safe fail-closed policy activated: %s", detail)},
		TriggeredRules: []RuleHit{
			{
				RuleID:      "FAIL-CLOSED-001",
				RuleName:    "SafeFailurePolicy",
				Score:       100.0,
				Reason:      detail,
				HardOutcome: e.cfg.FallbackDecisionOnError,
			},
		},
		EvaluatedAt: now,
	}
	if e.auditor != nil {
		_ = e.auditor.SaveEvaluation(ctx, res, req)
	}
	return res, nil
}
