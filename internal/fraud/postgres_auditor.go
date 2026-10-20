package fraud

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresAuditStore persists fraud evaluation decisions in PostgreSQL.
type PostgresAuditStore struct {
	pool *pgxpool.Pool
}

func NewPostgresAuditStore(pool *pgxpool.Pool) *PostgresAuditStore {
	return &PostgresAuditStore{pool: pool}
}

func (s *PostgresAuditStore) SaveEvaluation(ctx context.Context, res EvaluationResult, req EvaluationRequest) error {
	if s.pool == nil {
		return nil
	}

	hitsJSON, _ := json.Marshal(res.TriggeredRules)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO fraud_evaluations (
			id, idempotency_key, payment_id, account_id, beneficiary_id,
			amount, currency, score, decision, reasons, rule_hits, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		res.EvaluationID, req.IdempotencyKey, req.PaymentID, req.AccountID, req.BeneficiaryID,
		int64(req.Amount), req.Currency, res.RiskScore, string(res.Decision), res.Reasons, hitsJSON, res.EvaluatedAt,
	)
	return err
}

func (s *PostgresAuditStore) GetEvaluation(ctx context.Context, id string) (EvaluationResult, error) {
	var res EvaluationResult
	var reasons []string
	var hitsJSON []byte
	var decision string
	var score float64

	err := s.pool.QueryRow(ctx, `
		SELECT id, decision, score, reasons, rule_hits, created_at
		FROM fraud_evaluations WHERE id = $1`, id).Scan(
		&res.EvaluationID, &decision, &score, &reasons, &hitsJSON, &res.EvaluatedAt,
	)
	if err != nil {
		return EvaluationResult{}, err
	}

	res.Decision = Decision(decision)
	res.RiskScore = score
	res.Reasons = reasons
	_ = json.Unmarshal(hitsJSON, &res.TriggeredRules)
	return res, nil
}
