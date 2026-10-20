package outbox

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/messi/fintech-platform/internal/events"
)

// Config configures outbox relay polling and retry policies.
type Config struct {
	PollInterval    time.Duration
	BatchSize       int
	MaxRetries      int
	TopicEvents     string
	TopicDLQ        string
}

func DefaultConfig() Config {
	return Config{
		PollInterval: 200 * time.Millisecond,
		BatchSize:    50,
		MaxRetries:   5,
		TopicEvents:  "payment.events",
		TopicDLQ:     "payment.dlq",
	}
}

// Relay handles polling outbox table and reliably forwarding events to Kafka.
type Relay struct {
	pool      *pgxpool.Pool
	publisher events.Publisher
	cfg       Config
}

func NewRelay(pool *pgxpool.Pool, publisher events.Publisher, cfg Config) *Relay {
	return &Relay{
		pool:      pool,
		publisher: publisher,
		cfg:       cfg,
	}
}

// ProcessBatch scans and dispatches a single batch of pending outbox events.
func (r *Relay) ProcessBatch(ctx context.Context) (int, error) {
	if r.pool == nil {
		return 0, nil
	}

	rows, err := r.pool.Query(ctx, `
		SELECT id, event_type, aggregate_type, aggregate_id, payload, created_at, retry_count
		FROM outbox_events
		WHERE published_at IS NULL
		ORDER BY created_at ASC
		LIMIT $1`, r.cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	type outboxRow struct {
		id            string
		eventType     string
		aggregateType string
		aggregateID   string
		payload       []byte
		createdAt     time.Time
		retryCount    int
	}

	var batch []outboxRow
	for rows.Next() {
		var row outboxRow
		if err := rows.Scan(&row.id, &row.eventType, &row.aggregateType, &row.aggregateID, &row.payload, &row.createdAt, &row.retryCount); err != nil {
			return 0, err
		}
		batch = append(batch, row)
	}

	if len(batch) == 0 {
		return 0, nil
	}

	processedCount := 0
	for _, item := range batch {
		ev := events.Event{
			ID:            item.id,
			Type:          item.eventType,
			CorrelationID: item.aggregateID,
			AggregateType: item.aggregateType,
			AggregateID:   item.aggregateID,
			Payload:       json.RawMessage(item.payload),
			CreatedAt:     item.createdAt,
		}

		targetTopic := r.cfg.TopicEvents
		if item.retryCount >= r.cfg.MaxRetries {
			targetTopic = r.cfg.TopicDLQ
		}

		pubErr := r.publisher.Publish(ctx, targetTopic, item.aggregateID, ev)
		now := time.Now().UTC()
		if pubErr == nil {
			_, _ = r.pool.Exec(ctx, `
				UPDATE outbox_events
				SET published_at = $1
				WHERE id = $2`, now, item.id)
			processedCount++
		} else {
			errMsg := pubErr.Error()
			_, _ = r.pool.Exec(ctx, `
				UPDATE outbox_events
				SET retry_count = retry_count + 1, last_error = $1
				WHERE id = $2`, errMsg, item.id)
		}
	}

	return processedCount, nil
}

// Start runs the outbox relay background worker until context is cancelled.
func (r *Relay) Start(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			_, _ = r.ProcessBatch(ctx)
		}
	}
}
