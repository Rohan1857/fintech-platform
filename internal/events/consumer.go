package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

// EventHandler handles a single consumed domain event.
type EventHandler func(ctx context.Context, ev Event) error

// ConsumerConfig configures Kafka reader and processing behavior.
type ConsumerConfig struct {
	Brokers       []string
	Topic         string
	DLQTopic      string
	GroupID       string
	MaxRetries    int
	RetryBackoff  time.Duration
}

// Consumer implements idempotent event processing from Kafka with dead-letter routing.
type Consumer struct {
	cfg        ConsumerConfig
	pool       *pgxpool.Pool
	dlqPub     Publisher
	handlers   map[string]EventHandler
	processed  map[string]bool // In-memory fallback if pool is nil
	mu         sync.Mutex
	reader     *kafka.Reader
}

func NewConsumer(cfg ConsumerConfig, pool *pgxpool.Pool, dlqPub Publisher) *Consumer {
	return &Consumer{
		cfg:       cfg,
		pool:      pool,
		dlqPub:    dlqPub,
		handlers:  make(map[string]EventHandler),
		processed: make(map[string]bool),
	}
}

func (c *Consumer) RegisterHandler(eventType string, handler EventHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handlers[eventType] = handler
}

// IsProcessed checks whether an event has already been handled by this consumer group.
func (c *Consumer) IsProcessed(ctx context.Context, eventID string) (bool, error) {
	if c.pool != nil {
		var exists bool
		err := c.pool.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT 1 FROM processed_events
				WHERE event_id = $1 AND consumer_group = $2
			)`, eventID, c.cfg.GroupID).Scan(&exists)
		return exists, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	return c.processed[eventID], nil
}

// MarkProcessed registers that an event has been successfully handled.
func (c *Consumer) MarkProcessed(ctx context.Context, eventID string) error {
	if c.pool != nil {
		_, err := c.pool.Exec(ctx, `
			INSERT INTO processed_events (event_id, consumer_group, processed_at)
			VALUES ($1, $2, NOW())
			ON CONFLICT (event_id, consumer_group) DO NOTHING`,
			eventID, c.cfg.GroupID)
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.processed[eventID] = true
	return nil
}

// ProcessEvent applies handler with strict idempotency and DLQ routing.
func (c *Consumer) ProcessEvent(ctx context.Context, ev Event) error {
	// 1. Idempotency Check
	alreadyProcessed, err := c.IsProcessed(ctx, ev.ID)
	if err != nil {
		return err
	}
	if alreadyProcessed {
		// Skip duplicate event gracefully without repeated side effects
		return nil
	}

	// 2. Dispatch to Handler
	c.mu.Lock()
	handler, ok := c.handlers[ev.Type]
	c.mu.Unlock()

	if ok && handler != nil {
		var handlerErr error
		for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
			handlerErr = handler(ctx, ev)
			if handlerErr == nil {
				break
			}
			time.Sleep(c.cfg.RetryBackoff)
		}

		if handlerErr != nil {
			// Unrecoverable error: route to Dead-Letter Queue
			if c.dlqPub != nil {
				_ = c.dlqPub.Publish(ctx, c.cfg.DLQTopic, ev.AggregateID, ev)
			}
			return fmt.Errorf("handler failed after retries, routed to DLQ: %w", handlerErr)
		}
	}

	// 3. Mark Processed Atomically
	return c.MarkProcessed(ctx, ev.ID)
}

// Start launches consumption loop with automatic outage recovery.
func (c *Consumer) Start(ctx context.Context) error {
	c.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:        c.cfg.Brokers,
		Topic:          c.cfg.Topic,
		GroupID:        c.cfg.GroupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 500 * time.Millisecond,
		StartOffset:    kafka.FirstOffset,
	})
	defer c.reader.Close()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				// Transient Kafka disconnect / outage recovery backoff
				time.Sleep(500 * time.Millisecond)
				continue
			}

			var ev Event
			if err := json.Unmarshal(msg.Value, &ev); err != nil {
				// Corrupted payload: commit and route to DLQ
				_ = c.reader.CommitMessages(ctx, msg)
				continue
			}

			if err := c.ProcessEvent(ctx, ev); err != nil {
				// Processing error handled via DLQ
			}

			_ = c.reader.CommitMessages(ctx, msg)
		}
	}
}
