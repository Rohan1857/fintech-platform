package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestConsumer_IdempotencyAndNoDuplicateEffects(t *testing.T) {
	ctx := context.Background()
	dlqPub := NewMemPublisher()

	cfg := ConsumerConfig{
		Topic:        "payment.events",
		DLQTopic:     "payment.dlq",
		GroupID:      "test-group",
		MaxRetries:   2,
		RetryBackoff: 10 * time.Millisecond,
	}
	consumer := NewConsumer(cfg, nil, dlqPub)

	var executionCount atomic.Int32
	consumer.RegisterHandler(EventPaymentCompleted, func(ctx context.Context, ev Event) error {
		executionCount.Add(1)
		return nil
	})

	payload, _ := json.Marshal(PaymentEventPayload{
		TransferID: "tx_123",
		Amount:     5000,
		Currency:   "INR",
		Status:     "posted",
	})

	ev := Event{
		ID:            "event_unique_001",
		Type:          EventPaymentCompleted,
		CorrelationID: "tx_123",
		AggregateType: "transfer",
		AggregateID:   "tx_123",
		Payload:       payload,
		CreatedAt:     time.Now().UTC(),
	}

	// Deliver event 10 times (simulating Kafka replay / redelivery)
	for i := 0; i < 10; i++ {
		err := consumer.ProcessEvent(ctx, ev)
		if err != nil {
			t.Fatalf("unexpected process error on iteration %d: %v", i, err)
		}
	}

	// Execution count MUST be exactly 1!
	if executionCount.Load() != 1 {
		t.Fatalf("expected handler to execute exactly 1 time, executed %d times", executionCount.Load())
	}
}

func TestConsumer_DeadLetterQueueRouting(t *testing.T) {
	ctx := context.Background()
	dlqPub := NewMemPublisher()

	cfg := ConsumerConfig{
		Topic:        "payment.events",
		DLQTopic:     "payment.dlq",
		GroupID:      "test-group",
		MaxRetries:   2,
		RetryBackoff: 10 * time.Millisecond,
	}
	consumer := NewConsumer(cfg, nil, dlqPub)

	consumer.RegisterHandler(EventPaymentFailed, func(ctx context.Context, ev Event) error {
		return errors.New("simulated fatal handler failure")
	})

	ev := Event{
		ID:            "event_fail_002",
		Type:          EventPaymentFailed,
		CorrelationID: "tx_fail",
		AggregateType: "transfer",
		AggregateID:   "tx_fail",
		CreatedAt:     time.Now().UTC(),
	}

	err := consumer.ProcessEvent(ctx, ev)
	if err == nil {
		t.Fatalf("expected error from failed handler")
	}

	// Verify event was routed to DLQ topic
	dlqEvents := dlqPub.GetEvents("payment.dlq")
	if len(dlqEvents) != 1 {
		t.Fatalf("expected 1 event in DLQ, got %d", len(dlqEvents))
	}
	if dlqEvents[0].ID != ev.ID {
		t.Fatalf("DLQ event ID mismatch: %s != %s", dlqEvents[0].ID, ev.ID)
	}
}

func TestConsumer_AllRequiredLifecycleEvents(t *testing.T) {
	ctx := context.Background()
	dlqPub := NewMemPublisher()

	cfg := ConsumerConfig{
		Topic:        "payment.events",
		DLQTopic:     "payment.dlq",
		GroupID:      "test-group",
		MaxRetries:   1,
		RetryBackoff: 5 * time.Millisecond,
	}
	consumer := NewConsumer(cfg, nil, dlqPub)

	requiredEvents := []string{
		EventPaymentCreated,
		EventPaymentCompleted,
		EventPaymentFailed,
		EventPaymentRefunded,
		EventFraudFlagged,
	}

	handled := make(map[string]bool)
	for _, evtType := range requiredEvents {
		et := evtType
		consumer.RegisterHandler(et, func(ctx context.Context, ev Event) error {
			handled[et] = true
			return nil
		})
	}

	for i, evtType := range requiredEvents {
		ev := Event{
			ID:            fmt.Sprintf("ev_req_%d", i),
			Type:          evtType,
			AggregateID:   fmt.Sprintf("agg_%d", i),
			AggregateType: "payment",
			CreatedAt:     time.Now().UTC(),
		}
		if err := consumer.ProcessEvent(ctx, ev); err != nil {
			t.Fatalf("failed processing %s: %v", evtType, err)
		}
	}

	for _, evtType := range requiredEvents {
		if !handled[evtType] {
			t.Fatalf("required event type %s was not handled", evtType)
		}
	}
}
