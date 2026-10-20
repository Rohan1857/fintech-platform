package events

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// Publisher abstracts event delivery to asynchronous message brokers.
type Publisher interface {
	Publish(ctx context.Context, topic string, key string, event Event) error
	Close() error
}

// KafkaPublisher delivers events to Apache Kafka topics.
type KafkaPublisher struct {
	writers map[string]*kafka.Writer
	brokers []string
	mu      sync.Mutex
}

func NewKafkaPublisher(brokers []string) *KafkaPublisher {
	return &KafkaPublisher{
		brokers: brokers,
		writers: make(map[string]*kafka.Writer),
	}
}

func (p *KafkaPublisher) getWriter(topic string) *kafka.Writer {
	p.mu.Lock()
	defer p.mu.Unlock()

	if w, ok := p.writers[topic]; ok {
		return w
	}

	w := &kafka.Writer{
		Addr:         kafka.TCP(p.brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // Partitions deterministically by key (Account/Aggregate ID)
		WriteTimeout: 5 * time.Second,
		RequiredAcks: kafka.RequireOne,
	}
	p.writers[topic] = w
	return w
}

func (p *KafkaPublisher) Publish(ctx context.Context, topic string, key string, event Event) error {
	w := p.getWriter(topic)
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: data,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(event.Type)},
			{Key: "correlation_id", Value: []byte(event.CorrelationID)},
		},
		Time: event.CreatedAt,
	})
}

func (p *KafkaPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var lastErr error
	for _, w := range p.writers {
		if err := w.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// MemPublisher records published events in-memory for testing and offline modes.
type MemPublisher struct {
	mu        sync.RWMutex
	Events    map[string][]Event // topic -> events
	available bool
}

func NewMemPublisher() *MemPublisher {
	return &MemPublisher{
		Events:    make(map[string][]Event),
		available: true,
	}
}

func (m *MemPublisher) SetAvailable(available bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.available = available
}

func (m *MemPublisher) Publish(_ context.Context, topic string, _ string, event Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.available {
		return context.DeadlineExceeded
	}

	m.Events[topic] = append(m.Events[topic], event)
	return nil
}

func (m *MemPublisher) Close() error {
	return nil
}

func (m *MemPublisher) GetEvents(topic string) []Event {
	m.mu.RLock()
	defer m.mu.RUnlock()
	list := m.Events[topic]
	out := make([]Event, len(list))
	copy(out, list)
	return out
}
