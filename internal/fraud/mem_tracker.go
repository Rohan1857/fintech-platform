package fraud

import (
	"context"
	"sync"
	"time"

	"github.com/Rohan1857/fintech-platform/pkg/money"
)

type txRecord struct {
	beneficiaryID string
	amount        money.Money
	timestamp     time.Time
}

// MemVelocityTracker provides in-memory sliding-window velocity tracking.
type MemVelocityTracker struct {
	mu      sync.RWMutex
	records map[string][]txRecord // accountID -> records
	healthy bool
}

func NewMemVelocityTracker() *MemVelocityTracker {
	return &MemVelocityTracker{
		records: make(map[string][]txRecord),
		healthy: true,
	}
}

func (m *MemVelocityTracker) SetHealthy(healthy bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.healthy = healthy
}

func (m *MemVelocityTracker) IsHealthy(_ context.Context) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.healthy
}

func (m *MemVelocityTracker) RecordTransaction(_ context.Context, accountID, beneficiaryID string, amount money.Money, t time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.healthy {
		return context.DeadlineExceeded
	}

	m.records[accountID] = append(m.records[accountID], txRecord{
		beneficiaryID: beneficiaryID,
		amount:        amount,
		timestamp:     t,
	})
	return nil
}

func (m *MemVelocityTracker) GetVelocity(_ context.Context, accountID string, window time.Duration) (int64, money.Money, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.healthy {
		return 0, 0, context.DeadlineExceeded
	}

	cutoff := time.Now().UTC().Add(-window)
	var count int64
	var sum money.Money

	for _, rec := range m.records[accountID] {
		if rec.timestamp.After(cutoff) || rec.timestamp.Equal(cutoff) {
			count++
			sum += rec.amount
		}
	}

	return count, sum, nil
}

func (m *MemVelocityTracker) GetBeneficiaryFrequency(_ context.Context, accountID, beneficiaryID string, window time.Duration) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.healthy {
		return 0, context.DeadlineExceeded
	}

	cutoff := time.Now().UTC().Add(-window)
	var count int64

	for _, rec := range m.records[accountID] {
		if (rec.timestamp.After(cutoff) || rec.timestamp.Equal(cutoff)) && rec.beneficiaryID == beneficiaryID {
			count++
		}
	}

	return count, nil
}
