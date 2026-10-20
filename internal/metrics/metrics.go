package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Payment throughput and status counter
	PaymentRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "payment_requests_total",
			Help: "Total number of payment requests partitioned by operation and outcome status",
		},
		[]string{"operation", "status"},
	)

	// Payment latency histogram for p50, p95, p99 analysis
	PaymentDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "payment_duration_seconds",
			Help:    "Payment processing latency in seconds",
			Buckets: []float64{0.001, 0.002, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0},
		},
		[]string{"operation"},
	)

	// Fraud evaluation outcomes and rejection rate
	FraudEvaluationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "fraud_evaluations_total",
			Help: "Total number of fraud checks categorized by engine decision",
		},
		[]string{"decision"},
	)

	// Kafka consumer lag and throughput
	KafkaConsumerLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_consumer_lag",
			Help: "Estimated unread message lag for Kafka consumers",
		},
		[]string{"topic", "group"},
	)

	KafkaMessagesProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_messages_processed_total",
			Help: "Total Kafka events consumed and processed",
		},
		[]string{"topic", "status"},
	)

	// Outbox backlog gauge
	OutboxBacklog = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "outbox_backlog",
			Help: "Current count of unpublished transactional outbox events pending Kafka relay",
		},
	)

	// Database transaction failures
	DBTransactionErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "db_transaction_errors_total",
			Help: "Total database transaction errors classified by error category",
		},
		[]string{"error_type"},
	)
)

// ObservePayment records duration and increments status counters.
func ObservePayment(operation, status string, start time.Time) {
	duration := time.Since(start).Seconds()
	PaymentDurationSeconds.WithLabelValues(operation).Observe(duration)
	PaymentRequestsTotal.WithLabelValues(operation, status).Inc()
}

// ObserveFraud records fraud evaluation decision.
func ObserveFraud(decision string) {
	FraudEvaluationsTotal.WithLabelValues(decision).Inc()
}
