package api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// RouterConfig defines router and middleware options.
type RouterConfig struct {
	AuthToken      string
	RateLimitRate  float64
	RateLimitBurst float64
	RequestTimeout time.Duration
	Logger         *slog.Logger
}

func DefaultRouterConfig() RouterConfig {
	return RouterConfig{
		AuthToken:      "slice-secret-token-2026",
		RateLimitRate:  100,
		RateLimitBurst: 200,
		RequestTimeout: 10 * time.Second,
		Logger:         slog.Default(),
	}
}

// NewRouter constructs the http.Handler with all middleware applied.
func NewRouter(server *Server, cfg RouterConfig) http.Handler {
	mux := http.NewServeMux()

	// Public Observability
	mux.HandleFunc("GET /health", server.HandleHealth)
	mux.Handle("GET /metrics", promhttp.Handler())

	// Accounts
	mux.HandleFunc("POST /api/v1/accounts", server.HandleCreateAccount)
	mux.HandleFunc("GET /api/v1/accounts/{id}", server.HandleGetAccount)
	mux.HandleFunc("GET /api/v1/accounts/{id}/history", server.HandleGetHistory)

	// Direct Transfers
	mux.HandleFunc("POST /api/v1/transfers", server.HandleTransfer)

	// Auth-Capture Lifecycle
	mux.HandleFunc("POST /api/v1/payments/authorize", server.HandleAuthorize)
	mux.HandleFunc("POST /api/v1/payments/capture", server.HandleCapture)
	mux.HandleFunc("POST /api/v1/payments/void", server.HandleVoid)

	// Refunds
	mux.HandleFunc("POST /api/v1/payments/refund", server.HandleRefund)

	// Fraud Audits
	mux.HandleFunc("GET /api/v1/fraud/evaluations/{id}", server.HandleGetFraudEvaluation)

	// Administrative Reconciliation
	mux.HandleFunc("POST /api/v1/reconciliation", server.HandleReconcile)

	// Apply Middlewares in order: Recovery -> Logging -> Timeout -> RateLimit -> Auth -> Mux
	rl := NewRateLimiter(cfg.RateLimitRate, cfg.RateLimitBurst)

	var handler http.Handler = mux
	handler = AuthMiddleware(cfg.AuthToken)(handler)
	handler = RateLimitMiddleware(rl)(handler)
	handler = TimeoutMiddleware(cfg.RequestTimeout)(handler)
	handler = LoggingMiddleware(cfg.Logger)(handler)
	handler = RecoveryMiddleware(cfg.Logger)(handler)

	return handler
}
