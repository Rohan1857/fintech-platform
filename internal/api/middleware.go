package api

import (
	"context"
	"crypto/subtle"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type contextKey string

const (
	RequestIDKey contextKey = "request_id"
)

// LoggingMiddleware logs HTTP requests with structured fields, redacting secrets.
func LoggingMiddleware(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			reqID := r.Header.Get("X-Request-ID")
			if reqID == "" {
				reqID = uuid.New().String()
			}

			ctx := context.WithValue(r.Context(), RequestIDKey, reqID)
			r = r.WithContext(ctx)

			ww := &statusResponseWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(ww, r)

			duration := time.Since(start)
			logger.Info("http_request",
				"request_id", reqID,
				"method", r.Method,
				"path", r.URL.Path,
				"status", ww.status,
				"duration_ms", duration.Milliseconds(),
				"remote_addr", r.RemoteAddr,
			)
		})
	}
}

type statusResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusResponseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

// AuthMiddleware verifies Bearer token or API key.
func AuthMiddleware(validToken string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Bypass health and metrics
			if r.URL.Path == "/health" || r.URL.Path == "/metrics" {
				next.ServeHTTP(w, r)
				return
			}

			authHeader := r.Header.Get("Authorization")
			apiKey := r.Header.Get("X-API-Key")

			var providedToken string
			if strings.HasPrefix(authHeader, "Bearer ") {
				providedToken = strings.TrimPrefix(authHeader, "Bearer ")
			} else if apiKey != "" {
				providedToken = apiKey
			}

			if providedToken == "" || subtle.ConstantTimeCompare([]byte(providedToken), []byte(validToken)) != 1 {
				http.Error(w, `{"error":"unauthorized: invalid or missing authentication token"}`, http.StatusUnauthorized)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// TimeoutMiddleware sets a maximum request context deadline.
func TimeoutMiddleware(timeout time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			defer cancel()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RateLimiter implements an in-memory token bucket rate limiter.
type RateLimiter struct {
	mu       sync.Mutex
	tokens   map[string]float64
	lastSeen map[string]time.Time
	rate     float64 // tokens per second
	burst    float64 // max burst
}

func NewRateLimiter(rate, burst float64) *RateLimiter {
	return &RateLimiter{
		tokens:   make(map[string]float64),
		lastSeen: make(map[string]time.Time),
		rate:     rate,
		burst:    burst,
	}
}

func (rl *RateLimiter) Allow(clientKey string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	last, ok := rl.lastSeen[clientKey]
	if !ok {
		rl.tokens[clientKey] = rl.burst - 1
		rl.lastSeen[clientKey] = now
		return true
	}

	elapsed := now.Sub(last).Seconds()
	rl.lastSeen[clientKey] = now
	rl.tokens[clientKey] += elapsed * rl.rate
	if rl.tokens[clientKey] > rl.burst {
		rl.tokens[clientKey] = rl.burst
	}

	if rl.tokens[clientKey] >= 1 {
		rl.tokens[clientKey]--
		return true
	}
	return false
}

func RateLimitMiddleware(rl *RateLimiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/health" || r.URL.Path == "/metrics" {
				next.ServeHTTP(w, r)
				return
			}

			clientIP := r.RemoteAddr
			if idx := strings.LastIndex(clientIP, ":"); idx != -1 {
				clientIP = clientIP[:idx]
			}

			if !rl.Allow(clientIP) {
				http.Error(w, `{"error":"too many requests: rate limit exceeded"}`, http.StatusTooManyRequests)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// RecoveryMiddleware prevents server crashes by catching panics.
func RecoveryMiddleware(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					logger.Error("panic_recovered", "error", rec)
					http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
