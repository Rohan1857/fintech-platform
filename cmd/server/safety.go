package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/Rohan1857/fintech-platform/internal/store"
)

func loadAuthToken() (string, error) {
	token := os.Getenv("API_AUTH_TOKEN")
	if strings.TrimSpace(token) == "" || token == "slice-secret-token-2026" {
		return "", fmt.Errorf("API_AUTH_TOKEN must be explicitly set to a private value; the former public demo token is not accepted")
	}
	return token, nil
}

// runHoldExpiry sweeps on startup and periodically until service shutdown.
func runHoldExpiry(ctx context.Context, s store.Store, interval time.Duration, logger *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		n, err := s.ExpireHolds(ctx, time.Now().UTC())
		if err != nil && ctx.Err() == nil {
			logger.Error("hold expiry sweep failed", "error", err)
		}
		if err == nil && n > 0 {
			logger.Info("expired payment holds released", "count", n)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
