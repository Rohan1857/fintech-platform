package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Rohan1857/fintech-platform/internal/api"
	"github.com/Rohan1857/fintech-platform/internal/fraud"
	"github.com/Rohan1857/fintech-platform/internal/ledger"
	"github.com/Rohan1857/fintech-platform/internal/store/memstore"
	"github.com/Rohan1857/fintech-platform/internal/store/postgres"
	"github.com/Rohan1857/fintech-platform/pkg/money"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type BenchmarkResult struct {
	Tier           string
	Scenario       string
	Infrastructure string
	TotalRequests  int
	SuccessCount   int
	FailCount      int
	Duration       time.Duration
	ThroughputRPS  float64
	P50Latency     time.Duration
	P95Latency     time.Duration
	P99Latency     time.Duration
}

// postHTTPTransfer uses the same contract as an API client and verifies that
// the response represents a settled transfer, rather than a risk assessment.
func postHTTPTransfer(ctx context.Context, client *http.Client, baseURL, token, key, from, to string, amount money.Money) error {
	payload, err := json.Marshal(map[string]any{
		"from_account_id": from, "to_account_id": to, "amount": amount, "currency": "INR",
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/api/v1/transfers", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Idempotency-Key", key)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("payment not completed: HTTP %d", resp.StatusCode)
	}
	var transfer ledger.Transfer
	if err := json.NewDecoder(resp.Body).Decode(&transfer); err != nil {
		return err
	}
	if transfer.ID == "" || transfer.Status != ledger.StatusPosted || transfer.IdempotencyKey != key || transfer.Amount != amount {
		return fmt.Errorf("invalid completed transfer response: %+v", transfer)
	}
	return nil
}

func calculatePercentiles(latencies []time.Duration) (time.Duration, time.Duration, time.Duration) {
	if len(latencies) == 0 {
		return 0, 0, 0
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := latencies[int(float64(len(latencies))*0.50)]
	p95 := latencies[int(float64(len(latencies))*0.95)]
	p99 := latencies[int(float64(len(latencies))*0.99)]
	return p50, p95, p99
}

func runBenchmark(tier, scenario, infra string, totalReqs int, concurrency int, runFn func(workerID, reqIdx int) error) BenchmarkResult {
	latencies := make([]time.Duration, totalReqs)
	succeeded := make([]bool, totalReqs)
	var successCount atomic.Int32
	var failCount atomic.Int32

	reqCh := make(chan int, totalReqs)
	for i := 0; i < totalReqs; i++ {
		reqCh <- i
	}
	close(reqCh)

	start := time.Now()
	var wg sync.WaitGroup

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for reqIdx := range reqCh {
				t0 := time.Now()
				err := runFn(workerID, reqIdx)
				if err == nil {
					latencies[reqIdx] = time.Since(t0)
					succeeded[reqIdx] = true
					successCount.Add(1)
				} else {
					failCount.Add(1)
				}
			}
		}(w)
	}

	wg.Wait()
	duration := time.Since(start)

	completedLatencies := make([]time.Duration, 0, successCount.Load())
	for i, ok := range succeeded {
		if ok {
			completedLatencies = append(completedLatencies, latencies[i])
		}
	}
	p50, p95, p99 := calculatePercentiles(completedLatencies)
	rps := float64(successCount.Load()) / duration.Seconds()

	return BenchmarkResult{
		Tier:           tier,
		Scenario:       scenario,
		Infrastructure: infra,
		TotalRequests:  totalReqs,
		SuccessCount:   int(successCount.Load()),
		FailCount:      int(failCount.Load()),
		Duration:       duration,
		ThroughputRPS:  rps,
		P50Latency:     p50,
		P95Latency:     p95,
		P99Latency:     p99,
	}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return fmt.Errorf("DATABASE_URL must point to a disposable benchmark database; accounts and events will be created")
	}
	prefix := fmt.Sprintf("bench_%d_", time.Now().UnixNano())

	fmt.Println("=========================================================================================")
	fmt.Println("  FINTECH PAYMENT & FRAUD PLATFORM - RIGOROUS RE-BENCHMARK AUDIT")
	fmt.Println("=========================================================================================")
	fmt.Printf("Hardware: %s %s (%d CPUs)\n", runtime.GOOS, runtime.GOARCH, runtime.NumCPU())
	fmt.Println("Distinction: In-Memory Microbenchmark vs Real PostgreSQL vs Full HTTP Payment API")
	fmt.Println("=========================================================================================")
	fmt.Println()

	var allResults []BenchmarkResult

	// -------------------------------------------------------------------------
	// 1. TIER 1: IN-MEMORY LEDGER MICRO-BENCHMARK
	// -------------------------------------------------------------------------
	fmt.Println("[RUNNING TIER 1] In-Memory Ledger (sync.Mutex)...")
	memS := memstore.New()
	memNumAccounts := 100
	for i := 0; i < memNumAccounts; i++ {
		_ = memS.CreateAccount(ctx, ledger.Account{
			ID:       fmt.Sprintf("mem_wallet_%03d", i),
			Currency: money.CurrencyINR,
			Balance:  100000000,
		})
	}

	resMemOrd := runBenchmark("Tier 1: In-Memory", "Ordinary Independent Transfers", "In-Memory (sync.Mutex)", 2000, 20, func(workerID, reqIdx int) error {
		src := fmt.Sprintf("mem_wallet_%03d", reqIdx%memNumAccounts)
		dst := fmt.Sprintf("mem_wallet_%03d", (reqIdx+1)%memNumAccounts)
		key := fmt.Sprintf("mem_ord_tx_%d", reqIdx)
		_, err := memS.ApplyTransfer(ctx, ledger.TransferRequest{
			IdempotencyKey: key,
			FromAccountID:  src,
			ToAccountID:    dst,
			Amount:         100,
			Currency:       money.CurrencyINR,
		})
		return err
	})
	allResults = append(allResults, resMemOrd)

	resMemHot := runBenchmark("Tier 1: In-Memory", "Contended Hot-Account Transfers", "In-Memory (sync.Mutex)", 2000, 20, func(workerID, reqIdx int) error {
		dst := fmt.Sprintf("mem_wallet_%03d", (reqIdx%(memNumAccounts-1))+1)
		key := fmt.Sprintf("mem_hot_tx_%d", reqIdx)
		_, err := memS.ApplyTransfer(ctx, ledger.TransferRequest{
			IdempotencyKey: key,
			FromAccountID:  "mem_wallet_000",
			ToAccountID:    dst,
			Amount:         10,
			Currency:       money.CurrencyINR,
		})
		return err
	})
	allResults = append(allResults, resMemHot)

	// -------------------------------------------------------------------------
	// 2. TIER 2: REAL POSTGRESQL LEDGER BENCHMARK
	// -------------------------------------------------------------------------
	pgConfig, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		return fmt.Errorf("parse database URL: %w", err)
	}
	pgConfig.MaxConns = 50
	pgPool, err := pgxpool.NewWithConfig(ctx, pgConfig)
	if err != nil {
		return fmt.Errorf("create database pool: %w", err)
	}
	defer pgPool.Close()
	if err := pgPool.Ping(ctx); err != nil {
		return fmt.Errorf("database unavailable: %w", err)
	}

	pgS := postgres.New(pgPool)
	fmt.Println("[RUNNING TIER 2] Real PostgreSQL Ledger (ACID Row-Locks & Outbox Inserts)...")
	pgNumAccounts := 50
	for i := 0; i < pgNumAccounts; i++ {
		if err := pgS.CreateAccount(ctx, ledger.Account{
			ID:       fmt.Sprintf("%spg_wallet_%03d", prefix, i),
			Currency: money.CurrencyINR,
			Balance:  100000000,
		}); err != nil {
			return fmt.Errorf("seed ledger account: %w", err)
		}
	}

	resPGOrd := runBenchmark("Tier 2: Real PostgreSQL", "Independent Account Transfers", "PostgreSQL 16 (pgxpool 50)", 1000, 20, func(workerID, reqIdx int) error {
		src := fmt.Sprintf("%spg_wallet_%03d", prefix, reqIdx%pgNumAccounts)
		dst := fmt.Sprintf("%spg_wallet_%03d", prefix, (reqIdx+1)%pgNumAccounts)
		key := fmt.Sprintf("%spg_ord_tx_%d", prefix, reqIdx)
		_, err := pgS.ApplyTransfer(ctx, ledger.TransferRequest{
			IdempotencyKey: key,
			FromAccountID:  src,
			ToAccountID:    dst,
			Amount:         100,
			Currency:       money.CurrencyINR,
		})
		return err
	})
	allResults = append(allResults, resPGOrd)

	resPGHot := runBenchmark("Tier 2: Real PostgreSQL", "Contended Hot-Account Transfers", "PostgreSQL 16 (pgxpool 50)", 500, 10, func(workerID, reqIdx int) error {
		dst := fmt.Sprintf("%spg_wallet_%03d", prefix, (reqIdx%(pgNumAccounts-1))+1)
		key := fmt.Sprintf("%spg_hot_tx_%d", prefix, reqIdx)
		_, err := pgS.ApplyTransfer(ctx, ledger.TransferRequest{
			IdempotencyKey: key,
			FromAccountID:  prefix + "pg_wallet_000",
			ToAccountID:    dst,
			Amount:         10,
			Currency:       money.CurrencyINR,
		})
		return err
	})
	allResults = append(allResults, resPGHot)

	// -------------------------------------------------------------------------
	// 3. TIER 3: FULL HTTP PAYMENT API BENCHMARK (HTTP + AUTH + FRAUD + DB)
	// -------------------------------------------------------------------------
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "127.0.0.1:6379"
	}
	rClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rClient.Close()

	if err := rClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("Redis unavailable: %w", err)
	}
	fmt.Println("[RUNNING TIER 3] HTTP payments: 500 fresh sender/recipient pairs, default fraud rules enabled")
	// Setup is outside the measured interval. Each sender submits one payment,
	// so this scenario measures approved payments without disabling risk rules.
	const httpRequests = 500
	for i := 0; i < httpRequests; i++ {
		for _, account := range []ledger.Account{
			{ID: fmt.Sprintf("%shttp_from_%d", prefix, i), Currency: money.CurrencyINR, Balance: 10000},
			{ID: fmt.Sprintf("%shttp_to_%d", prefix, i), Currency: money.CurrencyINR},
		} {
			if err := pgS.CreateAccount(ctx, account); err != nil {
				return fmt.Errorf("seed HTTP account: %w", err)
			}
		}
	}
	tracker := fraud.NewRedisVelocityTracker(rClient)
	auditor := fraud.NewPostgresAuditStore(pgPool)
	engine := fraud.NewEngine(fraud.DefaultConfig(), tracker, auditor)
	cfg := api.DefaultRouterConfig()
	cfg.AuthToken = "benchmark-auth-token-2026"
	cfg.RateLimitRate, cfg.RateLimitBurst = 100000, 100000
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	ts := httptest.NewServer(api.NewRouter(api.NewServer(pgS, engine, auditor), cfg))
	defer ts.Close()
	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{MaxIdleConns: 100, MaxIdleConnsPerHost: 100},
	}
	defer client.CloseIdleConnections()
	resHTTP := runBenchmark("Tier 3: HTTP API", "Approved fresh-account payments", "HTTP + Redis + PostgreSQL", httpRequests, 20, func(_, i int) error {
		return postHTTPTransfer(ctx, client, ts.URL, cfg.AuthToken,
			fmt.Sprintf("%shttp_tx_%d", prefix, i), fmt.Sprintf("%shttp_from_%d", prefix, i),
			fmt.Sprintf("%shttp_to_%d", prefix, i), 50)
	})
	allResults = append(allResults, resHTTP)

	// -------------------------------------------------------------------------
	// PRINT HONEST BENCHMARK AUDIT TABLE
	// -------------------------------------------------------------------------
	fmt.Println("Completed/s and latency percentiles include SUCCESSFUL operations only; setup excluded.")
	fmt.Printf("%-24s | %-34s | %8s | %8s | %12s | %10s | %10s | %10s\n",
		"Tier", "Scenario", "Success", "Failed", "Completed/s", "p50", "p95", "p99")
	failed := false
	for _, r := range allResults {
		fmt.Printf("%-24s | %-34s | %8d | %8d | %12.1f | %10v | %10v | %10v\n",
			r.Tier, r.Scenario, r.SuccessCount, r.FailCount, r.ThroughputRPS, r.P50Latency, r.P95Latency, r.P99Latency)
		if r.FailCount > 0 {
			failed = true
		}
	}

	// Final Reconciliation Verification
	rep, err := pgS.Reconcile(ctx)
	if err != nil || !rep.IsClean {
		return fmt.Errorf("post-benchmark reconciliation failed: %+v, err=%v", rep, err)
	} else {
		fmt.Printf("\n[VERIFIED] PostgreSQL Reconciliation 100%% Clean! Total Accounts=%d, Total Transfers=%d, Total Entries=%d, TotalEntriesSum=%d\n",
			rep.TotalAccounts, rep.TotalTransfers, rep.TotalEntries, rep.TotalEntriesSum)
	}
	if failed {
		return fmt.Errorf("benchmark had failed operations; inspect Success/Failed columns")
	}
	return nil
}
