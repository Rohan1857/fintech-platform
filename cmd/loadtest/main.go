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
	"github.com/Rohan1857/fintech-platform/internal/events"
	"github.com/Rohan1857/fintech-platform/internal/fraud"
	"github.com/Rohan1857/fintech-platform/internal/ledger"
	"github.com/Rohan1857/fintech-platform/internal/outbox"
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
				latencies[reqIdx] = time.Since(t0)
				if err == nil {
					successCount.Add(1)
				} else {
					failCount.Add(1)
				}
			}
		}(w)
	}

	wg.Wait()
	duration := time.Since(start)

	p50, p95, p99 := calculatePercentiles(latencies)
	rps := float64(totalReqs) / duration.Seconds()

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
	ctx := context.Background()

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
	// 2. TIER 2: REAL POSTGRESQL 16 LEDGER BENCHMARK
	// -------------------------------------------------------------------------
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://ledger:ledger_password@127.0.0.1:5432/ledger?sslmode=disable"
	}

	pgConfig, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		fmt.Printf("Skipping Tier 2/3: cannot parse db URL: %v\n", err)
		return
	}
	pgConfig.MaxConns = 50
	pgPool, err := pgxpool.NewWithConfig(ctx, pgConfig)
	if err != nil || pgPool.Ping(ctx) != nil {
		fmt.Printf("Skipping Tier 2/3: PostgreSQL not reachable: %v\n", err)
		return
	}
	defer pgPool.Close()

	// Clean tables
	_, _ = pgPool.Exec(ctx, "TRUNCATE TABLE processed_events, fraud_evaluations, outbox_events, refunds, holds, entries, transfers, accounts CASCADE;")

	pgS := postgres.New(pgPool)
	fmt.Println("[RUNNING TIER 2] Real PostgreSQL Ledger (ACID Row-Locks & Outbox Inserts)...")
	pgNumAccounts := 50
	for i := 0; i < pgNumAccounts; i++ {
		_ = pgS.CreateAccount(ctx, ledger.Account{
			ID:       fmt.Sprintf("pg_wallet_%03d", i),
			Currency: money.CurrencyINR,
			Balance:  100000000,
		})
	}

	resPGOrd := runBenchmark("Tier 2: Real PostgreSQL", "Independent Account Transfers", "PostgreSQL 16 (pgxpool 50)", 1000, 20, func(workerID, reqIdx int) error {
		src := fmt.Sprintf("pg_wallet_%03d", reqIdx%pgNumAccounts)
		dst := fmt.Sprintf("pg_wallet_%03d", (reqIdx+1)%pgNumAccounts)
		key := fmt.Sprintf("pg_ord_tx_%d", reqIdx)
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
		dst := fmt.Sprintf("pg_wallet_%03d", (reqIdx%(pgNumAccounts-1))+1)
		key := fmt.Sprintf("pg_hot_tx_%d", reqIdx)
		_, err := pgS.ApplyTransfer(ctx, ledger.TransferRequest{
			IdempotencyKey: key,
			FromAccountID:  "pg_wallet_000",
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

	if rClient.Ping(ctx).Err() == nil {
		fmt.Println("[RUNNING TIER 3] Full HTTP Payment API (REST + Auth + Redis Fraud + PostgreSQL)...")
		rTracker := fraud.NewRedisVelocityTracker(rClient)
		pgAuditor := fraud.NewPostgresAuditStore(pgPool)
		fCfg := fraud.DefaultConfig()
		fCfg.FailClosed = true
		fraudEngine := fraud.NewEngine(fCfg, rTracker, pgAuditor)

		apiServer := api.NewServer(pgS, fraudEngine, pgAuditor)
		routerCfg := api.RouterConfig{
			AuthToken:      "benchmark-auth-token-2026",
			RateLimitRate:  100000, // disable rate limit bottleneck during loadtest
			RateLimitBurst: 100000,
			RequestTimeout: 10 * time.Second,
			Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		handler := api.NewRouter(apiServer, routerCfg)
		ts := httptest.NewServer(handler)
		defer ts.Close()

		httpClient := &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
			},
		}

		resHTTP := runBenchmark("Tier 3: HTTP API", "POST /api/v1/transfers (End-to-End)", "HTTP/1.1 + Redis + PG", 500, 20, func(workerID, reqIdx int) error {
			src := fmt.Sprintf("pg_wallet_%03d", reqIdx%pgNumAccounts)
			dst := fmt.Sprintf("pg_wallet_%03d", (reqIdx+1)%pgNumAccounts)
			key := fmt.Sprintf("http_tx_%d", reqIdx)

			payload, _ := json.Marshal(map[string]any{
				"idempotency_key": key,
				"from_account_id": src,
				"to_account_id":   dst,
				"amount":          50,
				"currency":        "INR",
			})

			req, _ := http.NewRequestWithContext(ctx, "POST", ts.URL+"/api/v1/transfers", bytes.NewReader(payload))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer benchmark-auth-token-2026")

			resp, err := httpClient.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
				return fmt.Errorf("unexpected status %d", resp.StatusCode)
			}
			return nil
		})
		allResults = append(allResults, resHTTP)
	}

	// -------------------------------------------------------------------------
	// 4. TIER 4: OUTBOX RELAY DISPATCH THROUGHPUT
	// -------------------------------------------------------------------------
	fmt.Println("[RUNNING TIER 4] Transactional Outbox Relay Batch Dispatch...")
	memPub := events.NewMemPublisher()
	relayCfg := outbox.Config{
		BatchSize:   100,
		MaxRetries:  3,
		TopicEvents: "payment.events",
		TopicDLQ:    "payment.dlq",
	}
	relay := outbox.NewRelay(pgPool, memPub, relayCfg)

	resOutbox := runBenchmark("Tier 4: Outbox Relay", "ProcessBatch Polling & Dispatch", "PostgreSQL outbox_events", 20, 1, func(workerID, reqIdx int) error {
		_, err := relay.ProcessBatch(ctx)
		return err
	})
	allResults = append(allResults, resOutbox)

	// -------------------------------------------------------------------------
	// PRINT HONEST BENCHMARK AUDIT TABLE
	// -------------------------------------------------------------------------
	fmt.Printf("\n%-24s | %-32s | %-24s | %-8s | %-12s | %-10s | %-10s | %-10s\n",
		"Tier", "Scenario", "Infrastructure", "Requests", "Throughput", "p50", "p95", "p99")
	fmt.Println("-------------------------------------------------------------------------------------------------------------------------------------------------------------")
	for _, r := range allResults {
		fmt.Printf("%-24s | %-32s | %-24s | %-8d | %8.1f rps | %10v | %10v | %10v\n",
			r.Tier, r.Scenario, r.Infrastructure, r.TotalRequests, r.ThroughputRPS, r.P50Latency, r.P95Latency, r.P99Latency)
	}
	fmt.Println("-------------------------------------------------------------------------------------------------------------------------------------------------------------")

	// Final Reconciliation Verification
	rep, err := pgS.Reconcile(ctx)
	if err != nil || !rep.IsClean {
		fmt.Printf("\n[ALERT] PostgreSQL Post-Benchmark Reconciliation Dirty: %+v, Err=%v\n", rep, err)
	} else {
		fmt.Printf("\n[VERIFIED] PostgreSQL Reconciliation 100%% Clean! Total Accounts=%d, Total Transfers=%d, Total Entries=%d, TotalEntriesSum=%d\n",
			rep.TotalAccounts, rep.TotalTransfers, rep.TotalEntries, rep.TotalEntriesSum)
	}
}
