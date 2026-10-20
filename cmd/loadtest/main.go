package main

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/messi/fintech-platform/internal/fraud"
	"github.com/messi/fintech-platform/internal/ledger"
	"github.com/messi/fintech-platform/internal/store/memstore"
	"github.com/messi/fintech-platform/pkg/money"
)

type BenchmarkResult struct {
	Scenario      string
	TotalRequests int
	SuccessCount  int
	FailCount     int
	Duration      time.Duration
	ThroughputRPS float64
	P50Latency    time.Duration
	P95Latency    time.Duration
	P99Latency    time.Duration
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

func runBenchmark(name string, totalReqs int, concurrency int, runFn func(workerID, reqIdx int) error) BenchmarkResult {
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
		Scenario:      name,
		TotalRequests: totalReqs,
		SuccessCount:  int(successCount.Load()),
		FailCount:     int(failCount.Load()),
		Duration:      duration,
		ThroughputRPS: rps,
		P50Latency:    p50,
		P95Latency:    p95,
		P99Latency:    p99,
	}
}

func main() {
	ctx := context.Background()
	s := memstore.New()
	tracker := fraud.NewMemVelocityTracker()
	auditor := fraud.NewMemAuditStore()
	cfg := fraud.DefaultConfig()
	fraudEngine := fraud.NewEngine(cfg, tracker, auditor)

	fmt.Println("================================================================================")
	fmt.Println("  FINTECH PAYMENT & FRAUD PLATFORM - REPRODUCIBLE LOAD TEST BENCHMARK")
	fmt.Println("================================================================================")

	// Pre-seed 100 accounts
	numAccounts := 100
	for i := 0; i < numAccounts; i++ {
		_ = s.CreateAccount(ctx, ledger.Account{
			ID:       fmt.Sprintf("wallet_%03d", i),
			Currency: money.CurrencyINR,
			Balance:  10000000, // 100,000 INR
		})
	}

	// 1. Scenario 1: Ordinary Sequential / Independent Transfers
	res1 := runBenchmark("Scenario 1: Ordinary Independent Transfers", 1000, 10, func(workerID, reqIdx int) error {
		src := fmt.Sprintf("wallet_%03d", reqIdx%numAccounts)
		dst := fmt.Sprintf("wallet_%03d", (reqIdx+1)%numAccounts)
		key := fmt.Sprintf("ord_tx_%d", reqIdx)

		// Fraud Evaluation
		eval, err := fraudEngine.Evaluate(ctx, fraud.EvaluationRequest{
			IdempotencyKey: key,
			AccountID:      src,
			BeneficiaryID:  dst,
			Amount:         1000,
			Currency:       money.CurrencyINR,
		})
		if err != nil || eval.Decision == fraud.DecisionReject {
			return fmt.Errorf("fraud rejected")
		}

		// Ledger Mutation
		_, err = s.ApplyTransfer(ctx, ledger.TransferRequest{
			IdempotencyKey: key,
			FromAccountID:  src,
			ToAccountID:    dst,
			Amount:         1000,
			Currency:       money.CurrencyINR,
		})
		return err
	})

	// 2. Scenario 2: Concurrent Transfers on Hot Account (Contention Stress)
	hotAccount := "wallet_000"
	res2 := runBenchmark("Scenario 2: Concurrent Hot-Account Transfers", 1000, 20, func(workerID, reqIdx int) error {
		dest := fmt.Sprintf("wallet_%03d", (reqIdx%99)+1)
		key := fmt.Sprintf("hot_tx_%d", reqIdx)

		_, err := s.ApplyTransfer(ctx, ledger.TransferRequest{
			IdempotencyKey: key,
			FromAccountID:  hotAccount,
			ToAccountID:    dest,
			Amount:         500,
			Currency:       money.CurrencyINR,
		})
		return err
	})

	// 3. Scenario 3: 100% Duplicate Payment Replays (Idempotency Cache Stress)
	sharedKeys := make([]string, 50)
	for k := range sharedKeys {
		sharedKeys[k] = fmt.Sprintf("shared_key_%d", k)
		// Pre-apply once
		_, _ = s.ApplyTransfer(ctx, ledger.TransferRequest{
			IdempotencyKey: sharedKeys[k],
			FromAccountID:  "wallet_001",
			ToAccountID:    "wallet_002",
			Amount:         100,
			Currency:       money.CurrencyINR,
		})
	}

	res3 := runBenchmark("Scenario 3: 100% Duplicate Retries (Idempotency Replays)", 2000, 20, func(workerID, reqIdx int) error {
		chosenKey := sharedKeys[rand.Intn(len(sharedKeys))]
		_, err := s.ApplyTransfer(ctx, ledger.TransferRequest{
			IdempotencyKey: chosenKey,
			FromAccountID:  "wallet_001",
			ToAccountID:    "wallet_002",
			Amount:         100,
			Currency:       money.CurrencyINR,
		})
		return err
	})

	// Print Results Table
	results := []BenchmarkResult{res1, res2, res3}
	fmt.Printf("\n%-50s | %-8s | %-12s | %-10s | %-10s | %-10s\n", "Scenario", "Requests", "Throughput", "p50", "p95", "p99")
	fmt.Println("----------------------------------------------------------------------------------------------------------------")
	for _, r := range results {
		fmt.Printf("%-50s | %-8d | %8.1f rps | %10v | %10v | %10v\n",
			r.Scenario, r.TotalRequests, r.ThroughputRPS, r.P50Latency, r.P95Latency, r.P99Latency)
	}

	// Verify ledger reconciliation after all load tests
	rep, err := s.Reconcile(ctx)
	if err != nil || !rep.IsClean {
		fmt.Printf("\n[ERROR] Reconciliation failed after benchmarks! Clean=%v, Err=%v\n", rep.IsClean, err)
	} else {
		fmt.Printf("\n[SUCCESS] Post-Benchmark Reconciliation Audit: 100%% Clean. Total Entries Sum = %d minor units.\n", rep.TotalEntriesSum)
	}
}
