package main

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/Rohan1857/fintech-platform/internal/api"
	"github.com/Rohan1857/fintech-platform/internal/ledger"
	"github.com/Rohan1857/fintech-platform/internal/store/memstore"
)

func TestRunBenchmark_RejectionsAreNotPaymentThroughput(t *testing.T) {
	r := runBenchmark("test", "all rejected", "memory", 20, 2, func(int, int) error {
		return errors.New("request rejected")
	})
	if r.SuccessCount != 0 || r.FailCount != 20 || r.ThroughputRPS != 0 {
		t.Fatalf("failed attempts inflated completed throughput: %+v", r)
	}
	if r.P50Latency != 0 || r.P95Latency != 0 || r.P99Latency != 0 {
		t.Fatalf("failed attempts included in completed-payment latencies: %+v", r)
	}
}

func TestRunBenchmark_CountsMixedResults(t *testing.T) {
	r := runBenchmark("test", "mixed", "memory", 10, 2, func(_, i int) error {
		if i%2 == 0 {
			return errors.New("rejected")
		}
		return nil
	})
	if r.SuccessCount != 5 || r.FailCount != 5 || r.TotalRequests != 10 {
		t.Fatalf("incorrect outcome counts: %+v", r)
	}
}

func TestHTTPBenchmark_CompletesRealPayment(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	for _, account := range []ledger.Account{
		{ID: "buyer", Currency: "INR", Balance: 10000},
		{ID: "seller", Currency: "INR"},
	} {
		if err := s.CreateAccount(ctx, account); err != nil {
			t.Fatal(err)
		}
	}
	cfg := api.DefaultRouterConfig()
	cfg.AuthToken = "benchmark-test"
	server := httptest.NewServer(api.NewRouter(api.NewServer(s, nil, nil), cfg))
	defer server.Close()
	if err := postHTTPTransfer(ctx, server.Client(), server.URL, "benchmark-test", "payment-1", "buyer", "seller", 50); err != nil {
		t.Fatal(err)
	}
	seller, err := s.GetAccount(ctx, "seller")
	if err != nil || seller.Balance != 50 {
		t.Fatalf("benchmark did not settle payment: %+v err=%v", seller, err)
	}
	if err := postHTTPTransfer(ctx, server.Client(), server.URL, "benchmark-test", "", "buyer", "seller", 50); err == nil {
		t.Fatal("benchmark counted missing-header rejection as a payment")
	}
}
