package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Rohan1857/fintech-platform/internal/api"
	"github.com/Rohan1857/fintech-platform/internal/fraud"
	"github.com/Rohan1857/fintech-platform/internal/store/memstore"
)

func TestDemo_RepeatablePaymentLifecycle(t *testing.T) {
	s := memstore.New()
	auditor := fraud.NewMemAuditStore()
	engine := fraud.NewEngine(fraud.DefaultConfig(), fraud.NewMemVelocityTracker(), auditor)
	cfg := api.DefaultRouterConfig()
	cfg.AuthToken = "demo-test"
	server := httptest.NewServer(api.NewRouter(api.NewServer(s, engine, auditor), cfg))
	defer server.Close()
	for i := 0; i < 2; i++ {
		if err := runDemo(context.Background(), server.URL, cfg.AuthToken, io.Discard); err != nil {
			t.Fatal(err)
		}
	}
	report, err := s.Reconcile(context.Background())
	if err != nil || !report.IsClean || report.TotalTransfers != 8 || report.TotalEntriesSum != 0 {
		t.Fatalf("demo lifecycle did not reconcile: %+v err=%v", report, err)
	}
}

func TestDemo_RejectsUnexpectedServerResponses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{}`)
	}))
	defer server.Close()
	if err := runDemo(context.Background(), server.URL, "demo-test", io.Discard); err == nil {
		t.Fatal("demo reported success without verifying server responses")
	}
}
