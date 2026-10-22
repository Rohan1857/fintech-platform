// Command demo verifies the public API contract against a running local server.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Rohan1857/fintech-platform/internal/ledger"
	"github.com/Rohan1857/fintech-platform/internal/store"
	"github.com/google/uuid"
)

type demoClient struct {
	client         *http.Client
	baseURL, token string
}

func (c demoClient) request(ctx context.Context, method, path, key string, payload, result any, wantStatus int) error {
	var body bytes.Buffer
	if payload != nil {
		if err := json.NewEncoder(&body).Encode(payload); err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, &body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s %s: want HTTP %d, got %d: %s", method, path, wantStatus, resp.StatusCode, body)
	}
	if result != nil {
		return json.NewDecoder(resp.Body).Decode(result)
	}
	return nil
}

func runDemo(ctx context.Context, baseURL, token string, out io.Writer) error {
	c := demoClient{&http.Client{Timeout: 5 * time.Second}, strings.TrimRight(baseURL, "/"), token}
	prefix := "demo_" + uuid.NewString()[:8]
	buyer, seller := prefix+"_buyer", prefix+"_seller"
	var health struct {
		Status string `json:"status"`
	}
	if err := c.request(ctx, "GET", "/health", "", nil, &health, 200); err != nil {
		return err
	}
	if health.Status != "healthy" {
		return fmt.Errorf("unexpected health response: %+v", health)
	}
	for _, account := range []map[string]any{
		{"id": buyer, "currency": "INR", "initial_balance": 50000},
		{"id": seller, "currency": "INR", "initial_balance": 0},
	} {
		if err := c.request(ctx, "POST", "/api/v1/accounts", "", account, nil, 201); err != nil {
			return err
		}
	}
	fmt.Fprintln(out, "PASS  fresh buyer INR 500.00 / merchant INR 0.00")

	payment := map[string]any{"from_account_id": buyer, "to_account_id": seller, "amount": 15000, "currency": "INR"}
	var transfer ledger.Transfer
	if err := c.request(ctx, "POST", "/api/v1/transfers", prefix+"_pay", payment, &transfer, 200); err != nil {
		return err
	}
	if transfer.ID == "" || transfer.Amount != 15000 {
		return fmt.Errorf("invalid transfer: %+v", transfer)
	}
	for i := 0; i < 5; i++ {
		var replay ledger.Transfer
		if err := c.request(ctx, "POST", "/api/v1/transfers", prefix+"_pay", payment, &replay, 200); err != nil {
			return err
		}
		if replay.ID != transfer.ID {
			return fmt.Errorf("retry created a different transfer")
		}
	}
	payment["amount"] = 15001
	if err := c.request(ctx, "POST", "/api/v1/transfers", prefix+"_pay", payment, nil, 409); err != nil {
		return err
	}
	fmt.Fprintln(out, "PASS  INR 150.00 transfer; five retries return the same ID; changed payload returns 409")

	checkBalances := func(wantBuyer, wantHeld, wantSeller int64) error {
		for _, want := range []struct {
			id            string
			balance, held int64
		}{
			{buyer, wantBuyer, wantHeld}, {seller, wantSeller, 0},
		} {
			var account ledger.Account
			if err := c.request(ctx, "GET", "/api/v1/accounts/"+want.id, "", nil, &account, 200); err != nil {
				return err
			}
			if int64(account.Balance) != want.balance || int64(account.Held) != want.held {
				return fmt.Errorf("account %s: want balance=%d held=%d, got balance=%d held=%d", want.id, want.balance, want.held, account.Balance, account.Held)
			}
		}
		return nil
	}
	if err := checkBalances(35000, 0, 15000); err != nil {
		return err
	}
	var hold ledger.Hold
	payment["amount"] = 5000
	payment["expires_in_sec"] = 3600
	if err := c.request(ctx, "POST", "/api/v1/payments/authorize", prefix+"_hold", payment, &hold, 201); err != nil {
		return err
	}
	if hold.ID == "" {
		return fmt.Errorf("missing authorization hold ID")
	}
	if err := checkBalances(35000, 5000, 15000); err != nil {
		return err
	}
	fmt.Fprintln(out, "PASS  INR 50.00 hold reserves funds without settling them")

	var capture ledger.Transfer
	if err := c.request(ctx, "POST", "/api/v1/payments/capture", prefix+"_capture", map[string]any{"hold_id": hold.ID, "amount": 4000}, &capture, 200); err != nil {
		return err
	}
	if capture.ID == "" || capture.Amount != 4000 {
		return fmt.Errorf("invalid capture: %+v", capture)
	}
	if err := checkBalances(31000, 0, 19000); err != nil {
		return err
	}
	fmt.Fprintln(out, "PASS  INR 40.00 partial capture releases the remaining INR 10.00")

	if err := c.request(ctx, "POST", "/api/v1/payments/refund", prefix+"_refund", map[string]any{"transfer_id": capture.ID, "amount": 1000}, nil, 200); err != nil {
		return err
	}
	if err := c.request(ctx, "POST", "/api/v1/payments/refund", prefix+"_overrefund", map[string]any{"transfer_id": capture.ID, "amount": 4001}, nil, 422); err != nil {
		return err
	}
	payment["amount"] = 30000000
	if err := c.request(ctx, "POST", "/api/v1/transfers", prefix+"_risk", payment, nil, 403); err != nil {
		return err
	}
	if err := checkBalances(32000, 0, 18000); err != nil {
		return err
	}
	fmt.Fprintln(out, "PASS  INR 10.00 refund; over-refund and high-risk payment leave balances unchanged")

	var report store.ReconciliationReport
	if err := c.request(ctx, "POST", "/api/v1/reconciliation", "", nil, &report, 200); err != nil {
		return err
	}
	if !report.IsClean || report.TotalEntriesSum != 0 || len(report.UnbalancedTransfers) != 0 || len(report.MismatchedAccounts) != 0 {
		return fmt.Errorf("ledger does not reconcile: %+v", report)
	}
	fmt.Fprintln(out, "PASS  reconciliation: entry sum 0, no unbalanced transfers or account mismatches")
	fmt.Fprintln(out, "VERIFIED: buyer INR 320.00; merchant INR 180.00; held INR 0.00")
	return nil
}

func main() {
	baseURL := os.Getenv("BASE_URL")
	if baseURL == "" {
		baseURL = "http://localhost:8090"
	}
	token := os.Getenv("API_AUTH_TOKEN")
	if strings.TrimSpace(token) == "" {
		fmt.Fprintln(os.Stderr, "FAIL: set API_AUTH_TOKEN to the private token configured on the server")
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := runDemo(ctx, baseURL, token, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", err)
		os.Exit(1)
	}
}
