#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8090}"
TOKEN="slice-secret-token-2026"
AUTH_HEADER="Authorization: Bearer ${TOKEN}"

echo "================================================================================"
echo "  FINTECH PAYMENT & FRAUD PLATFORM - INTERACTIVE VERIFICATION DEMO"
echo "================================================================================"

echo -e "\n[Step 1] Verifying System Health..."
curl -s "${BASE_URL}/health" | jq .

echo -e "\n[Step 2] Creating Alice Wallet (500.00 INR = 50,000 Paise)..."
curl -s -X POST "${BASE_URL}/api/v1/accounts" \
  -H "${AUTH_HEADER}" \
  -H "Content-Type: application/json" \
  -d '{"id":"alice_wallet","currency":"INR","initial_balance":50000}' | jq .

echo -e "\n[Step 3] Creating Bob Merchant Wallet (0.00 INR)..."
curl -s -X POST "${BASE_URL}/api/v1/accounts" \
  -H "${AUTH_HEADER}" \
  -H "Content-Type: application/json" \
  -d '{"id":"bob_merchant","currency":"INR","initial_balance":0}' | jq .

echo -e "\n[Step 4] Executing Direct Transfer (150.00 INR)..."
curl -s -X POST "${BASE_URL}/api/v1/transfers" \
  -H "${AUTH_HEADER}" \
  -H "Idempotency-Key: demo_tx_transfer_001" \
  -H "Content-Type: application/json" \
  -d '{"from_account_id":"alice_wallet","to_account_id":"bob_merchant","amount":15000,"currency":"INR"}' | jq .

echo -e "\n[Step 5] Retrying Transfer with Same Idempotency Key (No Double Charge)..."
curl -s -X POST "${BASE_URL}/api/v1/transfers" \
  -H "${AUTH_HEADER}" \
  -H "Idempotency-Key: demo_tx_transfer_001" \
  -H "Content-Type: application/json" \
  -d '{"from_account_id":"alice_wallet","to_account_id":"bob_merchant","amount":15000,"currency":"INR"}' | jq .

echo -e "\n[Step 6] Testing Fraud Engine: Triggering Critical Value Transfer (300,000 INR)..."
curl -s -i -X POST "${BASE_URL}/api/v1/transfers" \
  -H "${AUTH_HEADER}" \
  -H "Idempotency-Key: demo_fraud_trigger_001" \
  -H "Content-Type: application/json" \
  -d '{"from_account_id":"alice_wallet","to_account_id":"bob_merchant","amount":30000000,"currency":"INR"}' || true

echo -e "\n[Step 7] Testing Card Auth & Capture: Reserving 50.00 INR..."
HOLD_RESP=$(curl -s -X POST "${BASE_URL}/api/v1/payments/authorize" \
  -H "${AUTH_HEADER}" \
  -H "Idempotency-Key: demo_hold_001" \
  -H "Content-Type: application/json" \
  -d '{"from_account_id":"alice_wallet","to_account_id":"bob_merchant","amount":5000,"currency":"INR","expires_in_sec":3600}')
echo "${HOLD_RESP}" | jq .
HOLD_ID=$(echo "${HOLD_RESP}" | jq -r .id)

echo -e "\n[Step 8] Capturing Partial Hold (40.00 INR)..."
CAP_RESP=$(curl -s -X POST "${BASE_URL}/api/v1/payments/capture" \
  -H "${AUTH_HEADER}" \
  -H "Idempotency-Key: demo_cap_001" \
  -H "Content-Type: application/json" \
  -d "{\"hold_id\":\"${HOLD_ID}\",\"amount\":4000}")
echo "${CAP_RESP}" | jq .
TRANSFER_ID=$(echo "${CAP_RESP}" | jq -r .id)

echo -e "\n[Step 9] Refunding 10.00 INR from Captured Hold..."
curl -s -X POST "${BASE_URL}/api/v1/payments/refund" \
  -H "${AUTH_HEADER}" \
  -H "Idempotency-Key: demo_ref_001" \
  -H "Content-Type: application/json" \
  -d "{\"transfer_id\":\"${TRANSFER_ID}\",\"amount\":1000,\"reason\":\"partial return\"}" | jq .

echo -e "\n[Step 10] Running Administrative Reconciliation Audit..."
curl -s -X POST "${BASE_URL}/api/v1/reconciliation" \
  -H "${AUTH_HEADER}" | jq .

echo -e "\n[Step 11] Viewing Prometheus Metrics..."
curl -s "${BASE_URL}/metrics" | grep -E "^(payment_requests_total|fraud_evaluations_total)" || true
