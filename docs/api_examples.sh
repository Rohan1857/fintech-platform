#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8090}"
TOKEN="${API_AUTH_TOKEN:?Set API_AUTH_TOKEN to the private token configured on the server}"
AUTH_HEADER="Authorization: Bearer ${TOKEN}"

echo "=== 1. Health Check ==="
curl -s -X GET "${BASE_URL}/health" | jq .

echo -e "\n=== 2. Create User Wallet (Alice) ==="
curl -s -X POST "${BASE_URL}/api/v1/accounts" \
  -H "${AUTH_HEADER}" \
  -H "Content-Type: application/json" \
  -d '{"id":"alice_wallet","currency":"INR","initial_balance":500000}' | jq .

echo -e "\n=== 3. Create Merchant Wallet (Bob) ==="
curl -s -X POST "${BASE_URL}/api/v1/accounts" \
  -H "${AUTH_HEADER}" \
  -H "Content-Type: application/json" \
  -d '{"id":"bob_merchant","currency":"INR","initial_balance":0}' | jq .

echo -e "\n=== 4. Direct Transfer (Alice -> Bob: 25.00 INR = 2500 paise) ==="
curl -s -X POST "${BASE_URL}/api/v1/transfers" \
  -H "${AUTH_HEADER}" \
  -H "Idempotency-Key: demo_transfer_001" \
  -H "Content-Type: application/json" \
  -d '{"from_account_id":"alice_wallet","to_account_id":"bob_merchant","amount":2500,"currency":"INR"}' | jq .

echo -e "\n=== 5. Idempotent Retry of Transfer (Same Key) ==="
curl -s -X POST "${BASE_URL}/api/v1/transfers" \
  -H "${AUTH_HEADER}" \
  -H "Idempotency-Key: demo_transfer_001" \
  -H "Content-Type: application/json" \
  -d '{"from_account_id":"alice_wallet","to_account_id":"bob_merchant","amount":2500,"currency":"INR"}' | jq .

echo -e "\n=== 6. High-Value Fraud Detection (Rejected) ==="
curl -s -X POST "${BASE_URL}/api/v1/transfers" \
  -H "${AUTH_HEADER}" \
  -H "Idempotency-Key: demo_fraud_001" \
  -H "Content-Type: application/json" \
  -d '{"from_account_id":"alice_wallet","to_account_id":"bob_merchant","amount":30000000,"currency":"INR"}' | jq .

echo -e "\n=== 7. Authorize Hold (100.00 INR) ==="
HOLD_RESP=$(curl -s -X POST "${BASE_URL}/api/v1/payments/authorize" \
  -H "${AUTH_HEADER}" \
  -H "Idempotency-Key: demo_hold_001" \
  -H "Content-Type: application/json" \
  -d '{"from_account_id":"alice_wallet","to_account_id":"bob_merchant","amount":10000,"currency":"INR","expires_in_sec":3600}')
echo "${HOLD_RESP}" | jq .
HOLD_ID=$(echo "${HOLD_RESP}" | jq -r .id)

echo -e "\n=== 8. Capture Partial Hold (80.00 INR) ==="
CAP_RESP=$(curl -s -X POST "${BASE_URL}/api/v1/payments/capture" \
  -H "${AUTH_HEADER}" \
  -H "Idempotency-Key: demo_cap_001" \
  -H "Content-Type: application/json" \
  -d "{\"hold_id\":\"${HOLD_ID}\",\"amount\":8000}")
echo "${CAP_RESP}" | jq .
TRANSFER_ID=$(echo "${CAP_RESP}" | jq -r .id)

echo -e "\n=== 9. Refund Payment (30.00 INR) ==="
curl -s -X POST "${BASE_URL}/api/v1/payments/refund" \
  -H "${AUTH_HEADER}" \
  -H "Idempotency-Key: demo_refund_001" \
  -H "Content-Type: application/json" \
  -d "{\"transfer_id\":\"${TRANSFER_ID}\",\"amount\":3000,\"reason\":\"partial refund demo\"}" | jq .

echo -e "\n=== 10. Check Bob Balance ==="
curl -s -X GET "${BASE_URL}/api/v1/accounts/bob_merchant" \
  -H "${AUTH_HEADER}" | jq .

echo -e "\n=== 11. Run Administrative Reconciliation Audit ==="
curl -s -X POST "${BASE_URL}/api/v1/reconciliation" \
  -H "${AUTH_HEADER}" | jq .
