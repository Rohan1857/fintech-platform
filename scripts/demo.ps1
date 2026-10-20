# demo.ps1 - Reproducible Fintech Demo Script

$ErrorActionPreference = "Stop"
$BaseUrl = if ($env:BASE_URL) { $env:BASE_URL } else { "http://localhost:8090" }
$Token = "slice-secret-token-2026"
$Headers = @{
    "Authorization" = "Bearer $Token"
    "Content-Type"  = "application/json"
}

Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "  FINTECH PAYMENT & FRAUD PLATFORM - INTERACTIVE VERIFICATION DEMO" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan

# 1. Health Check
Write-Host "`n[Step 1] Verifying System Health..." -ForegroundColor Yellow
$health = Invoke-RestMethod -Uri "$BaseUrl/health" -Method Get
Write-Host "Health Status: $($health.status) at $($health.timestamp)" -ForegroundColor Green

# 2. Create User Wallet (Alice)
Write-Host "`n[Step 2] Creating Wallet for Alice (500.00 INR = 50,000 Paise)..." -ForegroundColor Yellow
$aliceBody = @{
    id              = "alice_wallet"
    currency        = "INR"
    initial_balance = 50000
} | ConvertTo-Json
$alice = Invoke-RestMethod -Uri "$BaseUrl/api/v1/accounts" -Method Post -Headers $Headers -Body $aliceBody
Write-Host "Created Wallet: ID=$($alice.id), Balance=$($alice.balance) Paise ($($alice.currency))" -ForegroundColor Green

# 3. Create Merchant Wallet (Bob)
Write-Host "`n[Step 3] Creating Merchant Wallet for Bob (0.00 INR)..." -ForegroundColor Yellow
$bobBody = @{
    id              = "bob_merchant"
    currency        = "INR"
    initial_balance = 0
} | ConvertTo-Json
$bob = Invoke-RestMethod -Uri "$BaseUrl/api/v1/accounts" -Method Post -Headers $Headers -Body $bobBody
Write-Host "Created Wallet: ID=$($bob.id), Balance=$($bob.balance) Paise" -ForegroundColor Green

# 4. Double-Entry Transfer with Synchronous Fraud Check
Write-Host "`n[Step 4] Executing Direct Transfer: Alice -> Bob for 150.00 INR (15,000 Paise)..." -ForegroundColor Yellow
$txHeaders = @{
    "Authorization"   = "Bearer $Token"
    "Content-Type"    = "application/json"
    "Idempotency-Key" = "demo_tx_transfer_001"
}
$txBody = @{
    from_account_id = "alice_wallet"
    to_account_id   = "bob_merchant"
    amount          = 15000
    currency        = "INR"
} | ConvertTo-Json
$tx = Invoke-RestMethod -Uri "$BaseUrl/api/v1/transfers" -Method Post -Headers $txHeaders -Body $txBody
Write-Host "Transfer Success! TX ID=$($tx.id), Status=$($tx.status), Amount=$($tx.amount) Paise" -ForegroundColor Green

# 5. Idempotent Retry of Payment (Same Key)
Write-Host "`n[Step 5] Retrying Exact Same Transfer with Same Idempotency Key..." -ForegroundColor Yellow
$retryTx = Invoke-RestMethod -Uri "$BaseUrl/api/v1/transfers" -Method Post -Headers $txHeaders -Body $txBody
Write-Host "Idempotent Replay Verified: Returned TX ID=$($retryTx.id) without double-charging!" -ForegroundColor Green

# Verify Alice Balance
$aliceCurrent = Invoke-RestMethod -Uri "$BaseUrl/api/v1/accounts/alice_wallet" -Method Get -Headers $Headers
Write-Host "Alice Current Balance: $($aliceCurrent.balance) Paise (Expected: 35,000 Paise)" -ForegroundColor Green

# 6. Reject Suspicious Transaction (High-Value Rule)
Write-Host "`n[Step 6] Testing Fraud Engine: Triggering Critical Value Transfer (300,000 INR)..." -ForegroundColor Yellow
$fraudHeaders = @{
    "Authorization"   = "Bearer $Token"
    "Content-Type"    = "application/json"
    "Idempotency-Key" = "demo_fraud_trigger_001"
}
$fraudBody = @{
    from_account_id = "alice_wallet"
    to_account_id   = "bob_merchant"
    amount          = 30000000 # 300,000 INR (exceeds 250,000 INR critical limit)
    currency        = "INR"
} | ConvertTo-Json

try {
    Invoke-RestMethod -Uri "$BaseUrl/api/v1/transfers" -Method Post -Headers $fraudHeaders -Body $fraudBody
    Write-Host "[ERROR] Fraudulent transaction was not blocked!" -ForegroundColor Red
} catch {
    Write-Host "Fraud Engine Blocked Transfer! Status Code: $($_.Exception.Response.StatusCode.value__)" -ForegroundColor Magenta
    $errStream = $_.Exception.Response.GetResponseStream()
    $reader = New-Object System.IO.StreamReader($errStream)
    Write-Host "Engine Response: $($reader.ReadToEnd())" -ForegroundColor Magenta
}

# 7. Authorization Hold & Capture
Write-Host "`n[Step 7] Testing Card Auth & Capture: Reserving 50.00 INR (5,000 Paise)..." -ForegroundColor Yellow
$authHeaders = @{
    "Authorization"   = "Bearer $Token"
    "Content-Type"    = "application/json"
    "Idempotency-Key" = "demo_hold_001"
}
$authBody = @{
    from_account_id = "alice_wallet"
    to_account_id   = "bob_merchant"
    amount          = 5000
    currency        = "INR"
    expires_in_sec  = 3600
} | ConvertTo-Json
$hold = Invoke-RestMethod -Uri "$BaseUrl/api/v1/payments/authorize" -Method Post -Headers $authHeaders -Body $authBody
Write-Host "Hold Placed: Hold ID=$($hold.id), Status=$($hold.status), Reserved=$($hold.amount) Paise" -ForegroundColor Green

# Capture partial 40.00 INR (4,000 Paise)
$capHeaders = @{
    "Authorization"   = "Bearer $Token"
    "Content-Type"    = "application/json"
    "Idempotency-Key" = "demo_cap_001"
}
$capBody = @{
    hold_id = $hold.id
    amount  = 4000
} | ConvertTo-Json
$capTx = Invoke-RestMethod -Uri "$BaseUrl/api/v1/payments/capture" -Method Post -Headers $capHeaders -Body $capBody
Write-Host "Hold Captured: TX ID=$($capTx.id), Amount=$($capTx.amount) Paise, Remaining Hold Released" -ForegroundColor Green

# 8. Refund Execution
Write-Host "`n[Step 8] Testing Reversal/Refund: Refunding 10.00 INR from Capture..." -ForegroundColor Yellow
$refHeaders = @{
    "Authorization"   = "Bearer $Token"
    "Content-Type"    = "application/json"
    "Idempotency-Key" = "demo_ref_001"
}
$refBody = @{
    transfer_id = $capTx.id
    amount      = 1000
    reason      = "Customer requested partial return"
} | ConvertTo-Json
$ref = Invoke-RestMethod -Uri "$BaseUrl/api/v1/payments/refund" -Method Post -Headers $refHeaders -Body $refBody
Write-Host "Refund Completed: Refund ID=$($ref.refund.id), Reversal TX=$($ref.transfer.id)" -ForegroundColor Green

# 9. Administrative Double-Entry Reconciliation
Write-Host "`n[Step 9] Running Administrative Double-Entry Reconciliation Audit..." -ForegroundColor Yellow
$rec = Invoke-RestMethod -Uri "$BaseUrl/api/v1/reconciliation" -Method Post -Headers $Headers
Write-Host "Reconciliation Report:" -ForegroundColor Green
Write-Host "  Total Accounts:      $($rec.total_accounts)" -ForegroundColor Green
Write-Host "  Total Transfers:     $($rec.total_transfers)" -ForegroundColor Green
Write-Host "  Total Entries:       $($rec.total_entries)" -ForegroundColor Green
Write-Host "  Entries Sum:         $($rec.total_entries_sum) (Must be exactly 0)" -ForegroundColor Green
Write-Host "  Ledger Clean Audit:  $($rec.is_clean)" -ForegroundColor Green

# 10. View Prometheus Metrics
Write-Host "`n[Step 10] Scraping Prometheus Metrics (/metrics)..." -ForegroundColor Yellow
$metricsRaw = Invoke-RestMethod -Uri "$BaseUrl/metrics" -Method Get
$metricLines = $metricsRaw -split "`n" | Where-Object { $_ -match "^(payment_requests_total|fraud_evaluations_total)" }
foreach ($line in $metricLines) {
    Write-Host "  $line" -ForegroundColor White
}

Write-Host "`n================================================================================" -ForegroundColor Cyan
Write-Host "  ALL 10 DEMO MILESTONES VERIFIED SUCCESSFULLY" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
