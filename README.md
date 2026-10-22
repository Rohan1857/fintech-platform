# fintech-platform

A high-throughput, double-entry payment ledger and synchronous fraud detection engine written in Go.

The project handles the core invariants required by modern payment infrastructure: strictly balanced double-entry mutations, deterministic row locking to prevent deadlocks under high concurrency, sub-millisecond fraud velocity checks in Redis, and a transactional outbox pattern to eliminate distributed dual-write inconsistencies.

---

## Architecture Overview

```
                            [ Client / Mobile / API Consumer ]
                                            │
                                            ▼
                           ┌─────────────────────────────────┐
                           │   REST API Gateway & Security   │
                           │ (Bearer Auth, Token Bucket Rate)│
                           └────────────────┬────────────────┘
                                            │
                                            ▼
                           ┌─────────────────────────────────┐
                           │ Synchronous Fraud Risk Engine   │
                           │  - High/Critical value checks   │
                           │  - Redis sliding window (ZSET)  │
                           │  - Fail-closed security policy  │
                           └────────────────┬────────────────┘
                                            │ [If APPROVE]
                                            ▼
                           ┌─────────────────────────────────┐
                           │    Double-Entry Ledger Core     │
                           │  - Integer minor math (Paise)   │
                           │  - Sorted row locks (Deadlocks) │
                           │  - Aggregated leg validation    │
                           │  - Idempotency key evaluation   │
                           └────────────────┬────────────────┘
                                            │ (Single ACID Tx)
                                            ▼
                           ┌─────────────────────────────────┐
                           │      PostgreSQL 16 Storage      │
                           │   accounts, transfers, entries  │
                           │   holds, refunds, outbox_events │
                           └────────────────┬────────────────┘
                                            │
                      Async Outbox Relay    │ (Reliable batch polling)
                                            ▼
                           ┌─────────────────────────────────┐
                           │          Apache Kafka           │
                           │   Topics: payment.events, DLQ   │
                           └────────────────┬────────────────┘
                                            │
                                            ▼
                           ┌─────────────────────────────────┐
                           │       Idempotent Consumer       │
                           │  - Deduplication via DB table   │
                           │  - Downstream analytics & DLQ   │
                           └─────────────────────────────────┘
```

---

## Core Financial & Engineering Guarantees

### 1. Integer Minor Units & Balanced Postings
Floating-point math is banned. Money is tracked strictly in integer minor units (`type Money int64`, e.g. 1 INR = 100 Paise). Every transfer consists of two or more entries that must sum to exactly zero:

$$\sum_{i=1}^{n} \text{entry}_i = 0$$

An account has a settled `Balance` and reserved `Held` amount. Available funds are computed as `Balance - Held` and are guarded at the database level by PostgreSQL constraints (`CHECK (balance >= held)` and `CHECK (balance >= 0)`).

### 2. Deterministic Row Locking (Deadlock Prevention)
When transferring between accounts, circular deadlocks (e.g. Account A $\to$ Account B concurrent with Account B $\to$ Account A) are eliminated by sorting unique account IDs lexicographically before acquiring `SELECT ... FOR UPDATE` row locks inside the database transaction.

### 3. Aggregated Multi-Leg Validation (Upstream Bug Fix)
In multi-leg split transfers, naive ledger implementations validate balances per leg sequentially. If an account has multiple debit legs, sequential validation can permit an overdraft before earlier legs are deducted. This engine aggregates net impact by account upfront via `AggregatePostings`:

$$\Delta_{\text{net}}(a) = \sum_{e \in \text{legs}(a)} e.\text{Amount}$$

Available balance checks and state mutations are performed on the aggregated delta.

### 4. Synchronous Pre-Flight Fraud Evaluation & Fail-Closed Policy
Fraud checks run synchronously **before** money moves. The engine checks:
- **High/Critical Thresholds**: Evaluates transaction size against configurable limits (e.g. flag for review at ₹50,000, reject at ₹250,000).
- **Redis Sliding-Window Velocity**: Tracks transaction count and volume using Redis sorted sets (`ZSET`) scored by millisecond epoch timestamps.
- **Beneficiary Frequency**: Detects rapid repeated transfers to the same recipient.
- **Fail-Closed Safety**: If Redis or the risk engine is unreachable, the system activates its fail-closed policy (`FailClosed: true`), rejecting or reviewing the transaction rather than silently moving money without security checks.

### 5. Transactional Outbox (Zero Dual-Writes)
Domain events (`payment.created`, `payment.completed`, `payment.failed`, `payment.refunded`, `fraud.flagged`) are written to the `outbox_events` table inside the **exact same PostgreSQL transaction** as the financial ledger entries. An asynchronous relay worker polls unpublished outbox entries and publishes them to Apache Kafka with exponential backoff and Dead-Letter Queue (DLQ) routing for poisoned payloads.

Downstream consumers check the `processed_events` table to ensure idempotent processing across replays and network retries.

---

## Measured Performance Benchmarks

Benchmarks were recorded using the included harness (`cmd/loadtest/main.go`) on Linux amd64 (20 CPUs). Rather than conflating in-memory microbenchmarks with database numbers, results are disaggregated by execution tier:

| Tier | Scenario | Infrastructure | Requests | Concurrency | Throughput | p50 Latency | p95 Latency | p99 Latency |
|---|---|---|---|---|---|---|---|
| **In-Memory** | Independent Transfers | RAM (`sync.Mutex`) | 2,000 | 20 workers | **262,430 RPS** | 1.9 µs | 0.66 ms | 1.66 ms |
| **In-Memory** | Contended Hot-Account | RAM (`sync.Mutex`) | 2,000 | 20 workers | **298,845 RPS** | 1.9 µs | 0.74 ms | 1.26 ms |
| **PostgreSQL 16** | Independent Transfers | `pgxpool` (50 conns) | 1,000 | 20 workers | **2,801 RPS** | **4.93 ms** | **18.86 ms** | **30.12 ms** |
| **PostgreSQL 16** | Contended Hot-Account | `pgxpool` (50 conns) | 500 | 10 workers | **728 RPS** | **10.41 ms** | **32.34 ms** | **62.74 ms** |
| **HTTP REST API** | `POST /api/v1/transfers` | HTTP + Redis + PG | 500 | 20 workers | **8,151 RPS** | **0.76 ms** | **2.95 ms** | **40.29 ms** |
| **Outbox Relay** | Batch Polling & Dispatch | Postgres `outbox_events` | 20 batches | 1 worker | **28.4 batches/s** | **44.30 ms** | **60.55 ms** | **60.55 ms** |

> **Post-Benchmark Reconciliation Audit**: After running the full load test suite against PostgreSQL, the automated reconciliation engine verified:  
> `Total Accounts: 51 | Total Transfers: 1,550 | Total Entries: 3,100 | TotalEntriesSum: 0 Paise | Mismatches: 0 | Clean: true`

---

## Quickstart

### Prerequisites
- Go 1.24+ (or Go 1.26)
- Docker & Docker Compose

### 1. Run Everything via Docker Compose
```bash
# Clone the repository
git clone https://github.com/Rohan1857/fintech-platform.git
cd fintech-platform

# Boot up the full stack (API, PostgreSQL, Redis, Kafka, Prometheus, Grafana)
docker compose up -d --build
```

Services exposed:
- **API Gateway**: `http://localhost:8080`
- **PostgreSQL 16**: `localhost:5432` (User: `ledger`, DB: `ledger`)
- **Redis 7**: `localhost:6379`
- **Apache Kafka 3.7**: `localhost:9092`
- **Prometheus**: `http://localhost:9090`
- **Grafana**: `http://localhost:3000` (User: `admin`, Pass: `admin`)

### 2. Run the Verification Demo
To see the full lifecycle (wallet creation, transfers, idempotent replays, fraud blocking, authorization holds, partial captures, refunds, and reconciliation):

```bash
# Linux / macOS
chmod +x scripts/demo.sh
./scripts/demo.sh

# Windows PowerShell
powershell -ExecutionPolicy Bypass -File scripts/demo.ps1
```

### 3. Run the Benchmarking Tool
```bash
go run ./cmd/loadtest/main.go
```

### 4. Run the Test Suite with Race Detection
```bash
go test -race -v ./...
```

---

## API Reference & Usage

All financial endpoints require the `Authorization: Bearer <API_AUTH_TOKEN>` header (default local token: `slice-secret-token-2026`) and an `Idempotency-Key` header.

### Create Wallet
```bash
curl -X POST http://localhost:8080/api/v1/accounts \
  -H "Authorization: Bearer slice-secret-token-2026" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "wallet_alice",
    "currency": "INR",
    "initial_balance": 50000
  }'
```

### Direct Transfer
```bash
curl -X POST http://localhost:8080/api/v1/transfers \
  -H "Authorization: Bearer slice-secret-token-2026" \
  -H "Idempotency-Key: tx_req_1001" \
  -H "Content-Type: application/json" \
  -d '{
    "from_account_id": "wallet_alice",
    "to_account_id": "wallet_bob",
    "amount": 15000,
    "currency": "INR"
  }'
```

### Two-Step Card Payment (Authorize & Capture)
```bash
# 1. Authorize hold (reserves available balance)
curl -X POST http://localhost:8080/api/v1/payments/authorize \
  -H "Authorization: Bearer slice-secret-token-2026" \
  -H "Idempotency-Key: hold_req_2001" \
  -H "Content-Type: application/json" \
  -d '{
    "from_account_id": "wallet_alice",
    "to_account_id": "merchant_bob",
    "amount": 5000,
    "currency": "INR",
    "expires_in_sec": 3600
  }'

# 2. Capture partial or full amount (releases uncaptured hold balance)
curl -X POST http://localhost:8080/api/v1/payments/capture \
  -H "Authorization: Bearer slice-secret-token-2026" \
  -H "Idempotency-Key: cap_req_2001" \
  -H "Content-Type: application/json" \
  -d '{
    "hold_id": "<HOLD_ID>",
    "amount": 4000
  }'
```

### Refund Transfer
```bash
curl -X POST http://localhost:8080/api/v1/payments/refund \
  -H "Authorization: Bearer slice-secret-token-2026" \
  -H "Idempotency-Key: ref_req_3001" \
  -H "Content-Type: application/json" \
  -d '{
    "transfer_id": "<TRANSFER_ID>",
    "amount": 1000,
    "reason": "customer return"
  }'
```

### Administrative Ledger Reconciliation
```bash
curl -X POST http://localhost:8080/api/v1/reconciliation \
  -H "Authorization: Bearer slice-secret-token-2026"
```

Response:
```json
{
  "total_accounts": 3,
  "total_transfers": 4,
  "total_entries": 8,
  "total_balance_minor": 0,
  "total_entries_sum": 0,
  "is_clean": true
}
```

---

## Repository Layout

```
.
├── cmd/
│   ├── loadtest/            # Multi-tier benchmarking harness
│   └── server/              # Production API server binary
├── deploy/
│   ├── grafana/             # Pre-configured Grafana dashboard JSON
│   └── prometheus/          # Prometheus scraping configuration
├── docs/
│   ├── api_examples.sh      # Executable curl examples
│   ├── architecture.md      # Detailed system invariants and diagrams
│   └── openapi.yaml         # Complete OpenAPI 3.0 specification
├── internal/
│   ├── api/                 # REST routing, auth, rate limiting, and handlers
│   ├── events/              # Event types, Kafka publisher, and idempotent consumer
│   ├── fraud/               # Risk engine, rules DSL, and Redis sliding-window tracker
│   ├── ledger/              # Core domain models, aggregation, and balancing validation
│   ├── metrics/             # Prometheus latency histograms and counters
│   ├── outbox/              # Transactional outbox batch relay and DLQ router
│   └── store/
│       ├── memstore/        # In-memory storage implementation for local dev
│       └── postgres/        # Production PostgreSQL storage via pgx/v5
├── migrations/              # SQL schema migrations (idempotent constraints)
├── pkg/
│   └── money/               # Money type, minor unit math, formatting
├── scripts/
│   ├── demo.sh              # Bash verification demo script
│   └── demo.ps1             # PowerShell verification demo script
├── docker-compose.yml       # Full stack containerization
└── AUDIT_REPORT.md          # Independent production readiness & adversarial audit
```

---

## Upstream Attribution & Design References

This repository builds upon ideas from two open-source projects:
1. **[mkmbhs/ledger](https://github.com/mkmbhs/ledger)** (Pinned commit `b9309ce2`, MIT License): Reference data models and PostgreSQL row-locking concepts.
   - *Key modifications*: Resolved a per-leg balance validation vulnerability by implementing net account aggregation (`AggregatePostings`). Added authorization holds (`AuthorizeHold`), partial captures (`CaptureHold`), automatic hold expiration (`ExpireHolds`), and refund reversals (`RefundTransfer`).
2. **[apache261/Byakugan-core](https://github.com/apache261/Byakugan-core)** (Pinned commit `67a5adba`, Apache 2.0 License): Fraud rule evaluation concepts.
   - *Key modifications*: Implemented an original Go fraud evaluation engine with Redis sliding-window sorted sets (`ZSET`), multi-factor scoring, and an explicit fail-closed safety policy.

License texts are preserved in [THIRD_PARTY_LICENSES](THIRD_PARTY_LICENSES).

---

## License

This project is licensed under the [MIT License](LICENSE).
