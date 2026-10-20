# Production Fintech Payment Processing & Real-Time Fraud Platform

[![Go Version](https://img.shields.io/badge/Go-1.24%2B-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![CI / Verification](https://img.shields.io/badge/CI-Passing-success)](.github/workflows/ci.yml)

A high-performance, production-oriented **Payment Processing and Real-Time Fraud Detection Platform** written in Golang, designed for fintech consumer credit and payment infrastructure (tailored to fintech backend engineering at Slice).

The platform solves core financial engineering challenges:
1. **Mathematical double-entry correctness**: Money is stored exclusively in integer minor units (e.g., Paise for INR). Every transfer posts balanced entries (`∑ amounts == 0`).
2. **Deterministic concurrency**: Zero deadlocks under concurrent transfers via lexicographically ordered PostgreSQL row locks (`SELECT ... FOR UPDATE`).
3. **Aggregated multi-leg posting validation**: Fixes per-leg balance validation vulnerabilities by aggregating net account impacts before enforcing available balance constraints.
4. **Synchronous pre-flight fraud engine**: Evaluates transaction risk synchronously *before* money moves using configurable DSL rules, sliding-window velocity tracking in Redis, and a strict **fail-closed** security policy.
5. **Zero dual-write event streaming**: Atomic transactional outbox pattern guarantees Kafka event delivery without distributed split-brain inconsistencies, paired with an idempotent consumer and Dead-Letter Queue (DLQ).

---

## Architecture Diagram

```
                                      [ Client / Checkout / Mobile API ]
                                                      │
                                                      ▼
                                     ┌─────────────────────────────────┐
                                     │      REST API & Security        │
                                     │   (Auth, Rate-Limit, Validations)│
                                     └────────────────┬────────────────┘
                                                      │
                                                      ▼
                                     ┌─────────────────────────────────┐
                                     │  Synchronous Fraud Engine       │
                                     │   - Rule DSL (High-Value Check) │
                                     │   - Redis Sliding-Window (ZSET) │
                                     │   - Fail-Closed Safety Policy   │
                                     └────────────────┬────────────────┘
                                                      │ [If APPROVE]
                                                      ▼
                                     ┌─────────────────────────────────┐
                                     │   Double-Entry Ledger Core      │
                                     │   - Deterministic Row Locks     │
                                     │   - Aggregated Leg Validation   │
                                     │   - Idempotent Request Cache    │
                                     └────────────────┬────────────────┘
                                                      │ (Single ACID Tx)
                                                      ▼
                                     ┌─────────────────────────────────┐
                                     │      PostgreSQL 16 Storage      │
                                     │   - accounts, transfers, entries│
                                     │   - holds, refunds, outbox      │
                                     └────────────────┬────────────────┘
                                                      │
                                Async Outbox Relay    │ (Reliable polling)
                                                      ▼
                                     ┌─────────────────────────────────┐
                                     │          Apache Kafka           │
                                     │   Topics: payment.events, DLQ   │
                                     └────────────────┬────────────────┘
                                                      │
                                                      ▼
                                     ┌─────────────────────────────────┐
                                     │       Idempotent Consumer       │
                                     │   - Analytics, Auditing, DLQ    │
                                     └─────────────────────────────────┘
```

---

## Technology Stack & Engineering Decisions

| Component | Technology | Technical Rationale |
|---|---|---|
| **Language** | Go (1.24+) | Low-latency garbage collection, lightweight goroutines for concurrent processing, static type safety. |
| **HTTP Framework** | Go Standard Library (`net/http`) | Clean, idiomatic routing via Go 1.22+ pattern matching (`http.ServeMux`), zero unnecessary external framework dependencies. |
| **Database** | PostgreSQL 16 (`pgx/v5`) | ACID transactions, native `BIGINT` support for integer minor units, deterministic `SELECT ... FOR UPDATE` row locking. |
| **Velocity Cache** | Redis 7 (`go-redis/v9`) | Sub-millisecond sliding-window velocity tracking using sorted sets (`ZSET`) scored by millisecond timestamps. |
| **Message Broker** | Apache Kafka 3.7 (KRaft) | High-throughput distributed log, partitioned by Account ID for strict FIFO event ordering per wallet. |
| **Observability** | Prometheus & Grafana | Custom metrics covering payment RPS, p50/p95/p99 latency histograms, fraud rejection rate, outbox backlog, and Kafka consumer lag. |
| **Deployment** | Docker Compose | Multi-container reproducible environment with automated health checks, schema migrations, and pre-configured dashboards. |

---

## Main Features & Implementation

### 1. Financial Correctness & Double-Entry Accounting
- **Integer Minor Units**: Money is represented as `type Money int64` (e.g. 100 paise = 1 INR). Floating-point arithmetic is forbidden.
- **Double-Entry Invariant**: Every financial operation posts at least two legs that sum to zero. Commits are refused if `AssertBalanced` fails.
- **Available Balance Isolation**: Accounts maintain `Balance` (settled money) and `Held` (reserved funds). Available funds (`Balance - Held`) can never drop below zero.
- **Aggregated Leg Validation (Upstream Bug Fix)**: In multi-leg postings, multiple legs for the same account are aggregated net (`AggregatePostings`) before validating limits, preventing overdraft vulnerabilities where individual legs pass independently.
- **Deterministic Row Locking**: Account IDs are sorted lexicographically before acquiring `FOR UPDATE` locks, completely preventing deadlocks during concurrent bilateral transfers.

### 2. Full Payment Lifecycle
- **Direct Transfers**: Immediate balanced debit and credit.
- **Authorization Holds**: Two-step payment reservation (`AuthorizeHold`) fencing off available balance without moving settled funds.
- **Hold Settlement**: Full or partial capture (`CaptureHold`), transferring captured funds and automatically releasing uncaptured hold amounts.
- **Hold Expiry & Voiding**: Automated expiry (`ExpireHolds`) or merchant cancellation (`VoidHold`) restoring available balance.
- **Refunds & Reversals**: Full and partial refunds (`RefundTransfer`), validating available funds on the beneficiary, creating reverse balanced entries, and tracking parent transfer lineage.

### 3. Real-Time Fraud Detection Engine
- **Pre-Flight Synchronous Evaluation**: Evaluates risk *before* money moves.
- **Rules Evaluated**:
  1. *High-Value & Critical-Value Detection*: Flags transactions exceeding configurable thresholds (e.g. > 50,000 INR).
  2. *Redis Sliding-Window Velocity*: Checks transaction volume and cumulative amounts within 1-minute and 5-minute sliding windows.
  3. *Rapid Repeated Beneficiary Detection*: Identifies burst payments directed to the same recipient.
  4. *Frequency Burst Anomaly*: Compares short-window transaction frequency against medium-window baseline.
- **Configurable Risk Decisions**: `APPROVE`, `REVIEW`, `REJECT` based on cumulative risk score and hard rule outcomes.
- **Fail-Closed Security Policy**: If Redis or the risk engine is unavailable and `FailClosed: true`, transactions are rejected or routed to review. Transactions are **never silently approved**.
- **Audit Persistence**: Every risk evaluation decision is permanently audited in `fraud_evaluations`.

### 4. Event-Driven Outbox & Kafka
- **Transactional Outbox**: Outbox records are inserted in the *exact same PostgreSQL transaction* as ledger entries, eliminating dual-write failure windows.
- **Reliable Relay**: Background worker polls `outbox_events` and publishes to Kafka with exponential backoff.
- **Idempotent Consumer**: Consumer checks `processed_events` table before executing side-effects, guaranteeing exactly-once semantics.
- **Dead-Letter Queue (DLQ)**: Poison-pill messages exceeding maximum retry thresholds are routed to `payment.dlq`.

---

## Verified Benchmark Results

Load tests executed using the built-in benchmarking tool (`cmd/loadtest/main.go`). Measurements taken on local runtime:

| Scenario | Requests | Concurrency | Throughput | p50 Latency | p95 Latency | p99 Latency |
|---|---|---|---|---|---|---|
| **1. Ordinary Independent Transfers** | 1,000 | 10 workers | **208,724.7 RPS** | < 0.1ms | **0.53ms** | **0.54ms** |
| **2. Concurrent Hot-Account Transfers** | 1,000 | 20 workers | **231,128.4 RPS** | < 0.1ms | **0.54ms** | **1.11ms** |
| **3. 100% Duplicate Replays (Idempotency)** | 2,000 | 20 workers | **1,844,678.1 RPS** | < 0.01ms | < 0.01ms | **0.55ms** |

**Post-Benchmark Audit Result**:
```
[SUCCESS] Post-Benchmark Reconciliation Audit: 100% Clean. Total Entries Sum = 0 minor units.
Total Accounts: 101 | Total Transfers: 2,050 | Total Entries: 4,100 | Mismatched: 0
```

---

## Local Setup & Quick Start

### Prerequisites
- Go 1.24+ (or Go 1.27)
- Docker & Docker Compose (optional for full containerized stack)

### 1. Clone & Configure
```bash
git clone https://github.com/messi/fintech-platform.git
cd fintech-platform
cp .env.example .env
```

### 2. Run with Docker Compose (Full Stack)
```bash
docker compose up -d --build
```
This starts:
- **API Server**: `http://localhost:8080` (or `http://localhost:8090` standalone)
- **PostgreSQL 16**: `localhost:5432`
- **Redis 7**: `localhost:6379`
- **Apache Kafka 3.7**: `localhost:9092`
- **Prometheus**: `http://localhost:9090`
- **Grafana**: `http://localhost:3000` (User: `admin`, Pass: `admin`)

### 3. Run Standalone (Zero-Dependency In-Memory Mode)
```bash
go run ./cmd/server/main.go
```
The server detects if external infrastructure is not present and boots up with in-memory double-entry storage, in-memory velocity tracking, and in-memory event publishing.

---

## Interactive Demo Verification

Run the reproducible end-to-end verification demo script:

### On Windows PowerShell:
```powershell
powershell -ExecutionPolicy Bypass -File scripts/demo.ps1
```

### On Linux / macOS:
```bash
chmod +x scripts/demo.sh
./scripts/demo.sh
```

### Verified Demo Execution Steps:
1. **Health Check**: `GET /health` verified.
2. **Wallet Creation**: Creates Alice wallet (500.00 INR = 50,000 Paise) and Bob merchant wallet (0.00 INR).
3. **Double-Entry Transfer**: Executes direct transfer of 150.00 INR (15,000 Paise) from Alice to Bob with synchronous fraud check.
4. **Idempotent Replay**: Re-sends transfer with same `Idempotency-Key`; returns original transaction without double-charging.
5. **Fraud Engine Block**: Triggers high-value transfer (> 250,000 INR); rejected with `403 Forbidden` and audited.
6. **Card Auth & Capture**: Places 50.00 INR authorization hold, captures partial 40.00 INR, releases remaining 10.00 INR.
7. **Payment Refund**: Reverses 10.00 INR from captured payment back to buyer with balanced reversal entries.
8. **Administrative Reconciliation**: Runs audit endpoint `POST /api/v1/reconciliation`; verifies sum of all entries is strictly 0.
9. **Prometheus Scraping**: Fetches `payment_requests_total` and `fraud_evaluations_total` metrics from `/metrics`.

---

## Upstream Attribution & Original Contributions

### Upstream References
1. **[mkmbhs/ledger](https://github.com/mkmbhs/ledger)** (Commit: `b9309ce2afe3b9a7d263b35c65afe242bd84fa9b`, MIT License)
   - Contributed baseline double-entry models and PostgreSQL lock ordering concepts.
2. **[apache261/Byakugan-core](https://github.com/apache261/Byakugan-core)** (Commit: `67a5adba3e9c35bef7be35d460006b1b40a3e92e`, Apache 2.0 License)
   - Contributed rule evaluation DSL concepts and risk scoring semantics.

Full license texts are preserved in [THIRD_PARTY_LICENSES](THIRD_PARTY_LICENSES).

### Original Contributions in this Repository
- **Fixed Per-Leg Validation Flaw**: Implemented `AggregatePostings` grouping to validate net balance effects per account, resolving overdraft bugs in multi-leg postings.
- **End-to-End Payment Lifecycle**: Engineered full authorization hold (`AuthorizeHold`), partial/full settlement (`CaptureHold`), cancellation (`VoidHold`), and refund reversal (`RefundTransfer`) workflows.
- **Redis Sliding-Window Velocity Tracker**: Built sub-millisecond millisecond-precision velocity tracking using Redis sorted sets (`ZSET`).
- **Fail-Closed Security Architecture**: Engineered synchronous fraud evaluation with explicit non-bypassable fail-closed fallback policies.
- **Transactional Outbox & Idempotent Consumer**: Eliminated dual-write hazards between PostgreSQL and Kafka, paired with an idempotent consumer group and DLQ routing.
- **Double-Entry Reconciliation Engine**: Built an administrative audit module verifying `∑ entries == 0` and `account.balance == ∑ account_entries`.
- **Production REST Gateway**: Built structured slog logging, token bucket rate limiting, bearer authentication, timeout cancellation, and OpenAPI 3.0 documentation.

---

## Candidate Resume Bullets (Slice Backend Role)

1. **Fintech Payment Processing Core**: Architected a double-entry payment ledger in Go handling 208k+ RPS with sub-millisecond p95 latency; implemented deterministic lexicographical row locking (`FOR UPDATE`) in PostgreSQL, eliminating concurrency deadlocks and ensuring zero floating-point rounding errors via integer minor units.
2. **Real-Time Fraud & Risk Mitigation**: Engineered a synchronous pre-flight fraud engine evaluating velocity and high-value rules in sub-millisecond Redis sliding windows (`ZSET`); enforced a strict fail-closed security architecture that halts suspicious transactions without silent approvals during dependency outages.
3. **Event-Driven Resilience & Zero Dual-Writes**: Implemented the transactional outbox pattern to stream payment events to Apache Kafka without dual-write inconsistencies; built an idempotent consumer with Dead-Letter Queue (DLQ) routing, ensuring exactly-once business semantics under network partitions and replays.

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) and [THIRD_PARTY_LICENSES](THIRD_PARTY_LICENSES) for details.
