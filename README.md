# Payments Ledger & Risk Engine

A Go backend exploring a practical checkout problem: **a payment must move money once, preserve the customer’s spendable balance, and remain explainable when requests are retried or dependencies fail.**

Wallet transfers, authorization holds, partial capture, refunds, synchronous risk checks, and reconciliation share one ledger. PostgreSQL is the financial source of truth; Redis tracks recent payment attempts; a transactional outbox decouples committed payments from event publishing.

[![Verification](https://github.com/Rohan1857/fintech-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/Rohan1857/fintech-platform/actions/workflows/ci.yml)

**Scope:** an engineering portfolio project and local testbed. It does not connect to a bank, card network, or real customer funds. [Verification evidence and boundaries](AUDIT_REPORT.md).

## Start with the customer behavior

| Situation | Required behavior | Where to inspect |
|---|---|---|
| The app times out after a successful payment | Return the committed transfer on retry; do not charge or rerun risk checks | [API regression tests](internal/api/payment_safety_test.go) |
| Two requests compete for the last available rupees | Serialize balance mutations and reject overspending | [PostgreSQL concurrency tests](internal/store/postgres/store_integration_test.go) |
| A merchant captures less than authorized | Settle the captured amount and release the remainder | [Ledger implementation](internal/store/postgres/store.go), [demo](cmd/demo/main.go) |
| A retry changes the amount or capture’s hold | Reject conflicting intent without changing funds | [Capture contract across both stores](internal/store/capture_contract_test.go) |
| Risk returns REVIEW | Return an assessment with HTTP 202; create no transfer or hold | [API safety tests](internal/api/payment_safety_test.go) |
| Risk storage becomes unavailable or read-only | Apply the configured fail-closed outcome before funds move | [Write-failure regression](internal/fraud/write_failure_test.go) |
| A refund exceeds the remaining refundable amount | Reject it and preserve balances | [Payment lifecycle tests](internal/store/postgres/store_integration_test.go) |
| Several authorizations expire on one wallet | Release their combined amount once and preserve live holds | [Shared expiry tests](internal/store/safety_contract_test.go) |
| A client supplies a negative balance or internal account ID | Reject the request without creating an account or moving reserve funds | [API input tests](internal/api/account_safety_test.go), [database constraints](internal/store/postgres/account_constraints_test.go) |

## Review in five minutes

1. Run the [demo](#run-the-demo): inspect actual balances after retries, capture, and refund.
2. Read [the design](docs/architecture.md): requirements, invariants, transaction boundaries, and tradeoffs.
3. Inspect [payment safety regressions](internal/api/payment_safety_test.go) and [the posting transaction](internal/store/postgres/store.go).
4. Read [verification results](AUDIT_REPORT.md), including what the suite does not establish.

## Architecture

```mermaid
flowchart TD
    Client[Checkout / API client] --> API[Go HTTP API: auth, validation, replay lookup]
    API --> Risk[Risk assessment: Redis attempt velocity]
    Risk -->|APPROVE| Ledger[Ledger transaction]
    Risk -->|REVIEW or REJECT| Assessment[Assessment only: no funds mutation]
    Ledger --> DB[(PostgreSQL: accounts, entries, holds, refunds, outbox)]
    DB --> Relay[Outbox relay]
    Relay --> Kafka[Kafka publisher / consumer adapters]
```

A matching **committed** transfer or hold is returned before a fresh risk assessment. This is an optimization for completed retries; store transactions and unique keys remain the final financial concurrency guard. Risk assessment and ledger commit are separate operations, with the consequences described in [the design](docs/architecture.md#risk-is-an-assessment-not-a-reservation).

## Run the demo

Requires Go **1.26+** and an explicitly configured private `API_AUTH_TOKEN`. Set the same token in the server and demo terminals. The former published demo token is rejected, including in development mode. The fastest path uses explicit in-memory development mode:

```bash
export API_AUTH_TOKEN="$(openssl rand -hex 32)"
ALLOW_IN_MEMORY_DEV_MODE=true HTTP_PORT=8090 go run ./cmd/server
```

PowerShell:

```powershell
$env:API_AUTH_TOKEN = [guid]::NewGuid().ToString("N")
$env:ALLOW_IN_MEMORY_DEV_MODE = "true"
$env:HTTP_PORT = "8090"
go run ./cmd/server
```

In another terminal, from the repository root, export the **same private token** as `API_AUTH_TOKEN` before running the demo:

```bash
go run ./cmd/demo
# Existing wrappers also work:
bash scripts/demo.sh
# PowerShell: .\scripts\demo.ps1
```

The demo checks HTTP statuses, transfer identity, balances, held funds, rejected mutations, and reconciliation. Every run uses fresh account IDs. Any mismatch exits nonzero.

```text
PASS  fresh buyer INR 500.00 / merchant INR 0.00
PASS  INR 150.00 transfer; five retries return the same ID; changed payload returns 409
PASS  INR 50.00 hold reserves funds without settling them
PASS  INR 40.00 partial capture releases the remaining INR 10.00
PASS  INR 10.00 refund; over-refund and high-risk payment leave balances unchanged
PASS  reconciliation: entry sum 0, no unbalanced transfers or account mismatches
VERIFIED: buyer INR 320.00; merchant INR 180.00; held INR 0.00
```

This path demonstrates the API and payment lifecycle in memory. PostgreSQL and Redis correctness are exercised separately by integration tests; do not interpret the development demo as durable storage.

Set `BASE_URL` and `API_AUTH_TOKEN` for another local server. The server defaults to port **8090**; the existing Docker Compose mapping uses **8080**. Set `BASE_URL=http://localhost:8080` when targeting Compose.

## Verify with PostgreSQL and Redis

Use a **disposable** PostgreSQL database and Redis instance. Integration fixtures truncate financial tables; `DATABASE_URL` and `REDIS_ADDR` are required explicitly. Run packages serially because fixtures share the database.

For the repository’s existing Compose service definitions:

```bash
export API_AUTH_TOKEN="$(openssl rand -hex 32)"
docker compose up -d postgres redis
export DATABASE_URL='postgres://ledger:ledger_password@127.0.0.1:5432/ledger?sslmode=disable'
export REDIS_ADDR='127.0.0.1:6379'
# Compose applies both migrations only when its PostgreSQL volume is first initialized.
go test -race -p 1 -count=1 -timeout 5m ./...
go vet ./...
go build ./cmd/server ./cmd/demo ./cmd/loadtest
```

For an independently provisioned empty database, apply `migrations/*.up.sql` in filename order with `psql -v ON_ERROR_STOP=1` first. Existing databases need `000002_restrict_system_accounts.up.sql`; restarting Compose does not migrate existing volumes. The upgrade changes constraints without deleting or repairing account data. Invalid legacy system-account rows cause a transaction rollback and require operator review before retrying. The equivalent PowerShell environment syntax is `$env:DATABASE_URL = "..."` and `$env:REDIS_ADDR = "127.0.0.1:6379"`.

The full Compose stack also defines Kafka and observability services. It was **not exercised** in the latest local verification; see [operational limits](AUDIT_REPORT.md#not-established-by-this-verification). The published CI workflow provisions fresh PostgreSQL/Redis services and uses the same serial race-test command.

## Performance: successful payments, with context

One local run on 22 October 2026: WSL2 Linux amd64, Go 1.26.0, 20 reported logical CPUs, PostgreSQL 16, Redis 7, pool limit 50. These are short local measurements, not a sustained capacity claim. Setup is excluded; completed/s counts successful operations only, and latency percentiles cover successful operations only. A separate run after the account/expiry fixes is recorded in [the verification report](AUDIT_REPORT.md#account-and-expiry-follow-up-23-october-2026).

| Scenario | Requests / workers | Completed / failed | Completed/s | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|---:|
| PostgreSQL account transfers | 1,000 / 20 | 1,000 / 0 | 3,704.5 | 4.68 ms | 11.54 ms | 16.20 ms |
| PostgreSQL hot sender | 500 / 10 | 500 / 0 | 756.0 | 10.39 ms | 30.00 ms | 45.04 ms |
| HTTP + auth + default Redis risk + PostgreSQL | 500 / 20 | 500 / 0 | 2,569.1 | 6.74 ms | 9.36 ms | 31.42 ms |

The first database workload cycles through 50 wallets, so some account contention remains. The HTTP workload uses **500 fresh sender/recipient pairs with one payment per sender**, keeping default fraud rules active without deliberately triggering velocity limits. It is a different workload from the store-only scenarios. HTTP rate limiting is raised for this load test.

```bash
# With DATABASE_URL and REDIS_ADDR pointing to disposable services:
go run ./cmd/loadtest
```

The harness creates uniquely named accounts without truncating the database, prints success/failure counts, reconciles the ledger, and exits nonzero on failed operations or reconciliation. It also reports separately labelled in-memory microbenchmarks. The old **8,151 HTTP RPS** claim is withdrawn: the previous HTTP workload omitted the required header, and its summary did not expose rejected requests. Empty outbox polling is no longer presented as event delivery throughput.

## API essentials

Client-created account IDs cannot start with `system_`, and initial balances must be non-negative. System reserve accounts are used only for internal opening entries; public transfers, authorizations, captures, and refunds cannot move reserve funds.

The service sweeps expired holds on startup and every 30 seconds. An attempted capture of an expired hold also releases its reservation atomically before returning the expiry outcome.

Amounts are integer minor units: **100 paise = INR 1**. Financial mutations require the `Idempotency-Key` header; putting the key only in JSON does not satisfy the contract. Examples assume the two demo accounts have been created.

```bash
curl -sS http://localhost:8090/api/v1/transfers \
  -H "Authorization: Bearer ${API_AUTH_TOKEN}" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: checkout-001' \
  -d '{"from_account_id":"buyer","to_account_id":"seller","amount":15000,"currency":"INR"}'
```

| Endpoint | Purpose |
|---|---|
| `POST /api/v1/accounts` | Create/fund a local demo wallet |
| `GET /api/v1/accounts/{id}` | Settled and held balances |
| `GET /api/v1/accounts/{id}/history` | Ledger entries |
| `POST /api/v1/transfers` | Direct transfer with risk assessment |
| `POST /api/v1/payments/authorize` | Reserve funds after risk approval |
| `POST /api/v1/payments/capture` | Capture once; release unused authorization |
| `POST /api/v1/payments/void` | Release an active authorization |
| `POST /api/v1/payments/refund` | Reverse funds within the refundable amount |
| `POST /api/v1/reconciliation` | Detect account/entry mismatches |
| `GET /api/v1/fraud/evaluations/{id}` | Read a risk assessment |

`202 pending_review` reports a risk assessment; there is **no durable approval queue or resume endpoint**. [OpenAPI](docs/openapi.yaml) describes request schemas. See [design boundaries](docs/architecture.md#boundaries-and-next-decisions) for idempotency and expiry limits.

## Repository map

```text
cmd/server/        Service entrypoint and dependency wiring
cmd/demo/          Repeatable HTTP payment lifecycle assertions
cmd/loadtest/      Success-counted store and HTTP workloads
internal/api/      Routes, middleware, request handling and regression tests
internal/ledger/   Domain types, balanced posting and matching rules
internal/store/    Store contract, memory/PostgreSQL implementations and tests
internal/fraud/    Scoring rules, Redis tracking and assessment persistence
internal/outbox/   PostgreSQL outbox polling and publishing
internal/events/   Kafka adapters and consumer bookkeeping
migrations/       Database schema
scripts/          Cross-platform demo entrypoints
```

## Attribution

Ledger data modelling and row-locking concepts reference [mkmbhs/ledger](https://github.com/mkmbhs/ledger), pinned at `b9309ce2` (MIT). Fraud evaluation concepts reference [apache261/Byakugan-core](https://github.com/apache261/Byakugan-core), pinned at `67a5adba` (Apache 2.0). This repository adds its Go implementation, aggregated posting validation, holds/capture/refunds, risk tracking, outbox integration, and regression tests. Upstream references are not a claim that every idea originated here. License notices are retained in [THIRD_PARTY_LICENSES](THIRD_PARTY_LICENSES).

[MIT License](LICENSE).
