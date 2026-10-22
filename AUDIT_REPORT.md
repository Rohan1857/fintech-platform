# Local verification report

**Date:** 22 October 2026
**Scope:** portfolio correctness regressions, test isolation, reproducible demo, and benchmark reporting
**Environment:** WSL2 Ubuntu 24.04, Linux amd64, 20 reported logical CPUs; Go 1.26.0; PostgreSQL 16.15; Redis 7.0.15

This is an implementation-session verification record, **not an independent security audit or production approval**. It supersedes the previous report's broader claims.

## What changed and why

| Customer-visible issue | Evidence before the fix | Verified behavior after the fix |
|---|---|---|
| Authorization reserved money on REVIEW | API regression returned 201 and an active hold | 202 assessment; no hold, transfer, or balance mutation |
| Committed transfer/hold retries failed when risk went offline | Regression returned 403 on the same committed key | Same committed response returned; new intent still blocked; retry adds no velocity count |
| Velocity write failure silently approved a payment | Read-only tracker regression returned APPROVE | Configured REJECT/REVIEW result with fail-closed score and assessment |
| Capture key could refer to another hold | Shared contract returned nil error in memory and PostgreSQL | Conflicting hold rejected; original replay retains the capture ID and funds |
| Rejections inflated payment throughput | All-rejected workload reported nonzero completed/s | Zero completed/s and no completed-payment latency samples |
| Demo printed success without proving balances | Old scripts displayed responses with fixed IDs | Shared Go demo asserts statuses, IDs, balances, reservations and reconciliation; repeated runs use fresh IDs |

The fixes were preceded by failing regression runs. An initial REVIEW fixture also triggered the default aggregate-amount reject rule; it was corrected to reproduce REVIEW using the default beneficiary/burst rules before implementation.

## Commands and results

Integration verification used a separately created disposable database, `fintech_slice_verify_20261022`, and a separate Redis process bound to `127.0.0.1:16380`. The existing application database and Redis instance were not used for fixture resets.

```bash
export DATABASE_URL='postgres://ledger:ledger_password@127.0.0.1:5432/fintech_slice_verify_20261022?sslmode=disable'
export REDIS_ADDR='127.0.0.1:16380'
go test -race -p 1 -count=1 -timeout 5m ./...
go vet ./...
go build ./cmd/server ./cmd/demo ./cmd/loadtest
gofmt -l .
```

Results: full race suite passed; vet and builds exited 0; formatting listed no files. Package serialization is deliberate: existing integration fixtures reset shared tables. All PostgreSQL test packages now honor the explicit `DATABASE_URL`, including consumer tests that previously hardcoded the application database.

Both `bash scripts/demo.sh` under WSL and `scripts/demo.ps1` under Windows ran against the same local server backed by real PostgreSQL and Redis. Each completed with:

```text
PASS  fresh buyer INR 500.00 / merchant INR 0.00
PASS  INR 150.00 transfer; five retries return the same ID; changed payload returns 409
PASS  INR 50.00 hold reserves funds without settling them
PASS  INR 40.00 partial capture releases the remaining INR 10.00
PASS  INR 10.00 refund; over-refund and high-risk payment leave balances unchanged
PASS  reconciliation: entry sum 0, no unbalanced transfers or account mismatches
VERIFIED: buyer INR 320.00; merchant INR 180.00; held INR 0.00
```

The live demo server used explicit development fallback for its event publisher only; its connection logs confirmed PostgreSQL and Redis connections. Kafka publishing was represented by the in-process adapter.

## Measured workload

`go run ./cmd/loadtest` exited 0 against the same isolated services. PostgreSQL pool limit: 50. Setup was outside measurement. Latencies include successful operations only; failed counts are reported separately.

| Scenario | Attempts | Workers | Completed | Failed | Completed/s | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Memory wallet cycle | 2,000 | 20 | 2,000 | 0 | 272,423.4 | 1.846 us | 819.660 us | 1.459 ms |
| Memory hot sender | 2,000 | 20 | 2,000 | 0 | 311,638.2 | 1.771 us | 314.512 us | 1.335 ms |
| PostgreSQL wallet cycle | 1,000 | 20 | 1,000 | 0 | 3,704.5 | 4.678 ms | 11.539 ms | 16.197 ms |
| PostgreSQL hot sender | 500 | 10 | 500 | 0 | 756.0 | 10.390 ms | 30.001 ms | 45.038 ms |
| HTTP + auth + Redis risk + PostgreSQL | 500 | 20 | 500 | 0 | 2,569.1 | 6.736 ms | 9.363 ms | 31.417 ms |

The wallet cycle uses 50 accounts and can contend on accounts. The HTTP scenario uses 500 fresh sender/recipient pairs, one transfer per sender, with default fraud rules active and HTTP rate limits raised. These are different short local workloads, not controlled comparisons or a sustained production capacity estimate.

Post-run reconciliation reported entry sum 0 and no mismatches. Counts include pre-existing test fixtures in this disposable database; the benchmark no longer clears tables. Unique names prevent cross-run idempotent replays from masquerading as fresh payments.

The old **8,151 HTTP RPS** result is withdrawn. Its request omitted `Idempotency-Key`, and the summary counted all requests without showing success/failure counts. Empty outbox polling has also been removed from the throughput table.

## Not established by this verification

- Live Kafka broker publishing, rebalancing, crash recovery, or exactly-once handler effects. Publisher adapters were used in outbox tests and the live demo.
- A running full Docker Compose stack. Docker's Linux engine pipe was unavailable in this session; PostgreSQL and Redis ran natively in WSL instead.
- Production security, tenant/account authorization, financial arithmetic overflow protection, bank/card integration, or a compliance audit guarantee.
- Durable review/approval workflow, scheduled hold expiry, uniform idempotency semantics for every operation, or atomic fraud reservations across concurrent new intents.
- Sustained capacity, multi-instance rate limiting, readiness/recovery, backup/restore, or fault tolerance under real network partitions.

See [the design's next decisions](docs/architecture.md#boundaries-and-next-decisions) for the corresponding implementation milestones. The demonstrated scope is a testable payment backend and an explicit engineering case study.
