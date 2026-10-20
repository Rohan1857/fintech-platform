# Architecture and Implementation Plan

## 1. System Overview

Fintech Payment Processing and Real-Time Fraud Detection Platform built in Go for high-throughput, low-latency transaction processing with double-entry correctness.

```
                                  [ Client / Checkout ]
                                            │
                                            ▼
                           ┌─────────────────────────────────┐
                           │   REST API Gateway & Security   │
                           │ (Auth, Rate-Limit, Validations) │
                           └────────────────┬────────────────┘
                                            │
                                            ▼
                           ┌─────────────────────────────────┐
                           │ Real-Time Fraud Engine (Sync)   │
                           │  - Byakugan DSL Rules           │
                           │  - Redis Sliding-Window Velocity│
                           │  - Fail-Closed Safety Policy    │
                           └────────────────┬────────────────┘
                                            │ [If APPROVE]
                                            ▼
                           ┌─────────────────────────────────┐
                           │   Double-Entry Ledger Engine    │
                           │  - Atomic Balance Mutations     │
                           │  - Deterministic Row Locks      │
                           │  - Aggregated Leg Validation    │
                           │  - Idempotent Replays           │
                           └────────────────┬────────────────┘
                                            │ (Single ACID Tx)
                                            ▼
                           ┌─────────────────────────────────┐
                           │   PostgreSQL Storage & Outbox   │
                           │  - Accounts, Entries, Transfers │
                           │  - Holds, Outbox Events         │
                           │  - Fraud Audit Records          │
                           └────────────────┬────────────────┘
                                            │
                      Async Outbox Relay    │ (Reliable polling)
                                            ▼
                           ┌─────────────────────────────────┐
                           │          Apache Kafka           │
                           │    Topics: payment.events, DLQ  │
                           └────────────────┬────────────────┘
                                            │
                                            ▼
                           ┌─────────────────────────────────┐
                           │       Idempotent Consumer       │
                           │  - Analytics, Audits, Tracing   │
                           │  - Exactly-once domain handling │
                           └─────────────────────────────────┘
```

## 2. Key Technical Invariants

1. **Monetary Representation**: Integer minor units (e.g. Paise for INR: 100 paise = 1 INR). Never floating point.
2. **Double-Entry Balance**: Every transfer consists of at least two entries summing to 0 (`∑ amounts == 0`).
3. **Atomic Mutations & No Negative Spend**: Available balance = `Balance - Held`. Available balance can never drop below zero.
4. **Aggregated Multi-Leg Postings**: In multi-leg postings, multiple legs for the same account are summed net before balance checks to prevent overspend or false rejections.
5. **Deterministic Deadlock Prevention**: Account locks (`FOR UPDATE`) are acquired strictly in lexicographical order by Account ID.
6. **Synchronous Fraud Evaluation**: Risk checks run synchronously before money moves.
7. **Fail-Closed Policy**: If Redis or fraud engine fails and `FRAUD_FAIL_CLOSED=true`, transactions are rejected or flagged for review; never silently approved.
8. **Transactional Outbox**: Events are written into `outbox_events` in the same PostgreSQL transaction as ledger entries. Outbox relay forwards to Kafka without dual-write risks.
9. **Idempotency**: Requests must provide `Idempotency-Key`. Replays return the original result; payload discrepancies are rejected with `409 Conflict`.
