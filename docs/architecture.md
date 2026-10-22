# Payment design and review notes

## Problem and acceptance criteria

A customer can retry checkout after losing a response. A merchant can authorize an amount, capture less, or refund part of a payment. Concurrent requests and risk dependency failures must not silently change the financial outcome.

Acceptance criteria for this local testbed:

1. A committed direct transfer replay returns the same transaction; changed amount, parties, or currency conflicts.
2. Posted entries balance to zero and account balances reconcile to entries. Ordinary customer funds cover spending and outstanding holds.
3. An authorization reserves funds without settling them. One partial capture settles its amount and releases the remaining reservation; incremental captures are outside this model.
4. Refunds cannot exceed the remaining refundable amount.
5. REVIEW and REJECT assessments create no holds or transfers. Configured fail-closed dependency failures, including velocity writes, stop new funds mutations.
6. Events for implemented ledger mutations are inserted in the same PostgreSQL transaction; broker availability is outside that transaction.

The system uses a single trusted service credential. It does not model customers, merchant ownership, KYC, card networks, or bank settlement. Initial wallet funding is a demo/admin capability with matching opening entries against a currency reserve account.

## Financial invariants

| Invariant | Implementation | Evidence |
|---|---|---|
| Entries sum to zero | `ledger.AggregatePostings`, `ledger.AssertBalanced` | Multi-leg posting tests and reconciliation |
| Available = settled balance - held | Account checks under lock; ordinary-account SQL constraints | 100 competing spending attempts |
| Concurrent retries cannot duplicate a transfer | Account locks plus unique transfer key | 100 concurrent transfer retry test |
| Repeated capture refers to the same authorization | Hold-to-capture linkage | Shared memory/PostgreSQL capture contract |
| Funds and payment event commit together | Outbox insert inside the financial transaction | PostgreSQL outbox integration test |

Money uses signed 64-bit integer paise, avoiding binary floating-point rounding. **Integer representation alone does not prove arithmetic is overflow-safe.** Checked arithmetic and explicit bounds remain necessary before exposing arbitrary financial inputs to untrusted clients.

## Transaction boundaries

### Direct payment

1. Authenticate, decode and validate; normalize omitted currency to INR.
2. Look up the transfer key. Return a matching committed standard transfer without another risk check; reject conflicting payloads.
3. Assess new intent synchronously using recent attempts and amount rules.
4. On approval, aggregate account impact and start the posting transaction. Lock account IDs in sorted order, then recheck the key inside the transaction.
5. Check currencies and available balances, write entries and balances, insert the outbox event, commit.

The API replay lookup does not replace transactional idempotency: concurrent first attempts can both miss it. Sorted account locking reduces circular account-lock deadlocks; it does not prove every hold/refund/worker lock interaction is deadlock-free.

### Authorization, capture and refund

| Operation | Settled balance | Held balance | Result |
|---|---|---|---|
| Authorize INR 50 | Unchanged | +50 | Active hold |
| Capture INR 40 | Buyer -40, merchant +40 | -50 | Captured; INR 10 released |
| Refund INR 10 | Merchant -10, buyer +10 | Unchanged | Reversal linked to original transfer |

Capture locks the hold before reading its committed capture result. A replay must match the amount and the hold's `capture_transfer_id`. Refunds lock their original transfer while checking prior refunds.

Expiry is represented in the hold model, but the service entrypoint does not schedule `ExpireHolds`. PostgreSQL rejects an expired capture; returning that error rolls back its attempted expiry update, leaving reservation release to a successful sweep. Automatic expiry cleanup is not established by current service wiring.

## Risk is an assessment, not a reservation

Redis sorted sets track recent **assessed attempts**, not committed ledger debits. Approval and review record an attempt before ledger commit. A later insufficient-funds result can still consume velocity budget. Completed API replays skip risk and do not add an attempt.

Queries and recording are separate Redis commands. Concurrent new intents can observe the same count before either records it. This is scoring evidence, **not a strict atomic velocity quota**. A hard limit would need an atomic reservation and a specified policy for failed ledger commits.

With fail-closed enabled, an unavailable tracker, failed read, or failed attempt write produces the configured reject/review assessment. Assessment audit persistence currently remains best effort; a durable compliance audit requirement would need a stricter policy.

`202 pending_review` means no funds mutation occurred. Only the assessment is recorded; there is no durable payment-review queue, approval endpoint, or resumable payment workflow. A repeated reviewed request may produce another assessment.

## Design decisions

| Decision | Reason | Cost / boundary |
|---|---|---|
| One Go service with store/tracker interfaces | Payment flow and error handling remain easy to trace | Shared deployment; no tenant authorization |
| PostgreSQL transactions and row locks | Spend checks and mutations happen together | Hot accounts serialize; benchmarked separately |
| Aggregate multi-leg impact before validation | Repeated debits on one account must be checked together | Arithmetic needs explicit overflow bounds |
| Transactional outbox | Financial commit does not wait for broker availability | Asynchronous publishing may duplicate after a crash |
| Redis for recent risk signals | Separates short-lived signals from durable accounting | No atomic risk/ledger reservation |
| One Go demo shared by shell wrappers | Windows and Linux execute identical assertions | Requires the existing Go toolchain |

## Messaging semantics

A crash after publishing and before marking an outbox row permits duplicate delivery. Delivery claims are **at least once**, with no universal exactly-once guarantee.

The consumer checks processed IDs separately from handler side effects. Sequential deduplication is tested; concurrent/crash-safe exactly-once effects are not established. The broker loop also needs a failure/offset-commit review before consequential downstream effects: malformed messages and some handler failures can advance offsets, and DLQ publishing is not transactionally coupled to acknowledgement. The server's example handlers only log events.

A live Kafka broker was not used in the latest local verification. Outbox tests exercise transient publisher errors with an in-process publisher adapter and real PostgreSQL.

## Debugging changes with reproducible regressions

| Observed bug | Root cause | Regression and change |
|---|---|---|
| REVIEW authorization created a hold | Only REJECT was handled | API asserts 202 and no hold/funds mutation |
| Committed retry failed during Redis outage | Risk ran before committed-result lookup | Same response on replay; new intent remains blocked |
| Read-only tracker allowed payment | Attempt write error discarded | Fail-closed write-failure regression |
| Capture key accepted another hold | Replay omitted hold identity | Shared store contract and capture linkage check |
| Benchmark reported rejected-request speed | Header absent; attempts counted; failures hidden | Real HTTP payment test and all-rejected throughput regression |

## Boundaries and next decisions

Before real funds or untrusted multi-user access, prioritize:

1. **Financial input and identity boundaries:** checked arithmetic, strict identifiers and reserve-account isolation, funding permissions, account ownership, bounded JSON, and secret handling.
2. **Complete lifecycle contracts:** durable review state, scheduled expiry, uniform retry matching for expiry/void/refund semantics, and cross-operation key policy. Keys currently live in operation-specific tables, not one global intent namespace.
3. **Consequential event delivery:** atomic handler/inbox effects, explicit offset/DLQ failure behavior, crash-window tests against live Kafka.
4. **Operational proof:** dependency readiness, distributed rate limiting, retention, worker shutdown, sustained load, backup and restore.

These are explicit next milestones, not production guarantees. No additional microservices are needed to demonstrate them.

## Attribution

Pinned upstream conceptual references are listed in the [README](../README.md#attribution) and [license notices](../THIRD_PARTY_LICENSES).
