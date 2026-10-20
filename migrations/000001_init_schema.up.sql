-- 000001_init_schema.up.sql
-- Production fintech schema with double-entry constraints and audit outbox

CREATE TABLE IF NOT EXISTS accounts (
    id VARCHAR(64) PRIMARY KEY,
    currency VARCHAR(3) NOT NULL DEFAULT 'INR',
    balance BIGINT NOT NULL DEFAULT 0,
    held BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT accounts_balance_non_negative CHECK (id LIKE 'system_%' OR balance >= 0),
    CONSTRAINT accounts_held_non_negative CHECK (held >= 0),
    CONSTRAINT accounts_available_funds CHECK (id LIKE 'system_%' OR balance >= held)
);

CREATE TABLE IF NOT EXISTS transfers (
    id VARCHAR(64) PRIMARY KEY,
    idempotency_key VARCHAR(128) NOT NULL UNIQUE,
    from_account_id VARCHAR(64),
    to_account_id VARCHAR(64),
    amount BIGINT,
    currency VARCHAR(3) NOT NULL,
    status VARCHAR(32) NOT NULL,
    type VARCHAR(32) NOT NULL DEFAULT 'standard',
    parent_transfer_id VARCHAR(64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transfers_from_account ON transfers (from_account_id);
CREATE INDEX IF NOT EXISTS idx_transfers_to_account ON transfers (to_account_id);
CREATE INDEX IF NOT EXISTS idx_transfers_created_at ON transfers (created_at);

CREATE TABLE IF NOT EXISTS entries (
    id VARCHAR(64) PRIMARY KEY,
    transfer_id VARCHAR(64) NOT NULL REFERENCES transfers(id) ON DELETE RESTRICT,
    account_id VARCHAR(64) NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    amount BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_entries_account_created ON entries (account_id, created_at);
CREATE INDEX IF NOT EXISTS idx_entries_transfer ON entries (transfer_id);

CREATE TABLE IF NOT EXISTS holds (
    id VARCHAR(64) PRIMARY KEY,
    idempotency_key VARCHAR(128) NOT NULL UNIQUE,
    from_account_id VARCHAR(64) NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    to_account_id VARCHAR(64) NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    amount BIGINT NOT NULL,
    captured BIGINT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    capture_transfer_id VARCHAR(64) REFERENCES transfers(id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    CONSTRAINT holds_amount_positive CHECK (amount > 0),
    CONSTRAINT holds_captured_lte_amount CHECK (captured <= amount)
);

CREATE INDEX IF NOT EXISTS idx_holds_status_expires ON holds (status, expires_at) WHERE status = 'active';

CREATE TABLE IF NOT EXISTS refunds (
    id VARCHAR(64) PRIMARY KEY,
    idempotency_key VARCHAR(128) NOT NULL UNIQUE,
    original_transfer_id VARCHAR(64) NOT NULL REFERENCES transfers(id) ON DELETE RESTRICT,
    refund_transfer_id VARCHAR(64) NOT NULL REFERENCES transfers(id) ON DELETE RESTRICT,
    amount BIGINT NOT NULL,
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT refunds_amount_positive CHECK (amount > 0)
);

CREATE INDEX IF NOT EXISTS idx_refunds_original ON refunds (original_transfer_id);

CREATE TABLE IF NOT EXISTS outbox_events (
    id VARCHAR(64) PRIMARY KEY,
    event_type VARCHAR(64) NOT NULL,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    retry_count INT NOT NULL DEFAULT 0,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox_events (created_at) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS fraud_evaluations (
    id VARCHAR(64) PRIMARY KEY,
    idempotency_key VARCHAR(128),
    payment_id VARCHAR(64),
    account_id VARCHAR(64) NOT NULL,
    beneficiary_id VARCHAR(64),
    amount BIGINT NOT NULL,
    currency VARCHAR(3) NOT NULL,
    score NUMERIC(5,2) NOT NULL,
    decision VARCHAR(32) NOT NULL,
    reasons TEXT[] NOT NULL DEFAULT '{}',
    rule_hits JSONB NOT NULL DEFAULT '[]',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fraud_account ON fraud_evaluations (account_id, created_at);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id VARCHAR(64) NOT NULL,
    consumer_group VARCHAR(64) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, consumer_group)
);
