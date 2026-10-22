-- Tighten existing installations without modifying or deleting account data.
-- Invalid legacy accounts cause a rollback and require explicit operator review.
BEGIN;
ALTER TABLE accounts
    DROP CONSTRAINT accounts_balance_non_negative,
    DROP CONSTRAINT accounts_available_funds,
    ADD CONSTRAINT accounts_balance_non_negative
        CHECK (id = 'system_reserve_' || currency OR balance >= 0),
    ADD CONSTRAINT accounts_available_funds
        CHECK (id = 'system_reserve_' || currency OR balance >= held);

-- The initial schema already includes this constraint for fresh databases.
ALTER TABLE accounts DROP CONSTRAINT IF EXISTS accounts_system_namespace;
ALTER TABLE accounts ADD CONSTRAINT accounts_system_namespace
    CHECK (left(id, 7) <> 'system_' OR id = 'system_reserve_' || currency);
COMMIT;
