-- Track D D1m fixed-forward: one durable Company Public Web source-run owner
-- row per effective idempotency identity.
--
-- The source-run creator strips protocol whitespace before writing. Index the
-- same effective identity so brownfield space/tab/newline variants cannot
-- bypass the uniqueness fence.  Empty/whitespace-only legacy
-- keys remain outside the index; normal D1m writes reject them before SQL.
-- CREATE UNIQUE INDEX intentionally fails closed when brownfield duplicates
-- already exist.  The migration runner then rolls back both this DDL and its
-- ledger row atomically.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- One database-owned order authority for logical source projections. Values
-- are reserved only through the exact-claim native UoW; transactional rollback
-- and abandoned attempts may leave gaps, which are deliberately harmless.
CREATE SEQUENCE company_public_web_source_projection_revision_seq
    AS bigint
    INCREMENT BY 1
    MINVALUE 1
    START WITH 1
    NO CYCLE;

CREATE UNIQUE INDEX company_public_web_asset_runs_idempotency_key_uk
    ON company_public_web_asset_runs ((btrim(idempotency_key, E' \t\n\r\f\013')))
    WHERE btrim(idempotency_key, E' \t\n\r\f\013') <> '';

SET LOCAL lock_timeout = DEFAULT;
SET LOCAL statement_timeout = DEFAULT;
