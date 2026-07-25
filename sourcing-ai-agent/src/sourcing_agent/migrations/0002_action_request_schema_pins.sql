-- Track D D1c: immutable ActionRequestSpec pins.
-- Empty pairs are the explicit brownfield/schema-less compatibility residual.
-- Brownfield installation is bounded and does not scan either populated table.
-- A later, separately deployed migration must VALIDATE these constraints after
-- the compatibility population has been audited.

SET LOCAL lock_timeout = '5s';

ALTER TABLE agent_actions
    ADD COLUMN request_schema_version text DEFAULT ''::text NOT NULL,
    ADD COLUMN request_schema_digest text DEFAULT ''::text NOT NULL,
    ADD CONSTRAINT agent_actions_request_schema_pin_pair_check CHECK (
        (
            request_schema_version = ''
            AND request_schema_digest = ''
        )
        OR
        (
            request_schema_version <> ''
            AND request_schema_version ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
            AND request_schema_digest ~ '^[0-9a-f]{64}$'
        )
    ) NOT VALID;

ALTER TABLE operation_runs
    ADD COLUMN request_schema_version text DEFAULT ''::text NOT NULL,
    ADD COLUMN request_schema_digest text DEFAULT ''::text NOT NULL,
    ADD CONSTRAINT operation_runs_request_schema_pin_pair_check CHECK (
        (
            request_schema_version = ''
            AND request_schema_digest = ''
        )
        OR
        (
            request_schema_version <> ''
            AND request_schema_version ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
            AND request_schema_digest ~ '^[0-9a-f]{64}$'
        )
    ) NOT VALID;

SET LOCAL lock_timeout = DEFAULT;
