-- Track D D1n F4a: immutable acquisition-plan preview persistence.
--
-- The commandless plan_acquisition owner creates the AgentAction,
-- OperationRun, preview, and terminal event in one
-- transaction.  This table owns the revision/timestamp allocation and all
-- request/result/start pins needed to verify an exact replay.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE SEQUENCE acquisition_plan_preview_revision_seq
    AS bigint
    INCREMENT BY 1
    MINVALUE 1
    START WITH 1
    NO CYCLE;

-- Result contract pins are first-class AgentAction/OperationRun identity, not
-- metadata. Brownfield rows remain explicitly unpinned; the F4a writer requires
-- the complete non-empty group for plan_acquisition.
ALTER TABLE agent_actions
    ADD COLUMN result_schema_version TEXT NOT NULL DEFAULT '',
    ADD COLUMN result_schema_digest TEXT NOT NULL DEFAULT '',
    ADD COLUMN result_serializer_owner TEXT NOT NULL DEFAULT '',
    ADD COLUMN result_serializer_revision TEXT NOT NULL DEFAULT '',
    ADD COLUMN result_serializer_contract_digest TEXT NOT NULL DEFAULT '';

ALTER TABLE operation_runs
    ADD COLUMN result_schema_version TEXT NOT NULL DEFAULT '',
    ADD COLUMN result_schema_digest TEXT NOT NULL DEFAULT '',
    ADD COLUMN result_serializer_owner TEXT NOT NULL DEFAULT '',
    ADD COLUMN result_serializer_revision TEXT NOT NULL DEFAULT '',
    ADD COLUMN result_serializer_contract_digest TEXT NOT NULL DEFAULT '';

ALTER TABLE agent_actions
    ADD CONSTRAINT agent_actions_result_contract_pin_group_ck CHECK (
        (
            result_schema_version = ''
            AND result_schema_digest = ''
            AND result_serializer_owner = ''
            AND result_serializer_revision = ''
            AND result_serializer_contract_digest = ''
        )
        OR (
            result_schema_version ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
            AND result_schema_digest ~ '^[0-9a-f]{64}$'
            AND result_serializer_owner ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$'
            AND result_serializer_revision ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
            AND result_serializer_contract_digest ~ '^[0-9a-f]{64}$'
        )
    );

ALTER TABLE operation_runs
    ADD CONSTRAINT operation_runs_result_contract_pin_group_ck CHECK (
        (
            result_schema_version = ''
            AND result_schema_digest = ''
            AND result_serializer_owner = ''
            AND result_serializer_revision = ''
            AND result_serializer_contract_digest = ''
        )
        OR (
            result_schema_version ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
            AND result_schema_digest ~ '^[0-9a-f]{64}$'
            AND result_serializer_owner ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$'
            AND result_serializer_revision ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
            AND result_serializer_contract_digest ~ '^[0-9a-f]{64}$'
        )
    );

CREATE TABLE acquisition_plan_previews (
    preview_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    requester_id TEXT NOT NULL,
    action_id TEXT NOT NULL UNIQUE,
    operation_run_id TEXT NOT NULL UNIQUE,
    canonical_company_id TEXT NOT NULL,
    company_registry_revision TEXT NOT NULL,
    company_registry_digest TEXT NOT NULL,
    company_target_digest TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    preview_revision BIGINT NOT NULL DEFAULT nextval('acquisition_plan_preview_revision_seq'),
    preview_digest TEXT NOT NULL,
    effective_request_digest TEXT NOT NULL,
    provider_manifest_digest TEXT NOT NULL,
    physical_query_digest TEXT NOT NULL,
    request_schema_version TEXT NOT NULL,
    request_schema_digest TEXT NOT NULL,
    result_schema_version TEXT NOT NULL,
    result_schema_digest TEXT NOT NULL,
    result_serializer_owner TEXT NOT NULL,
    result_serializer_revision TEXT NOT NULL,
    result_serializer_contract_digest TEXT NOT NULL,
    start_request_schema_version TEXT NOT NULL,
    start_request_schema_digest TEXT NOT NULL,
    preview_json JSONB NOT NULL,
    schema_version TEXT NOT NULL DEFAULT 'acquisition_plan_preview.v2',
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    UNIQUE (workspace_id, idempotency_key),
    UNIQUE (preview_revision),
    UNIQUE (workspace_id, requester_id, canonical_company_id, preview_revision),
    CONSTRAINT acquisition_plan_previews_action_fk
        FOREIGN KEY (action_id) REFERENCES agent_actions (action_id) ON DELETE RESTRICT,
    CONSTRAINT acquisition_plan_previews_operation_fk
        FOREIGN KEY (operation_run_id) REFERENCES operation_runs (operation_run_id) ON DELETE RESTRICT,
    CONSTRAINT acquisition_plan_previews_revision_positive_ck
        CHECK (preview_revision > 0),
    CONSTRAINT acquisition_plan_previews_digest_shape_ck
        CHECK (
            company_registry_digest ~ '^[0-9a-f]{64}$'
            AND company_target_digest ~ '^[0-9a-f]{64}$'
            AND preview_digest ~ '^[0-9a-f]{64}$'
            AND effective_request_digest ~ '^[0-9a-f]{64}$'
            AND provider_manifest_digest ~ '^[0-9a-f]{64}$'
            AND physical_query_digest ~ '^[0-9a-f]{64}$'
            AND request_schema_digest ~ '^[0-9a-f]{64}$'
            AND result_schema_digest ~ '^[0-9a-f]{64}$'
            AND result_serializer_contract_digest ~ '^[0-9a-f]{64}$'
            AND start_request_schema_digest ~ '^[0-9a-f]{64}$'
        ),
    CONSTRAINT acquisition_plan_previews_identity_shape_ck
        CHECK (
            char_length(workspace_id) BETWEEN 1 AND 200
            AND workspace_id !~ '[[:space:][:cntrl:]]'
            AND char_length(requester_id) BETWEEN 1 AND 200
            AND requester_id !~ '[[:space:][:cntrl:]]'
            AND action_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$'
            AND operation_run_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$'
            AND canonical_company_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$'
            AND preview_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$'
            AND char_length(idempotency_key) BETWEEN 1 AND 512
            AND idempotency_key !~ '[[:space:][:cntrl:]]'
        ),
    CONSTRAINT acquisition_plan_previews_pin_shape_ck
        CHECK (
            request_schema_version ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
            AND result_schema_version ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
            AND start_request_schema_version ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
            AND result_serializer_owner ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$'
            AND result_serializer_revision ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
        ),
    CONSTRAINT acquisition_plan_previews_schema_version_ck
        CHECK (schema_version = 'acquisition_plan_preview.v2'),
    CONSTRAINT acquisition_plan_previews_json_identity_ck
        CHECK (
            jsonb_typeof(preview_json) IS NOT DISTINCT FROM 'object'
            AND (preview_json ->> 'schema_version') IS NOT DISTINCT FROM schema_version
            AND (preview_json ->> 'preview_id') IS NOT DISTINCT FROM preview_id
            AND CASE
                WHEN jsonb_typeof(preview_json -> 'preview_revision') = 'number'
                THEN (preview_json ->> 'preview_revision')::bigint = preview_revision
                ELSE FALSE
            END
            AND (preview_json ->> 'workspace_id') IS NOT DISTINCT FROM workspace_id
            AND (preview_json ->> 'requester_id') IS NOT DISTINCT FROM requester_id
            AND (preview_json ->> 'preview_digest') IS NOT DISTINCT FROM preview_digest
            AND (preview_json ->> 'effective_request_digest') IS NOT DISTINCT FROM effective_request_digest
            AND (preview_json #>> '{company_target,canonical_company_id}')
                IS NOT DISTINCT FROM canonical_company_id
            AND (preview_json #>> '{company_target,company_registry_revision}')
                IS NOT DISTINCT FROM company_registry_revision
            AND (preview_json #>> '{company_target,company_registry_digest}')
                IS NOT DISTINCT FROM company_registry_digest
            AND (preview_json #>> '{company_target,company_target_digest}')
                IS NOT DISTINCT FROM company_target_digest
            AND (preview_json #>> '{provider_planning_manifest,manifest_digest}')
                IS NOT DISTINCT FROM provider_manifest_digest
            AND (preview_json #>> '{provider_planning_manifest,physical_query_digest}')
                IS NOT DISTINCT FROM physical_query_digest
            AND (preview_json #>> '{schema_pins,plan_request_schema_version}')
                IS NOT DISTINCT FROM request_schema_version
            AND (preview_json #>> '{schema_pins,plan_request_schema_digest}')
                IS NOT DISTINCT FROM request_schema_digest
            AND (preview_json #>> '{schema_pins,plan_result_schema_version}')
                IS NOT DISTINCT FROM result_schema_version
            AND (preview_json #>> '{schema_pins,plan_result_schema_digest}')
                IS NOT DISTINCT FROM result_schema_digest
            AND (preview_json #>> '{schema_pins,intended_start_request_schema_version}')
                IS NOT DISTINCT FROM start_request_schema_version
            AND (preview_json #>> '{schema_pins,intended_start_request_schema_digest}')
                IS NOT DISTINCT FROM start_request_schema_digest
            AND (preview_json ->> 'created_at') IS NOT DISTINCT FROM
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
            AND (preview_json ->> 'expires_at') IS NOT DISTINCT FROM
                to_char(expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
        ),
    CONSTRAINT acquisition_plan_previews_timestamp_ttl_ck
        CHECK (
            created_at = date_trunc('second', created_at)
            AND expires_at = date_trunc('second', expires_at)
            AND expires_at > created_at
            AND expires_at <= created_at + INTERVAL '24 hours'
        )
);

ALTER SEQUENCE acquisition_plan_preview_revision_seq
    OWNED BY acquisition_plan_previews.preview_revision;

CREATE INDEX acquisition_plan_previews_owner_company_revision_idx
    ON acquisition_plan_previews (workspace_id, requester_id, canonical_company_id, preview_revision DESC);

CREATE FUNCTION reject_acquisition_plan_preview_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = 'P0001',
        CONSTRAINT = 'acquisition_plan_previews_immutable',
        MESSAGE = 'acquisition_plan_previews rows are immutable';
END;
$$;

CREATE TRIGGER acquisition_plan_previews_immutable_trg
BEFORE UPDATE OR DELETE ON acquisition_plan_previews
FOR EACH ROW EXECUTE FUNCTION reject_acquisition_plan_preview_mutation();

SET LOCAL lock_timeout = DEFAULT;
SET LOCAL statement_timeout = DEFAULT;
