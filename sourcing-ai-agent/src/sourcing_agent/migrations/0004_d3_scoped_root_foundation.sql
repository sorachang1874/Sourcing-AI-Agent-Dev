-- Track D D3c2b: dormant scoped review-session and OperationRun root foundation.
--
-- This is the scoped-root subbatch of D3b Migration A. It does not activate
-- the scoped-session repository, adopt legacy sessions/runs, expose these
-- columns through runtime descriptors, or authorize claim/dispatch behavior.

SET LOCAL lock_timeout = '5s';

ALTER TABLE plan_review_sessions
    ADD COLUMN runtime_namespace text DEFAULT ''::text NOT NULL,
    ADD COLUMN provider_mode text DEFAULT ''::text NOT NULL,
    ADD COLUMN workspace_id text DEFAULT ''::text NOT NULL,
    ADD COLUMN scope_issuer text DEFAULT ''::text NOT NULL,
    ADD COLUMN scope_digest text DEFAULT ''::text NOT NULL,
    ADD COLUMN creation_source_workflow_command_id text DEFAULT ''::text NOT NULL,
    ADD COLUMN creation_source_event_id text DEFAULT ''::text NOT NULL,
    ADD COLUMN creation_plan_id text DEFAULT ''::text NOT NULL,
    ADD COLUMN creation_plan_revision bigint DEFAULT 0 NOT NULL,
    ADD COLUMN creation_plan_bundle_digest text DEFAULT ''::text NOT NULL,
    ADD COLUMN creation_idempotency_key text DEFAULT ''::text NOT NULL,
    ADD CONSTRAINT plan_review_sessions_runtime_namespace_shape_ck CHECK (
        runtime_namespace = '' OR runtime_namespace ~ '[^[:space:]]'
    ) NOT VALID,
    ADD CONSTRAINT plan_review_sessions_provider_mode_shape_ck CHECK (
        provider_mode = '' OR provider_mode IN ('live', 'simulate', 'scripted', 'replay')
    ) NOT VALID,
    ADD CONSTRAINT plan_review_sessions_workspace_id_shape_ck CHECK (
        workspace_id = '' OR workspace_id ~ '[^[:space:]]'
    ) NOT VALID,
    ADD CONSTRAINT plan_review_sessions_scope_issuer_shape_ck CHECK (
        scope_issuer = '' OR scope_issuer = 'plan_review_session'
    ) NOT VALID,
    ADD CONSTRAINT plan_review_sessions_scope_digest_shape_ck CHECK (
        scope_digest = '' OR scope_digest ~ '^[0-9a-f]{64}$'
    ) NOT VALID,
    ADD CONSTRAINT plan_review_sessions_creation_source_command_id_shape_ck CHECK (
        creation_source_workflow_command_id = ''
        OR creation_source_workflow_command_id ~ '[^[:space:]]'
    ) NOT VALID,
    ADD CONSTRAINT plan_review_sessions_creation_source_event_id_shape_ck CHECK (
        creation_source_event_id = '' OR creation_source_event_id ~ '[^[:space:]]'
    ) NOT VALID,
    ADD CONSTRAINT plan_review_sessions_creation_plan_id_shape_ck CHECK (
        creation_plan_id = '' OR creation_plan_id ~ '[^[:space:]]'
    ) NOT VALID,
    ADD CONSTRAINT plan_review_sessions_creation_plan_revision_nonnegative_ck CHECK (
        creation_plan_revision >= 0
    ) NOT VALID,
    ADD CONSTRAINT plan_review_sessions_creation_plan_bundle_digest_shape_ck CHECK (
        creation_plan_bundle_digest = ''
        OR creation_plan_bundle_digest ~ '^[0-9a-f]{64}$'
    ) NOT VALID,
    ADD CONSTRAINT plan_review_sessions_creation_idempotency_key_shape_ck CHECK (
        creation_idempotency_key = ''
        OR creation_idempotency_key ~ '^[0-9a-f]{64}$'
    ) NOT VALID;

ALTER TABLE operation_runs
    ADD COLUMN runtime_namespace text DEFAULT ''::text NOT NULL,
    ADD COLUMN provider_mode text DEFAULT ''::text NOT NULL,
    ADD COLUMN scope_issuer text DEFAULT ''::text NOT NULL,
    ADD COLUMN scope_digest text DEFAULT ''::text NOT NULL,
    ADD COLUMN coordination_plan_review_id bigint,
    ADD CONSTRAINT operation_runs_runtime_namespace_shape_ck CHECK (
        runtime_namespace = '' OR runtime_namespace ~ '[^[:space:]]'
    ) NOT VALID,
    ADD CONSTRAINT operation_runs_provider_mode_shape_ck CHECK (
        provider_mode = '' OR provider_mode IN ('live', 'simulate', 'scripted', 'replay')
    ) NOT VALID,
    ADD CONSTRAINT operation_runs_scope_issuer_shape_ck CHECK (
        scope_issuer = '' OR scope_issuer = 'plan_review_session'
    ) NOT VALID,
    ADD CONSTRAINT operation_runs_scope_digest_shape_ck CHECK (
        scope_digest = '' OR scope_digest ~ '^[0-9a-f]{64}$'
    ) NOT VALID,
    ADD CONSTRAINT operation_runs_coordination_plan_review_id_shape_ck CHECK (
        coordination_plan_review_id IS NULL OR coordination_plan_review_id > 0
    ) NOT VALID;

SET LOCAL lock_timeout = DEFAULT;
