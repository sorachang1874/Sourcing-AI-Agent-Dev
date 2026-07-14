-- Track D D3c2d: dormant ActivityRun/ActivityAttempt claim-chain foundation.
--
-- This is only the activity-chain subbatch of D3b Migration A. It does not
-- activate descriptor reads, strict writers, claim/effect predicates, foreign
-- keys, indexes, backfill, or constraint validation. Existing activity rows
-- retain explicit brownfield sentinels and are not strict-D3 identities.

SET LOCAL lock_timeout = '5s';

ALTER TABLE workflow_activity_runs
    ADD COLUMN runtime_namespace text DEFAULT ''::text NOT NULL,
    ADD COLUMN provider_mode text DEFAULT ''::text NOT NULL,
    ADD COLUMN scope_digest text DEFAULT ''::text NOT NULL,
    ADD COLUMN coordination_plan_review_id bigint,
    ADD COLUMN claim_authority_spec_digest text DEFAULT ''::text NOT NULL,
    ADD COLUMN d3_business_fence_digest text DEFAULT ''::text NOT NULL,
    ADD CONSTRAINT workflow_activity_runs_runtime_namespace_shape_ck CHECK (
        runtime_namespace = '' OR runtime_namespace ~ '[^[:space:]]'
    ) NOT VALID,
    ADD CONSTRAINT workflow_activity_runs_provider_mode_shape_ck CHECK (
        provider_mode = '' OR provider_mode IN ('live', 'simulate', 'scripted', 'replay')
    ) NOT VALID,
    ADD CONSTRAINT workflow_activity_runs_workspace_id_shape_ck CHECK (
        workspace_id ~ '[^[:space:]]'
    ) NOT VALID,
    ADD CONSTRAINT workflow_activity_runs_scope_digest_shape_ck CHECK (
        scope_digest = '' OR scope_digest ~ '^[0-9a-f]{64}$'
    ) NOT VALID,
    ADD CONSTRAINT workflow_activity_runs_coordination_plan_review_id_shape_ck CHECK (
        coordination_plan_review_id IS NULL OR coordination_plan_review_id > 0
    ) NOT VALID,
    ADD CONSTRAINT workflow_activity_runs_claim_authority_spec_digest_shape_ck CHECK (
        claim_authority_spec_digest = ''
        OR claim_authority_spec_digest ~ '^[0-9a-f]{64}$'
    ) NOT VALID,
    ADD CONSTRAINT workflow_activity_runs_d3_business_fence_digest_shape_ck CHECK (
        d3_business_fence_digest = '' OR d3_business_fence_digest ~ '^[0-9a-f]{64}$'
    ) NOT VALID;

ALTER TABLE workflow_activity_attempts
    ADD COLUMN operation_run_id text DEFAULT ''::text NOT NULL,
    ADD COLUMN runtime_namespace text DEFAULT ''::text NOT NULL,
    ADD COLUMN provider_mode text DEFAULT ''::text NOT NULL,
    ADD COLUMN scope_digest text DEFAULT ''::text NOT NULL,
    ADD COLUMN coordination_plan_review_id bigint,
    ADD COLUMN claim_authority_spec_digest text DEFAULT ''::text NOT NULL,
    ADD COLUMN d3_business_fence_digest text DEFAULT ''::text NOT NULL,
    ADD COLUMN claim_generation bigint DEFAULT 0 NOT NULL,
    ADD COLUMN command_attempt bigint DEFAULT 0 NOT NULL,
    ADD COLUMN control_epoch bigint DEFAULT 0 NOT NULL,
    ADD CONSTRAINT workflow_activity_attempts_operation_run_id_shape_ck CHECK (
        operation_run_id = '' OR operation_run_id ~ '[^[:space:]]'
    ) NOT VALID,
    ADD CONSTRAINT workflow_activity_attempts_runtime_namespace_shape_ck CHECK (
        runtime_namespace = '' OR runtime_namespace ~ '[^[:space:]]'
    ) NOT VALID,
    ADD CONSTRAINT workflow_activity_attempts_provider_mode_shape_ck CHECK (
        provider_mode = '' OR provider_mode IN ('live', 'simulate', 'scripted', 'replay')
    ) NOT VALID,
    ADD CONSTRAINT workflow_activity_attempts_workspace_id_shape_ck CHECK (
        workspace_id ~ '[^[:space:]]'
    ) NOT VALID,
    ADD CONSTRAINT workflow_activity_attempts_scope_digest_shape_ck CHECK (
        scope_digest = '' OR scope_digest ~ '^[0-9a-f]{64}$'
    ) NOT VALID,
    ADD CONSTRAINT workflow_activity_attempts_coordination_plan_review_id_shape_ck CHECK (
        coordination_plan_review_id IS NULL OR coordination_plan_review_id > 0
    ) NOT VALID,
    ADD CONSTRAINT workflow_activity_attempts_claim_authority_spec_digest_shape_ck CHECK (
        claim_authority_spec_digest = ''
        OR claim_authority_spec_digest ~ '^[0-9a-f]{64}$'
    ) NOT VALID,
    ADD CONSTRAINT workflow_activity_attempts_d3_business_fence_digest_shape_ck CHECK (
        d3_business_fence_digest = '' OR d3_business_fence_digest ~ '^[0-9a-f]{64}$'
    ) NOT VALID,
    ADD CONSTRAINT workflow_activity_attempts_claim_generation_nonnegative_ck CHECK (
        claim_generation >= 0
    ) NOT VALID,
    ADD CONSTRAINT workflow_activity_attempts_command_attempt_nonnegative_ck CHECK (
        command_attempt >= 0
    ) NOT VALID,
    ADD CONSTRAINT workflow_activity_attempts_control_epoch_nonnegative_ck CHECK (
        control_epoch >= 0
    ) NOT VALID;

SET LOCAL lock_timeout = DEFAULT;
