-- Track D D3c2f: dormant WorkflowEvent terminal-lineage foundation.
--
-- This is only the event-core subbatch of D3b Migration A. It does not
-- activate descriptor reads, strict writers, terminal UoW behavior, transport
-- provenance, receipts, dispatch exposure, verification intent, quarantine,
-- FKs, indexes, backfill, or constraint validation. Existing event
-- rows retain explicit brownfield sentinels and are not strict-D3 identities.

SET LOCAL lock_timeout = '5s';

ALTER TABLE workflow_events
    ADD COLUMN runtime_namespace text DEFAULT ''::text NOT NULL,
    ADD COLUMN provider_mode text DEFAULT ''::text NOT NULL,
    ADD COLUMN workspace_id text DEFAULT ''::text NOT NULL,
    ADD COLUMN scope_digest text DEFAULT ''::text NOT NULL,
    ADD COLUMN coordination_plan_review_id bigint,
    ADD COLUMN activity_run_id text DEFAULT ''::text NOT NULL,
    ADD COLUMN claim_generation bigint DEFAULT 0 NOT NULL,
    ADD COLUMN control_epoch bigint DEFAULT 0 NOT NULL,
    ADD COLUMN claim_authority_spec_digest text DEFAULT ''::text NOT NULL,
    ADD COLUMN d3_business_fence_digest text DEFAULT ''::text NOT NULL,
    ADD COLUMN terminal_outcome_digest text,
    ADD CONSTRAINT workflow_events_runtime_namespace_shape_ck CHECK (
        runtime_namespace = '' OR runtime_namespace ~ '[^[:space:]]'
    ) NOT VALID,
    ADD CONSTRAINT workflow_events_provider_mode_shape_ck CHECK (
        provider_mode = '' OR provider_mode IN ('live', 'simulate', 'scripted', 'replay')
    ) NOT VALID,
    ADD CONSTRAINT workflow_events_workspace_id_shape_ck CHECK (
        workspace_id = '' OR workspace_id ~ '[^[:space:]]'
    ) NOT VALID,
    ADD CONSTRAINT workflow_events_scope_digest_shape_ck CHECK (
        scope_digest = '' OR scope_digest ~ '^[0-9a-f]{64}$'
    ) NOT VALID,
    ADD CONSTRAINT workflow_events_coordination_plan_review_id_shape_ck CHECK (
        coordination_plan_review_id IS NULL OR coordination_plan_review_id > 0
    ) NOT VALID,
    ADD CONSTRAINT workflow_events_activity_run_id_shape_ck CHECK (
        activity_run_id = '' OR activity_run_id ~ '[^[:space:]]'
    ) NOT VALID,
    ADD CONSTRAINT workflow_events_claim_generation_nonnegative_ck CHECK (
        claim_generation >= 0
    ) NOT VALID,
    ADD CONSTRAINT workflow_events_control_epoch_nonnegative_ck CHECK (
        control_epoch >= 0
    ) NOT VALID,
    ADD CONSTRAINT workflow_events_claim_authority_spec_digest_shape_ck CHECK (
        claim_authority_spec_digest = ''
        OR claim_authority_spec_digest ~ '^[0-9a-f]{64}$'
    ) NOT VALID,
    ADD CONSTRAINT workflow_events_d3_business_fence_digest_shape_ck CHECK (
        d3_business_fence_digest = '' OR d3_business_fence_digest ~ '^[0-9a-f]{64}$'
    ) NOT VALID,
    ADD CONSTRAINT workflow_events_terminal_outcome_digest_shape_ck CHECK (
        terminal_outcome_digest IS NULL OR terminal_outcome_digest ~ '^[0-9a-f]{64}$'
    ) NOT VALID;

SET LOCAL lock_timeout = DEFAULT;
