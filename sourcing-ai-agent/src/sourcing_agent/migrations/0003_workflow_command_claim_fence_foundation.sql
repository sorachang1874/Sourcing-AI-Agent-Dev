-- Track D D3c2a: dormant workflow-command claim-fence physical foundation.
--
-- This is only the workflow_commands subbatch of D3b Migration A.  It does
-- not activate descriptor reads, claim mint/CAS, strict-D3 writers, foreign
-- keys, indexes, active-row coupling, or constraint validation.  Brownfield
-- rows retain the explicit sentinel shapes described by the D3b contract.

SET LOCAL lock_timeout = '5s';

ALTER TABLE workflow_commands
    ADD COLUMN runtime_namespace text DEFAULT ''::text NOT NULL,
    ADD COLUMN provider_mode text DEFAULT ''::text NOT NULL,
    ADD COLUMN workspace_id text DEFAULT ''::text NOT NULL,
    ADD COLUMN scope_digest text DEFAULT ''::text NOT NULL,
    ADD COLUMN coordination_plan_review_id bigint,
    ADD COLUMN claim_authority_spec_digest text DEFAULT ''::text NOT NULL,
    ADD COLUMN expected_predecessor_intent_id text,
    ADD COLUMN expected_predecessor_phase_generation bigint,
    ADD COLUMN expected_predecessor_source_control_epoch bigint,
    ADD COLUMN expected_predecessor_decision_source_event_id text,
    ADD COLUMN d3_business_fence_digest text DEFAULT ''::text NOT NULL,
    ADD COLUMN claim_selection_generation bigint DEFAULT 0 NOT NULL,
    ADD COLUMN consumed_claim_authority_id text DEFAULT ''::text NOT NULL,
    ADD COLUMN claim_generation bigint DEFAULT 0 NOT NULL,
    ADD COLUMN claim_token_digest text DEFAULT ''::text NOT NULL,
    ADD COLUMN control_epoch bigint DEFAULT 0 NOT NULL,
    ADD COLUMN heartbeat_sequence bigint DEFAULT 0 NOT NULL,
    ADD COLUMN last_heartbeat_id text DEFAULT ''::text NOT NULL,
    ADD COLUMN terminal_event_id text,
    ADD COLUMN terminal_outcome_digest text,
    ADD CONSTRAINT workflow_commands_runtime_namespace_shape_ck CHECK (
        runtime_namespace = '' OR runtime_namespace ~ '[^[:space:]]'
    ) NOT VALID,
    ADD CONSTRAINT workflow_commands_provider_mode_shape_ck CHECK (
        provider_mode = '' OR provider_mode IN ('live', 'simulate', 'scripted', 'replay')
    ) NOT VALID,
    ADD CONSTRAINT workflow_commands_workspace_id_shape_ck CHECK (
        workspace_id = '' OR workspace_id ~ '[^[:space:]]'
    ) NOT VALID,
    ADD CONSTRAINT workflow_commands_coordination_plan_review_id_shape_ck CHECK (
        coordination_plan_review_id IS NULL OR coordination_plan_review_id > 0
    ) NOT VALID,
    ADD CONSTRAINT workflow_commands_scope_digest_shape_ck CHECK (
        scope_digest = '' OR scope_digest ~ '^[0-9a-f]{64}$'
    ) NOT VALID,
    ADD CONSTRAINT workflow_commands_claim_authority_spec_digest_shape_ck CHECK (
        claim_authority_spec_digest = '' OR claim_authority_spec_digest ~ '^[0-9a-f]{64}$'
    ) NOT VALID,
    ADD CONSTRAINT workflow_commands_d3_business_fence_digest_shape_ck CHECK (
        d3_business_fence_digest = '' OR d3_business_fence_digest ~ '^[0-9a-f]{64}$'
    ) NOT VALID,
    ADD CONSTRAINT workflow_commands_terminal_outcome_digest_shape_ck CHECK (
        terminal_outcome_digest IS NULL OR terminal_outcome_digest ~ '^[0-9a-f]{64}$'
    ) NOT VALID,
    ADD CONSTRAINT workflow_commands_terminal_pair_shape_ck CHECK (
        (terminal_event_id IS NULL AND terminal_outcome_digest IS NULL)
        OR (terminal_event_id IS NOT NULL AND terminal_outcome_digest IS NOT NULL)
    ) NOT VALID,
    ADD CONSTRAINT workflow_commands_consumed_claim_authority_id_shape_ck CHECK (
        consumed_claim_authority_id = '' OR consumed_claim_authority_id ~ '[^[:space:]]'
    ) NOT VALID,
    ADD CONSTRAINT workflow_commands_claim_generation_nonnegative_ck CHECK (
        claim_generation >= 0
    ) NOT VALID,
    ADD CONSTRAINT workflow_commands_claim_selection_generation_nonnegative_ck CHECK (
        claim_selection_generation >= 0
    ) NOT VALID,
    ADD CONSTRAINT workflow_commands_control_epoch_nonnegative_ck CHECK (
        control_epoch >= 0
    ) NOT VALID,
    ADD CONSTRAINT workflow_commands_heartbeat_sequence_nonnegative_ck CHECK (
        heartbeat_sequence >= 0
    ) NOT VALID,
    ADD CONSTRAINT workflow_commands_claim_token_digest_shape_ck CHECK (
        claim_token_digest = '' OR claim_token_digest ~ '^[0-9a-f]{64}$'
    ) NOT VALID,
    ADD CONSTRAINT workflow_commands_expected_predecessor_shape_ck CHECK (
        (
            expected_predecessor_intent_id IS NULL
            AND expected_predecessor_phase_generation IS NULL
            AND expected_predecessor_source_control_epoch IS NULL
            AND expected_predecessor_decision_source_event_id IS NULL
        )
        OR
        (
            expected_predecessor_intent_id IS NOT NULL
            AND expected_predecessor_intent_id ~ '[^[:space:]]'
            AND expected_predecessor_phase_generation IS NOT NULL
            AND expected_predecessor_phase_generation > 0
            AND expected_predecessor_source_control_epoch IS NOT NULL
            AND expected_predecessor_source_control_epoch >= 0
            AND expected_predecessor_decision_source_event_id IS NOT NULL
            AND expected_predecessor_decision_source_event_id ~ '[^[:space:]]'
        )
    ) NOT VALID;

SET LOCAL lock_timeout = DEFAULT;
