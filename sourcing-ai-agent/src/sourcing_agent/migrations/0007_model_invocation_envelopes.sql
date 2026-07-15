-- Track D D0f: immutable durable ModelInvocationEnvelopeV1 evidence.
--
-- This table persists exact canonical terminal-envelope bytes under one
-- full-PFX owner-issued reference. It deliberately does not implement logical
-- result-slot accept/consume, AgentAction effects, transport receipts, or cost
-- ledger rows.

SET LOCAL lock_timeout = '5s';

CREATE TABLE model_invocation_envelopes (
    runtime_namespace text NOT NULL,
    provider_mode text NOT NULL,
    workspace_id text NOT NULL,
    scope_digest text NOT NULL,
    coordination_plan_review_id bigint NOT NULL,
    model_invocation_envelope_ref text NOT NULL,
    envelope_schema_version text NOT NULL,
    envelope_digest text NOT NULL,
    envelope_record_json text,
    retention_policy_version text NOT NULL,
    retention_state text NOT NULL DEFAULT 'retained',
    retained_until timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    purged_at timestamptz,
    state_version bigint NOT NULL DEFAULT 0,
    CONSTRAINT model_invocation_envelopes_pkey PRIMARY KEY (
        runtime_namespace,
        provider_mode,
        workspace_id,
        scope_digest,
        coordination_plan_review_id,
        model_invocation_envelope_ref
    ),
    CONSTRAINT model_invocation_envelopes_ref_digest_uk UNIQUE (
        runtime_namespace,
        provider_mode,
        workspace_id,
        scope_digest,
        coordination_plan_review_id,
        model_invocation_envelope_ref,
        envelope_digest
    ),
    CONSTRAINT model_invocation_envelopes_digest_uk UNIQUE (
        runtime_namespace,
        provider_mode,
        workspace_id,
        scope_digest,
        coordination_plan_review_id,
        envelope_digest
    ),
    CONSTRAINT model_invocation_envelopes_runtime_namespace_shape_ck CHECK (
        runtime_namespace ~ '[^[:space:]]'
    ),
    CONSTRAINT model_invocation_envelopes_provider_mode_shape_ck CHECK (
        provider_mode IN ('live', 'simulate', 'scripted')
    ),
    CONSTRAINT model_invocation_envelopes_workspace_id_shape_ck CHECK (
        workspace_id ~ '[^[:space:]]'
    ),
    CONSTRAINT model_invocation_envelopes_scope_digest_shape_ck CHECK (
        scope_digest ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT model_invocation_envelopes_coordination_review_shape_ck CHECK (
        coordination_plan_review_id > 0
    ),
    CONSTRAINT model_invocation_envelopes_ref_shape_ck CHECK (
        model_invocation_envelope_ref ~ '^mie:v1:[0-9a-f]{64}:[0-9a-f]{64}$'
    ),
    CONSTRAINT model_invocation_envelopes_schema_version_ck CHECK (
        envelope_schema_version = 'model_invocation_envelope_v1'
    ),
    CONSTRAINT model_invocation_envelopes_digest_shape_ck CHECK (
        envelope_digest ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT model_invocation_envelopes_record_json_shape_ck CHECK (
        envelope_record_json IS NULL OR jsonb_typeof(envelope_record_json::jsonb) = 'object'
    ),
    CONSTRAINT model_invocation_envelopes_retention_policy_ck CHECK (
        retention_policy_version = 'model_invocation_retention_30d_v1'
    ),
    CONSTRAINT model_invocation_envelopes_retained_until_ck CHECK (
        retained_until = created_at + interval '30 days'
    ),
    CONSTRAINT model_invocation_envelopes_retention_state_ck CHECK (
        retention_state IN ('retained', 'purged_tombstone')
    ),
    CONSTRAINT model_invocation_envelopes_retention_pair_ck CHECK (
        (
            retention_state = 'retained'
            AND envelope_record_json IS NOT NULL
            AND purged_at IS NULL
            AND state_version = 0
        ) OR (
            retention_state = 'purged_tombstone'
            AND envelope_record_json IS NULL
            AND purged_at IS NOT NULL
            AND purged_at >= retained_until
            AND state_version = 1
        )
    )
);

SET LOCAL lock_timeout = DEFAULT;
