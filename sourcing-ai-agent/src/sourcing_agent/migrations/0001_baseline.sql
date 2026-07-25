-- migrations/0001_baseline.sql
-- Track B B1.1 — PG-native control-plane schema baseline (single source of truth).
--
-- GENERATED, not hand-written: captured from a fresh ControlPlaneStore bootstrap via
-- the real code path (init_schema + ensure_bootstrapped + writer/coordination ensures),
-- pg_dump --schema-only, normalized to be schema-agnostic (table set verified == live
-- `public`, 83 tables, 0 diff). Regenerate with scripts/capture_pg_schema_baseline.py
-- and review the git diff; keep byte-stable until a reviewed 0002_*.sql migration.
--
-- The migration runner sets search_path to the target schema before applying this file,
-- so all object names here are intentionally UNQUALIFIED. Do not add a schema prefix.
--

-- Name: acquisition_discovery_lanes; Type: TABLE; Schema: -; Owner: -

CREATE TABLE acquisition_discovery_lanes (
    lane_id text NOT NULL,
    workspace_id text DEFAULT 'default'::text NOT NULL,
    acquisition_run_id text DEFAULT ''::text NOT NULL,
    workflow_run_id text DEFAULT ''::text NOT NULL,
    operation_run_id text DEFAULT ''::text NOT NULL,
    source_command_id text DEFAULT ''::text NOT NULL,
    activity_run_id text DEFAULT ''::text NOT NULL,
    target_company text DEFAULT ''::text NOT NULL,
    query text DEFAULT ''::text NOT NULL,
    provider text DEFAULT ''::text NOT NULL,
    status text DEFAULT 'planned'::text NOT NULL,
    phase text DEFAULT 'planned'::text NOT NULL,
    lane_plan_json text DEFAULT '{}'::text NOT NULL,
    provider_ref_json text DEFAULT '{}'::text NOT NULL,
    artifact_refs_json text DEFAULT '[]'::text NOT NULL,
    entity_counts_json text DEFAULT '{}'::text NOT NULL,
    downstream_command_ids_json text DEFAULT '[]'::text NOT NULL,
    idempotency_key text NOT NULL,
    metadata_json text DEFAULT '{}'::text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: acquisition_runs; Type: TABLE; Schema: -; Owner: -

CREATE TABLE acquisition_runs (
    acquisition_run_id text NOT NULL,
    workspace_id text DEFAULT 'default'::text NOT NULL,
    operation_run_id text DEFAULT ''::text NOT NULL,
    workflow_run_id text DEFAULT ''::text NOT NULL,
    plan_id text DEFAULT ''::text NOT NULL,
    plan_review_id bigint DEFAULT 0 NOT NULL,
    target_company text DEFAULT ''::text NOT NULL,
    query text DEFAULT ''::text NOT NULL,
    status text DEFAULT 'planned'::text NOT NULL,
    current_phase text DEFAULT ''::text NOT NULL,
    request_json text DEFAULT '{}'::text NOT NULL,
    plan_json text DEFAULT '{}'::text NOT NULL,
    execution_bundle_json text DEFAULT '{}'::text NOT NULL,
    metadata_json text DEFAULT '{}'::text NOT NULL,
    idempotency_key text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: acquisition_shard_registry_current; Type: TABLE; Schema: -; Owner: -

CREATE TABLE acquisition_shard_registry_current (
    shard_key text NOT NULL,
    target_company text NOT NULL,
    company_key text,
    snapshot_id text NOT NULL,
    asset_view text DEFAULT 'canonical_merged'::text NOT NULL,
    lane text NOT NULL,
    status text DEFAULT 'completed'::text NOT NULL,
    employment_scope text DEFAULT 'all'::text NOT NULL,
    strategy_type text,
    shard_id text,
    shard_title text,
    search_query text,
    query_signature text,
    company_scope_json text DEFAULT '[]'::text NOT NULL,
    locations_json text DEFAULT '[]'::text NOT NULL,
    function_ids_json text DEFAULT '[]'::text NOT NULL,
    result_count bigint DEFAULT 0 NOT NULL,
    estimated_total_count bigint DEFAULT 0 NOT NULL,
    provider_cap_hit boolean DEFAULT false NOT NULL,
    source_path text,
    source_job_id text,
    metadata_json text DEFAULT '{}'::text NOT NULL,
    first_seen_at text,
    last_completed_at text,
    materialization_generation_key text,
    materialization_generation_sequence bigint DEFAULT 0 NOT NULL,
    materialization_watermark text,
    created_at text DEFAULT CURRENT_TIMESTAMP,
    updated_at text DEFAULT CURRENT_TIMESTAMP
);

-- Name: acquisition_shard_registry_former; Type: TABLE; Schema: -; Owner: -

CREATE TABLE acquisition_shard_registry_former (
    shard_key text NOT NULL,
    target_company text NOT NULL,
    company_key text,
    snapshot_id text NOT NULL,
    asset_view text DEFAULT 'canonical_merged'::text NOT NULL,
    lane text NOT NULL,
    status text DEFAULT 'completed'::text NOT NULL,
    employment_scope text DEFAULT 'all'::text NOT NULL,
    strategy_type text,
    shard_id text,
    shard_title text,
    search_query text,
    query_signature text,
    company_scope_json text DEFAULT '[]'::text NOT NULL,
    locations_json text DEFAULT '[]'::text NOT NULL,
    function_ids_json text DEFAULT '[]'::text NOT NULL,
    result_count bigint DEFAULT 0 NOT NULL,
    estimated_total_count bigint DEFAULT 0 NOT NULL,
    provider_cap_hit boolean DEFAULT false NOT NULL,
    source_path text,
    source_job_id text,
    metadata_json text DEFAULT '{}'::text NOT NULL,
    first_seen_at text,
    last_completed_at text,
    materialization_generation_key text,
    materialization_generation_sequence bigint DEFAULT 0 NOT NULL,
    materialization_watermark text,
    created_at text DEFAULT CURRENT_TIMESTAMP,
    updated_at text DEFAULT CURRENT_TIMESTAMP
);

-- Name: acquisition_shard_registry; Type: VIEW; Schema: -; Owner: -

CREATE VIEW acquisition_shard_registry AS
 SELECT acquisition_shard_registry_current.shard_key,
    acquisition_shard_registry_current.target_company,
    acquisition_shard_registry_current.company_key,
    acquisition_shard_registry_current.snapshot_id,
    acquisition_shard_registry_current.asset_view,
    acquisition_shard_registry_current.lane,
    acquisition_shard_registry_current.status,
    acquisition_shard_registry_current.employment_scope,
    acquisition_shard_registry_current.strategy_type,
    acquisition_shard_registry_current.shard_id,
    acquisition_shard_registry_current.shard_title,
    acquisition_shard_registry_current.search_query,
    acquisition_shard_registry_current.query_signature,
    acquisition_shard_registry_current.company_scope_json,
    acquisition_shard_registry_current.locations_json,
    acquisition_shard_registry_current.function_ids_json,
    acquisition_shard_registry_current.result_count,
    acquisition_shard_registry_current.estimated_total_count,
    acquisition_shard_registry_current.provider_cap_hit,
    acquisition_shard_registry_current.source_path,
    acquisition_shard_registry_current.source_job_id,
    acquisition_shard_registry_current.metadata_json,
    acquisition_shard_registry_current.first_seen_at,
    acquisition_shard_registry_current.last_completed_at,
    acquisition_shard_registry_current.materialization_generation_key,
    acquisition_shard_registry_current.materialization_generation_sequence,
    acquisition_shard_registry_current.materialization_watermark,
    acquisition_shard_registry_current.created_at,
    acquisition_shard_registry_current.updated_at
   FROM acquisition_shard_registry_current
UNION ALL
 SELECT acquisition_shard_registry_former.shard_key,
    acquisition_shard_registry_former.target_company,
    acquisition_shard_registry_former.company_key,
    acquisition_shard_registry_former.snapshot_id,
    acquisition_shard_registry_former.asset_view,
    acquisition_shard_registry_former.lane,
    acquisition_shard_registry_former.status,
    acquisition_shard_registry_former.employment_scope,
    acquisition_shard_registry_former.strategy_type,
    acquisition_shard_registry_former.shard_id,
    acquisition_shard_registry_former.shard_title,
    acquisition_shard_registry_former.search_query,
    acquisition_shard_registry_former.query_signature,
    acquisition_shard_registry_former.company_scope_json,
    acquisition_shard_registry_former.locations_json,
    acquisition_shard_registry_former.function_ids_json,
    acquisition_shard_registry_former.result_count,
    acquisition_shard_registry_former.estimated_total_count,
    acquisition_shard_registry_former.provider_cap_hit,
    acquisition_shard_registry_former.source_path,
    acquisition_shard_registry_former.source_job_id,
    acquisition_shard_registry_former.metadata_json,
    acquisition_shard_registry_former.first_seen_at,
    acquisition_shard_registry_former.last_completed_at,
    acquisition_shard_registry_former.materialization_generation_key,
    acquisition_shard_registry_former.materialization_generation_sequence,
    acquisition_shard_registry_former.materialization_watermark,
    acquisition_shard_registry_former.created_at,
    acquisition_shard_registry_former.updated_at
   FROM acquisition_shard_registry_former;

-- Name: agent_actions; Type: TABLE; Schema: -; Owner: -

CREATE TABLE agent_actions (
    action_id text NOT NULL,
    workspace_id text DEFAULT 'default'::text NOT NULL,
    conversation_id text DEFAULT ''::text NOT NULL,
    action_type text NOT NULL,
    owner_module text NOT NULL,
    operation_type text NOT NULL,
    target_ref_json text DEFAULT '{}'::text NOT NULL,
    input_json text DEFAULT '{}'::text NOT NULL,
    approval_status text DEFAULT 'not_required'::text NOT NULL,
    approval_policy text DEFAULT 'not_required'::text NOT NULL,
    budget_json text DEFAULT '{}'::text NOT NULL,
    idempotency_key text NOT NULL,
    status text DEFAULT 'planned'::text NOT NULL,
    result_ref_json text DEFAULT '{}'::text NOT NULL,
    metadata_json text DEFAULT '{}'::text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: agent_runtime_sessions_session_id_seq; Type: SEQUENCE; Schema: -; Owner: -

CREATE SEQUENCE agent_runtime_sessions_session_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Name: agent_runtime_sessions; Type: TABLE; Schema: -; Owner: -

CREATE TABLE agent_runtime_sessions (
    session_id bigint DEFAULT nextval('agent_runtime_sessions_session_id_seq'::regclass) NOT NULL,
    job_id text NOT NULL,
    target_company text,
    request_signature text,
    request_family_signature text,
    runtime_mode text NOT NULL,
    status text NOT NULL,
    lanes_json text NOT NULL,
    metadata_json text,
    created_at text,
    updated_at text
);

-- Name: agent_trace_spans; Type: TABLE; Schema: -; Owner: -

CREATE TABLE agent_trace_spans (
    span_id bigint NOT NULL,
    session_id bigint NOT NULL,
    job_id text NOT NULL,
    parent_span_id bigint,
    lane_id text NOT NULL,
    handoff_from_lane text,
    handoff_to_lane text,
    span_name text NOT NULL,
    stage text NOT NULL,
    status text NOT NULL,
    input_json text,
    output_json text,
    metadata_json text,
    started_at text,
    completed_at text,
    created_at text
);

-- Name: agent_trace_spans_span_id_seq; Type: SEQUENCE; Schema: -; Owner: -

CREATE SEQUENCE agent_trace_spans_span_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Name: agent_worker_runs; Type: TABLE; Schema: -; Owner: -

CREATE TABLE agent_worker_runs (
    worker_id bigint NOT NULL,
    session_id bigint NOT NULL,
    job_id text NOT NULL,
    span_id bigint,
    lane_id text NOT NULL,
    worker_key text NOT NULL,
    status text NOT NULL,
    interrupt_requested bigint NOT NULL,
    budget_json text,
    checkpoint_json text,
    input_json text,
    output_json text,
    metadata_json text,
    lease_owner text,
    lease_expires_at text,
    attempt_count bigint NOT NULL,
    last_error text,
    created_at text,
    updated_at text
);

-- Name: agent_worker_runs_worker_id_seq; Type: SEQUENCE; Schema: -; Owner: -

CREATE SEQUENCE agent_worker_runs_worker_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Name: asset_default_pointer_history; Type: TABLE; Schema: -; Owner: -

CREATE TABLE asset_default_pointer_history (
    history_id text NOT NULL,
    pointer_key text NOT NULL,
    company_key text NOT NULL,
    scope_kind text NOT NULL,
    scope_key text NOT NULL,
    asset_kind text NOT NULL,
    snapshot_id text NOT NULL,
    lifecycle_status text NOT NULL,
    event_type text NOT NULL,
    payload_json text NOT NULL,
    occurred_at text NOT NULL,
    created_at text
);

-- Name: asset_default_pointers; Type: TABLE; Schema: -; Owner: -

CREATE TABLE asset_default_pointers (
    pointer_key text NOT NULL,
    company_key text NOT NULL,
    scope_kind text NOT NULL,
    scope_key text NOT NULL,
    asset_kind text NOT NULL,
    snapshot_id text NOT NULL,
    lifecycle_status text NOT NULL,
    coverage_proof_json text NOT NULL,
    previous_snapshot_id text NOT NULL,
    promoted_by_job_id text NOT NULL,
    promoted_at text NOT NULL,
    metadata_json text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: asset_materialization_generations; Type: TABLE; Schema: -; Owner: -

CREATE TABLE asset_materialization_generations (
    target_company text NOT NULL,
    company_key text,
    snapshot_id text NOT NULL,
    asset_view text NOT NULL,
    artifact_kind text NOT NULL,
    artifact_key text NOT NULL,
    generation_key text NOT NULL,
    generation_sequence bigint NOT NULL,
    source_path text NOT NULL,
    payload_signature text NOT NULL,
    member_signature text NOT NULL,
    member_count bigint NOT NULL,
    summary_json text NOT NULL,
    metadata_json text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: asset_membership_index; Type: TABLE; Schema: -; Owner: -

CREATE TABLE asset_membership_index (
    generation_key text NOT NULL,
    target_company text NOT NULL,
    snapshot_id text NOT NULL,
    asset_view text NOT NULL,
    artifact_kind text NOT NULL,
    artifact_key text NOT NULL,
    lane text NOT NULL,
    employment_scope text NOT NULL,
    member_key text NOT NULL,
    member_key_kind text NOT NULL,
    candidate_id text NOT NULL,
    profile_url_key text NOT NULL,
    metadata_json text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: candidate_evidence_index; Type: TABLE; Schema: -; Owner: -

CREATE TABLE candidate_evidence_index (
    person_identity_key text NOT NULL,
    indexed_text text NOT NULL,
    evidence_terms_json text NOT NULL,
    assertion_terms_json text NOT NULL,
    source_evidence_ids_json text NOT NULL,
    source_assertion_ids_json text NOT NULL,
    indexed_field_sources_json text NOT NULL,
    evidence_index_watermark text NOT NULL,
    evidence_indexed_at text NOT NULL,
    metadata_json text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: candidate_materialization_state; Type: TABLE; Schema: -; Owner: -

CREATE TABLE candidate_materialization_state (
    target_company text NOT NULL,
    company_key text,
    snapshot_id text NOT NULL,
    asset_view text NOT NULL,
    candidate_id text NOT NULL,
    fingerprint text NOT NULL,
    shard_path text NOT NULL,
    list_page bigint NOT NULL,
    dirty_reason text NOT NULL,
    materialized_at text,
    metadata_json text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: candidate_review_registry; Type: TABLE; Schema: -; Owner: -

CREATE TABLE candidate_review_registry (
    record_id text NOT NULL,
    job_id text NOT NULL,
    history_id text NOT NULL,
    candidate_id text NOT NULL,
    candidate_name text NOT NULL,
    headline text,
    current_company text,
    avatar_url text,
    linkedin_url text,
    primary_email text,
    status text NOT NULL,
    comment text NOT NULL,
    source text NOT NULL,
    metadata_json text NOT NULL,
    added_at text,
    updated_at text
);

-- Name: candidates; Type: TABLE; Schema: -; Owner: -

CREATE TABLE candidates (
    candidate_id text NOT NULL,
    name_en text NOT NULL,
    name_zh text,
    display_name text,
    category text,
    target_company text,
    organization text,
    employment_status text,
    role text,
    team text,
    joined_at text,
    left_at text,
    current_destination text,
    ethnicity_background text,
    investment_involvement text,
    focus_areas text,
    education text,
    work_history text,
    notes text,
    linkedin_url text,
    media_url text,
    source_dataset text,
    source_path text,
    metadata_json text
);

-- Name: cloud_asset_operation_ledger_ledger_id_seq; Type: SEQUENCE; Schema: -; Owner: -

CREATE SEQUENCE cloud_asset_operation_ledger_ledger_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Name: cloud_asset_operation_ledger; Type: TABLE; Schema: -; Owner: -

CREATE TABLE cloud_asset_operation_ledger (
    ledger_id bigint DEFAULT nextval('cloud_asset_operation_ledger_ledger_id_seq'::regclass) NOT NULL,
    operation_type text NOT NULL,
    bundle_kind text NOT NULL,
    bundle_id text NOT NULL,
    sync_run_id text,
    status text NOT NULL,
    manifest_path text,
    target_runtime_dir text,
    target_db_path text,
    scoped_companies_json text NOT NULL,
    scoped_snapshot_id text,
    summary_json text NOT NULL,
    metadata_json text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: collection_authoritative_pointers; Type: TABLE; Schema: -; Owner: -

CREATE TABLE collection_authoritative_pointers (
    collection_id text NOT NULL,
    active_projection_id text NOT NULL,
    active_collection_version text NOT NULL,
    previous_projection_id text NOT NULL,
    state text NOT NULL,
    writer_id text NOT NULL,
    metadata_json text NOT NULL,
    published_at text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: company_assertions; Type: TABLE; Schema: -; Owner: -

CREATE TABLE company_assertions (
    assertion_id text NOT NULL,
    workspace_id text DEFAULT 'default'::text NOT NULL,
    company_key text DEFAULT ''::text NOT NULL,
    target_company text DEFAULT ''::text NOT NULL,
    assertion_type text DEFAULT ''::text NOT NULL,
    value text DEFAULT ''::text NOT NULL,
    normalized_value text DEFAULT ''::text NOT NULL,
    authority text DEFAULT 'provider_observed'::text NOT NULL,
    verification_status text DEFAULT 'needs_review'::text NOT NULL,
    source_evidence_id text DEFAULT ''::text NOT NULL,
    source_run_id text DEFAULT ''::text NOT NULL,
    source_command_id text DEFAULT ''::text NOT NULL,
    confidence_score double precision DEFAULT 0 NOT NULL,
    valid_from text DEFAULT ''::text NOT NULL,
    valid_to text DEFAULT ''::text NOT NULL,
    metadata_json text DEFAULT '{}'::text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: company_assets; Type: TABLE; Schema: -; Owner: -

CREATE TABLE company_assets (
    asset_id text NOT NULL,
    workspace_id text DEFAULT 'default'::text NOT NULL,
    company_key text DEFAULT ''::text NOT NULL,
    target_company text DEFAULT ''::text NOT NULL,
    asset_type text DEFAULT ''::text NOT NULL,
    source_kind text DEFAULT ''::text NOT NULL,
    source_run_id text DEFAULT ''::text NOT NULL,
    source_command_id text DEFAULT ''::text NOT NULL,
    activity_run_id text DEFAULT ''::text NOT NULL,
    content_ref text DEFAULT ''::text NOT NULL,
    content_hash text DEFAULT ''::text NOT NULL,
    source_url text DEFAULT ''::text NOT NULL,
    fetched_at text DEFAULT ''::text NOT NULL,
    visibility_scope text DEFAULT 'internal'::text NOT NULL,
    status text DEFAULT 'available'::text NOT NULL,
    metadata_json text DEFAULT '{}'::text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: company_evidence; Type: TABLE; Schema: -; Owner: -

CREATE TABLE company_evidence (
    evidence_id text NOT NULL,
    workspace_id text DEFAULT 'default'::text NOT NULL,
    company_key text DEFAULT ''::text NOT NULL,
    target_company text DEFAULT ''::text NOT NULL,
    asset_id text DEFAULT ''::text NOT NULL,
    evidence_type text DEFAULT ''::text NOT NULL,
    value text DEFAULT ''::text NOT NULL,
    normalized_value text DEFAULT ''::text NOT NULL,
    source_url text DEFAULT ''::text NOT NULL,
    source_domain text DEFAULT ''::text NOT NULL,
    confidence_score double precision DEFAULT 0 NOT NULL,
    evidence_excerpt text DEFAULT ''::text NOT NULL,
    artifact_refs_json text DEFAULT '{}'::text NOT NULL,
    status text DEFAULT 'observed'::text NOT NULL,
    metadata_json text DEFAULT '{}'::text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: company_public_web_asset_runs; Type: TABLE; Schema: -; Owner: -

CREATE TABLE company_public_web_asset_runs (
    run_id text NOT NULL,
    target_company text NOT NULL,
    company_key text NOT NULL,
    idempotency_key text NOT NULL,
    status text NOT NULL,
    phase text NOT NULL,
    source_families_json text NOT NULL,
    seed_urls_json text NOT NULL,
    options_json text NOT NULL,
    discovered_assets_json text NOT NULL,
    summary_json text NOT NULL,
    artifact_root text NOT NULL,
    requested_by text NOT NULL,
    force_refresh bigint NOT NULL,
    started_at text,
    completed_at text,
    last_error text NOT NULL,
    metadata_json text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: company_public_web_assets; Type: TABLE; Schema: -; Owner: -

CREATE TABLE company_public_web_assets (
    asset_id text NOT NULL,
    company_key text NOT NULL,
    target_company text NOT NULL,
    latest_run_id text NOT NULL,
    source_family text NOT NULL,
    asset_kind text NOT NULL,
    title text NOT NULL,
    url text NOT NULL,
    normalized_url_key text NOT NULL,
    summary text NOT NULL,
    model_safe_payload_json text NOT NULL,
    source_run_ids_json text NOT NULL,
    artifact_refs_json text NOT NULL,
    status text NOT NULL,
    metadata_json text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: confidence_policy_controls_control_id_seq; Type: SEQUENCE; Schema: -; Owner: -

CREATE SEQUENCE confidence_policy_controls_control_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Name: confidence_policy_controls; Type: TABLE; Schema: -; Owner: -

CREATE TABLE confidence_policy_controls (
    control_id bigint DEFAULT nextval('confidence_policy_controls_control_id_seq'::regclass) NOT NULL,
    target_company text,
    request_signature text,
    request_family_signature text,
    matching_request_signature text,
    matching_request_family_signature text,
    scope_kind text NOT NULL,
    control_mode text NOT NULL,
    status text NOT NULL,
    high_threshold double precision NOT NULL,
    medium_threshold double precision NOT NULL,
    reviewer text,
    notes text,
    locked_policy_json text,
    created_at text,
    updated_at text
);

-- Name: confidence_policy_runs_policy_run_id_seq; Type: SEQUENCE; Schema: -; Owner: -

CREATE SEQUENCE confidence_policy_runs_policy_run_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Name: confidence_policy_runs; Type: TABLE; Schema: -; Owner: -

CREATE TABLE confidence_policy_runs (
    policy_run_id bigint DEFAULT nextval('confidence_policy_runs_policy_run_id_seq'::regclass) NOT NULL,
    target_company text,
    job_id text,
    criteria_version_id bigint,
    trigger_feedback_id bigint,
    request_signature text,
    request_family_signature text,
    matching_request_signature text,
    matching_request_family_signature text,
    scope_kind text NOT NULL,
    high_threshold double precision NOT NULL,
    medium_threshold double precision NOT NULL,
    summary_json text NOT NULL,
    policy_json text NOT NULL,
    created_at text
);

-- Name: criteria_compiler_runs_compiler_run_id_seq; Type: SEQUENCE; Schema: -; Owner: -

CREATE SEQUENCE criteria_compiler_runs_compiler_run_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Name: criteria_compiler_runs; Type: TABLE; Schema: -; Owner: -

CREATE TABLE criteria_compiler_runs (
    compiler_run_id bigint DEFAULT nextval('criteria_compiler_runs_compiler_run_id_seq'::regclass) NOT NULL,
    version_id bigint,
    job_id text,
    trigger_feedback_id bigint,
    provider_name text NOT NULL,
    compiler_kind text NOT NULL,
    status text NOT NULL,
    input_json text NOT NULL,
    output_json text NOT NULL,
    notes text,
    created_at text
);

-- Name: criteria_feedback_feedback_id_seq; Type: SEQUENCE; Schema: -; Owner: -

CREATE SEQUENCE criteria_feedback_feedback_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Name: criteria_feedback; Type: TABLE; Schema: -; Owner: -

CREATE TABLE criteria_feedback (
    feedback_id bigint DEFAULT nextval('criteria_feedback_feedback_id_seq'::regclass) NOT NULL,
    job_id text,
    candidate_id text,
    target_company text,
    feedback_type text NOT NULL,
    subject text,
    value text,
    reviewer text,
    notes text,
    payload_json text,
    created_at text
);

-- Name: criteria_pattern_suggestions_suggestion_id_seq; Type: SEQUENCE; Schema: -; Owner: -

CREATE SEQUENCE criteria_pattern_suggestions_suggestion_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Name: criteria_pattern_suggestions; Type: TABLE; Schema: -; Owner: -

CREATE TABLE criteria_pattern_suggestions (
    suggestion_id bigint DEFAULT nextval('criteria_pattern_suggestions_suggestion_id_seq'::regclass) NOT NULL,
    target_company text,
    request_signature text,
    request_family_signature text,
    matching_request_signature text,
    matching_request_family_signature text,
    source_feedback_id bigint,
    source_job_id text,
    candidate_id text NOT NULL,
    pattern_type text NOT NULL,
    subject text,
    value text,
    status text NOT NULL,
    confidence text NOT NULL,
    rationale text,
    evidence_json text,
    metadata_json text,
    reviewed_by text,
    review_notes text,
    applied_pattern_id bigint,
    reviewed_at text,
    created_at text,
    updated_at text
);

-- Name: criteria_patterns_pattern_id_seq; Type: SEQUENCE; Schema: -; Owner: -

CREATE SEQUENCE criteria_patterns_pattern_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Name: criteria_patterns; Type: TABLE; Schema: -; Owner: -

CREATE TABLE criteria_patterns (
    pattern_id bigint DEFAULT nextval('criteria_patterns_pattern_id_seq'::regclass) NOT NULL,
    target_company text,
    pattern_type text NOT NULL,
    subject text,
    value text,
    status text NOT NULL,
    confidence text NOT NULL,
    source_feedback_id bigint,
    metadata_json text,
    created_at text,
    updated_at text
);

-- Name: criteria_result_diffs_diff_id_seq; Type: SEQUENCE; Schema: -; Owner: -

CREATE SEQUENCE criteria_result_diffs_diff_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Name: criteria_result_diffs; Type: TABLE; Schema: -; Owner: -

CREATE TABLE criteria_result_diffs (
    diff_id bigint DEFAULT nextval('criteria_result_diffs_diff_id_seq'::regclass) NOT NULL,
    target_company text,
    trigger_feedback_id bigint,
    criteria_version_id bigint,
    baseline_job_id text,
    rerun_job_id text,
    summary_json text NOT NULL,
    diff_json text NOT NULL,
    artifact_path text,
    created_at text
);

-- Name: criteria_versions_version_id_seq; Type: SEQUENCE; Schema: -; Owner: -

CREATE SEQUENCE criteria_versions_version_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Name: criteria_versions; Type: TABLE; Schema: -; Owner: -

CREATE TABLE criteria_versions (
    version_id bigint DEFAULT nextval('criteria_versions_version_id_seq'::regclass) NOT NULL,
    target_company text,
    request_signature text NOT NULL,
    source_kind text NOT NULL,
    parent_version_id bigint,
    trigger_feedback_id bigint,
    evolution_stage text NOT NULL,
    request_json text NOT NULL,
    plan_json text NOT NULL,
    patterns_json text,
    notes text,
    created_at text
);

-- Name: crm_engagements; Type: TABLE; Schema: -; Owner: -

CREATE TABLE crm_engagements (
    engagement_id text NOT NULL,
    crm_record_id text NOT NULL,
    pipeline_id text NOT NULL,
    stage text NOT NULL,
    stage_category text NOT NULL,
    priority text NOT NULL,
    quality_score double precision,
    next_action_at text NOT NULL,
    last_contacted_at text NOT NULL,
    source_projection_id text NOT NULL,
    source_run_id text NOT NULL,
    source_selection_reason text NOT NULL,
    created_by_actor text NOT NULL,
    metadata_json text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: crm_events; Type: TABLE; Schema: -; Owner: -

CREATE TABLE crm_events (
    event_id text NOT NULL,
    workspace_id text NOT NULL,
    crm_record_id text NOT NULL,
    engagement_id text NOT NULL,
    person_identity_key text NOT NULL,
    event_type text NOT NULL,
    actor_type text NOT NULL,
    actor_id text NOT NULL,
    idempotency_key text NOT NULL,
    payload_json text NOT NULL,
    metadata_json text NOT NULL,
    occurred_at text NOT NULL,
    created_at text
);

-- Name: crm_public_web_batches; Type: TABLE; Schema: -; Owner: -

CREATE TABLE crm_public_web_batches (
    batch_id text NOT NULL,
    idempotency_key text NOT NULL,
    workspace_id text NOT NULL,
    status text NOT NULL,
    requested_crm_record_ids_json text NOT NULL,
    source_families_json text NOT NULL,
    options_json text NOT NULL,
    run_ids_json text NOT NULL,
    summary_json text NOT NULL,
    metadata_json text NOT NULL,
    requested_by text NOT NULL,
    force_refresh bigint NOT NULL,
    execution_backend text NOT NULL,
    source_target_batch_id text NOT NULL,
    started_at text,
    completed_at text,
    created_at text,
    updated_at text
);

-- Name: crm_public_web_promotions; Type: TABLE; Schema: -; Owner: -

CREATE TABLE crm_public_web_promotions (
    promotion_id text NOT NULL,
    signal_id text NOT NULL,
    run_id text NOT NULL,
    asset_id text NOT NULL,
    person_identity_key text NOT NULL,
    crm_record_id text NOT NULL,
    workspace_id text NOT NULL,
    candidate_id text NOT NULL,
    candidate_name text NOT NULL,
    current_company text NOT NULL,
    linkedin_url_key text NOT NULL,
    signal_kind text NOT NULL,
    signal_type text NOT NULL,
    email_type text NOT NULL,
    value text NOT NULL,
    normalized_value text NOT NULL,
    url text NOT NULL,
    source_url text NOT NULL,
    source_domain text NOT NULL,
    source_family text NOT NULL,
    source_title text NOT NULL,
    confidence_label text NOT NULL,
    confidence_score double precision NOT NULL,
    identity_match_label text NOT NULL,
    identity_match_score double precision NOT NULL,
    publishable bigint NOT NULL,
    clean_profile_link bigint NOT NULL,
    link_shape_warnings_json text NOT NULL,
    action text NOT NULL,
    promotion_status text NOT NULL,
    promoted_field text NOT NULL,
    previous_value text NOT NULL,
    new_value text NOT NULL,
    operator text NOT NULL,
    note text NOT NULL,
    evidence_excerpt text NOT NULL,
    execution_backend text NOT NULL,
    source_target_promotion_id text NOT NULL,
    metadata_json text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: crm_public_web_runs; Type: TABLE; Schema: -; Owner: -

CREATE TABLE crm_public_web_runs (
    run_id text NOT NULL,
    batch_id text NOT NULL,
    crm_record_id text NOT NULL,
    workspace_id text NOT NULL,
    candidate_id text NOT NULL,
    candidate_name text NOT NULL,
    current_company text NOT NULL,
    linkedin_url text NOT NULL,
    linkedin_url_key text NOT NULL,
    person_identity_key text NOT NULL,
    idempotency_key text NOT NULL,
    status text NOT NULL,
    phase text NOT NULL,
    source_families_json text NOT NULL,
    options_json text NOT NULL,
    query_manifest_json text NOT NULL,
    search_checkpoint_json text NOT NULL,
    fetch_checkpoint_json text NOT NULL,
    analysis_checkpoint_json text NOT NULL,
    summary_json text NOT NULL,
    artifact_root text NOT NULL,
    worker_key text NOT NULL,
    lease_owner text NOT NULL,
    lease_expires_at text,
    attempt_count bigint NOT NULL,
    last_error text NOT NULL,
    execution_backend text NOT NULL,
    source_target_run_id text NOT NULL,
    started_at text,
    completed_at text,
    created_at text,
    updated_at text
);

-- Name: crm_records; Type: TABLE; Schema: -; Owner: -

CREATE TABLE crm_records (
    crm_record_id text NOT NULL,
    workspace_id text NOT NULL,
    person_identity_key text NOT NULL,
    candidate_identity_key text NOT NULL,
    collection_id text NOT NULL,
    display_name_cache text NOT NULL,
    headline_cache text NOT NULL,
    primary_company_cache text NOT NULL,
    avatar_asset_id text NOT NULL,
    lifecycle_status text NOT NULL,
    visibility_status text NOT NULL,
    owner_user_id text NOT NULL,
    source_projection_id text NOT NULL,
    source_run_id text NOT NULL,
    source_collection_id text NOT NULL,
    source_reason text NOT NULL,
    current_engagement_id text NOT NULL,
    crm_version bigint NOT NULL,
    metadata_json text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: crm_tasks; Type: TABLE; Schema: -; Owner: -

CREATE TABLE crm_tasks (
    task_id text NOT NULL,
    workspace_id text DEFAULT 'default'::text NOT NULL,
    crm_record_id text DEFAULT ''::text NOT NULL,
    engagement_id text DEFAULT ''::text NOT NULL,
    person_identity_key text DEFAULT ''::text NOT NULL,
    title text DEFAULT ''::text NOT NULL,
    description text DEFAULT ''::text NOT NULL,
    status text DEFAULT 'open'::text NOT NULL,
    priority text DEFAULT 'normal'::text NOT NULL,
    due_at text DEFAULT ''::text NOT NULL,
    completed_at text DEFAULT ''::text NOT NULL,
    created_by_actor text DEFAULT ''::text NOT NULL,
    created_by_actor_id text DEFAULT ''::text NOT NULL,
    source_event_id text DEFAULT ''::text NOT NULL,
    idempotency_key text DEFAULT ''::text NOT NULL,
    metadata_json text DEFAULT '{}'::text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: evidence; Type: TABLE; Schema: -; Owner: -

CREATE TABLE evidence (
    evidence_id text NOT NULL,
    candidate_id text NOT NULL,
    source_type text,
    title text,
    url text,
    summary text,
    source_dataset text,
    source_path text,
    metadata_json text
);

-- Name: frontend_history_links; Type: TABLE; Schema: -; Owner: -

CREATE TABLE frontend_history_links (
    history_id text NOT NULL,
    query_text text NOT NULL,
    target_company text NOT NULL,
    review_id bigint NOT NULL,
    job_id text NOT NULL,
    phase text NOT NULL,
    request_json text NOT NULL,
    plan_json text NOT NULL,
    metadata_json text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: job_board_visible_patches; Type: TABLE; Schema: -; Owner: -

CREATE TABLE job_board_visible_patches (
    patch_id text NOT NULL,
    job_id text NOT NULL,
    target_company text NOT NULL,
    company_key text NOT NULL,
    snapshot_id text NOT NULL,
    baseline_snapshot_id text NOT NULL,
    asset_view text NOT NULL,
    patch_kind text NOT NULL,
    patch_phase text NOT NULL,
    source text NOT NULL,
    reason text NOT NULL,
    sequence_index bigint NOT NULL,
    candidate_count bigint NOT NULL,
    cumulative_candidate_count bigint NOT NULL,
    served_candidate_count bigint NOT NULL,
    result_view_id text NOT NULL,
    serving_projection_id text NOT NULL,
    serving_projection_phase text NOT NULL,
    overlay_path text NOT NULL,
    candidate_ids_json text NOT NULL,
    cumulative_candidate_ids_json text NOT NULL,
    metadata_json text NOT NULL,
    published_at text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: job_events_event_id_seq; Type: SEQUENCE; Schema: -; Owner: -

CREATE SEQUENCE job_events_event_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Name: job_events; Type: TABLE; Schema: -; Owner: -

CREATE TABLE job_events (
    event_id bigint DEFAULT nextval('job_events_event_id_seq'::regclass) NOT NULL,
    job_id text NOT NULL,
    stage text NOT NULL,
    status text NOT NULL,
    detail text,
    payload_json text,
    created_at text
);

-- Name: job_materialization_items; Type: TABLE; Schema: -; Owner: -

CREATE TABLE job_materialization_items (
    item_id text NOT NULL,
    job_id text NOT NULL,
    target_company text NOT NULL,
    company_key text NOT NULL,
    snapshot_id text NOT NULL,
    baseline_snapshot_id text NOT NULL,
    asset_view text NOT NULL,
    item_kind text NOT NULL,
    source text NOT NULL,
    reason text NOT NULL,
    status text NOT NULL,
    phase text NOT NULL,
    priority bigint NOT NULL,
    attempt_count bigint NOT NULL,
    max_attempts bigint NOT NULL,
    candidate_count bigint NOT NULL,
    candidate_ids_json text NOT NULL,
    source_worker_ids_json text NOT NULL,
    idempotency_key text NOT NULL,
    result_patch_id text NOT NULL,
    result_view_id text NOT NULL,
    serving_projection_id text NOT NULL,
    lease_owner text NOT NULL,
    lease_expires_at text NOT NULL,
    not_before_at text NOT NULL,
    last_error text NOT NULL,
    metadata_json text NOT NULL,
    completed_at text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: job_progress_event_summaries; Type: TABLE; Schema: -; Owner: -

CREATE TABLE job_progress_event_summaries (
    job_id text NOT NULL,
    event_count bigint NOT NULL,
    latest_event_json text NOT NULL,
    stage_sequence_json text NOT NULL,
    stage_stats_json text NOT NULL,
    latest_metrics_json text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: job_result_lifecycle; Type: TABLE; Schema: -; Owner: -

CREATE TABLE job_result_lifecycle (
    job_id text NOT NULL,
    view_id text NOT NULL,
    company_key text NOT NULL,
    target_company text NOT NULL,
    workflow_kind text NOT NULL,
    phase text NOT NULL,
    phase_status text NOT NULL,
    state text NOT NULL,
    baseline_snapshot_id text NOT NULL,
    current_snapshot_id text NOT NULL,
    served_snapshot_id text NOT NULL,
    served_generation_key text NOT NULL,
    serving_projection_id text NOT NULL,
    serving_projection_phase text NOT NULL,
    baseline_candidate_count bigint NOT NULL,
    expected_candidate_count bigint NOT NULL,
    served_candidate_count bigint NOT NULL,
    delta_profile_required_count bigint NOT NULL,
    delta_profile_fetched_count bigint NOT NULL,
    delta_profile_applied_count bigint NOT NULL,
    delta_profile_materialized_count bigint NOT NULL,
    delta_profile_board_visible_count bigint NOT NULL,
    stage1_current_search_returned_count bigint NOT NULL,
    stage1_former_search_returned_count bigint NOT NULL,
    stage1_all_search_returned_count bigint NOT NULL,
    stage1_deduped_candidate_count bigint NOT NULL,
    stage1_deduped_profile_url_count bigint NOT NULL,
    stage1_profile_fetch_required_count bigint NOT NULL,
    stage1_profile_fetched_count bigint NOT NULL,
    delta_profile_progress_applicable bigint NOT NULL,
    delta_profile_progress_reason text NOT NULL,
    background_snapshot_materialization_status text NOT NULL,
    outreach_layering_status text NOT NULL,
    source_validation_status text NOT NULL,
    last_event_id bigint NOT NULL,
    projection_source_snapshot_id text NOT NULL,
    metadata_json text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: job_result_views; Type: TABLE; Schema: -; Owner: -

CREATE TABLE job_result_views (
    view_id text NOT NULL,
    job_id text NOT NULL,
    target_company text NOT NULL,
    company_key text,
    source_kind text NOT NULL,
    view_kind text NOT NULL,
    snapshot_id text NOT NULL,
    asset_view text NOT NULL,
    source_path text NOT NULL,
    authoritative_snapshot_id text NOT NULL,
    materialization_generation_key text NOT NULL,
    request_signature text NOT NULL,
    summary_json text NOT NULL,
    metadata_json text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: job_results; Type: TABLE; Schema: -; Owner: -

CREATE TABLE job_results (
    job_id text NOT NULL,
    candidate_id text NOT NULL,
    rank_index bigint NOT NULL,
    score double precision NOT NULL,
    confidence_label text,
    confidence_score double precision,
    confidence_reason text,
    explanation text,
    matched_fields_json text
);

-- Name: jobs; Type: TABLE; Schema: -; Owner: -

CREATE TABLE jobs (
    job_id text NOT NULL,
    job_type text NOT NULL,
    status text NOT NULL,
    stage text NOT NULL,
    request_json text NOT NULL,
    plan_json text,
    execution_bundle_json text NOT NULL,
    matching_request_json text NOT NULL,
    summary_json text,
    artifact_path text,
    request_signature text,
    request_family_signature text,
    matching_request_signature text,
    matching_request_family_signature text,
    requester_id text,
    tenant_id text,
    idempotency_key text,
    created_at text,
    updated_at text
);

-- Name: linkedin_profile_registry; Type: TABLE; Schema: -; Owner: -

CREATE TABLE linkedin_profile_registry (
    profile_url_key text NOT NULL,
    profile_url text NOT NULL,
    raw_linkedin_url text,
    sanity_linkedin_url text,
    status text NOT NULL,
    retry_count bigint NOT NULL,
    last_error text,
    last_run_id text,
    last_dataset_id text,
    last_snapshot_dir text,
    last_raw_path text,
    first_queued_at text,
    last_queued_at text,
    last_fetched_at text,
    last_failed_at text,
    source_shards_json text NOT NULL,
    source_jobs_json text NOT NULL,
    refill_queue_state text,
    last_refill_trigger_kind text,
    last_refill_plan_reason text,
    last_refill_deferred_reason text,
    last_refill_planned_at text,
    refill_not_before_at text,
    refill_plan_batch_size bigint NOT NULL,
    refill_plan_batch_count bigint NOT NULL,
    refill_plan_window_url_count bigint NOT NULL,
    last_refill_attempt_count bigint NOT NULL,
    refill_owner_worker_id bigint NOT NULL,
    refill_owner_run_id text,
    refill_owner_dataset_id text,
    refill_owner_payload_hash text,
    refill_terminal_status text,
    refill_terminal_at text,
    created_at text,
    updated_at text
);

-- Name: linkedin_profile_registry_aliases; Type: TABLE; Schema: -; Owner: -

CREATE TABLE linkedin_profile_registry_aliases (
    alias_url_key text NOT NULL,
    profile_url_key text NOT NULL,
    alias_url text NOT NULL,
    alias_kind text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: linkedin_profile_registry_backfill_runs; Type: TABLE; Schema: -; Owner: -

CREATE TABLE linkedin_profile_registry_backfill_runs (
    run_key text NOT NULL,
    scope_company text,
    scope_snapshot_id text,
    checkpoint_json text NOT NULL,
    summary_json text NOT NULL,
    status text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: linkedin_profile_registry_events_event_id_seq; Type: SEQUENCE; Schema: -; Owner: -

CREATE SEQUENCE linkedin_profile_registry_events_event_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Name: linkedin_profile_registry_events; Type: TABLE; Schema: -; Owner: -

CREATE TABLE linkedin_profile_registry_events (
    event_id bigint DEFAULT nextval('linkedin_profile_registry_events_event_id_seq'::regclass) NOT NULL,
    profile_url_key text NOT NULL,
    event_type text NOT NULL,
    event_status text,
    detail text,
    run_id text,
    dataset_id text,
    metadata_json text,
    duration_ms bigint,
    created_at text
);

-- Name: linkedin_profile_registry_leases; Type: TABLE; Schema: -; Owner: -

CREATE TABLE linkedin_profile_registry_leases (
    profile_url_key text NOT NULL,
    lease_owner text NOT NULL,
    lease_token text NOT NULL,
    lease_expires_at text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: manual_review_items_review_item_id_seq; Type: SEQUENCE; Schema: -; Owner: -

CREATE SEQUENCE manual_review_items_review_item_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Name: manual_review_items; Type: TABLE; Schema: -; Owner: -

CREATE TABLE manual_review_items (
    review_item_id bigint DEFAULT nextval('manual_review_items_review_item_id_seq'::regclass) NOT NULL,
    job_id text,
    candidate_id text,
    target_company text,
    review_type text NOT NULL,
    priority text NOT NULL,
    status text NOT NULL,
    summary text,
    candidate_json text NOT NULL,
    evidence_json text,
    metadata_json text,
    reviewed_by text,
    review_notes text,
    reviewed_at text,
    created_at text,
    updated_at text
);

-- Name: operation_events; Type: TABLE; Schema: -; Owner: -

CREATE TABLE operation_events (
    event_id text NOT NULL,
    workspace_id text DEFAULT 'default'::text NOT NULL,
    event_stream_id text NOT NULL,
    operation_run_id text DEFAULT ''::text NOT NULL,
    action_id text DEFAULT ''::text NOT NULL,
    event_family text NOT NULL,
    event_type text NOT NULL,
    sequence_number bigint DEFAULT 0 NOT NULL,
    idempotency_key text NOT NULL,
    occurred_at text DEFAULT ''::text NOT NULL,
    recorded_at text DEFAULT ''::text NOT NULL,
    actor text DEFAULT ''::text NOT NULL,
    source text DEFAULT ''::text NOT NULL,
    payload_json text DEFAULT '{}'::text NOT NULL,
    schema_version text DEFAULT 'operation_event_v1'::text NOT NULL,
    created_at text
);

-- Name: operation_runs; Type: TABLE; Schema: -; Owner: -

CREATE TABLE operation_runs (
    operation_run_id text NOT NULL,
    workspace_id text DEFAULT 'default'::text NOT NULL,
    action_id text NOT NULL,
    owner_module text NOT NULL,
    operation_type text NOT NULL,
    status text DEFAULT 'queued'::text NOT NULL,
    progress_json text DEFAULT '{}'::text NOT NULL,
    workflow_ref_json text DEFAULT '{}'::text NOT NULL,
    cost_budget_json text DEFAULT '{}'::text NOT NULL,
    idempotency_key text NOT NULL,
    result_ref_json text DEFAULT '{}'::text NOT NULL,
    metadata_json text DEFAULT '{}'::text NOT NULL,
    started_at text DEFAULT ''::text NOT NULL,
    completed_at text DEFAULT ''::text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: organization_asset_registry_registry_id_seq; Type: SEQUENCE; Schema: -; Owner: -

CREATE SEQUENCE organization_asset_registry_registry_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Name: organization_asset_registry; Type: TABLE; Schema: -; Owner: -

CREATE TABLE organization_asset_registry (
    registry_id bigint DEFAULT nextval('organization_asset_registry_registry_id_seq'::regclass) NOT NULL,
    target_company text NOT NULL,
    company_key text,
    snapshot_id text NOT NULL,
    asset_view text NOT NULL,
    status text NOT NULL,
    authoritative bigint NOT NULL,
    candidate_count bigint NOT NULL,
    evidence_count bigint NOT NULL,
    profile_detail_count bigint NOT NULL,
    explicit_profile_capture_count bigint NOT NULL,
    missing_linkedin_count bigint NOT NULL,
    manual_review_backlog_count bigint NOT NULL,
    profile_completion_backlog_count bigint NOT NULL,
    source_snapshot_count bigint NOT NULL,
    completeness_score double precision NOT NULL,
    completeness_band text NOT NULL,
    current_lane_coverage_json text NOT NULL,
    former_lane_coverage_json text NOT NULL,
    current_lane_effective_candidate_count bigint NOT NULL,
    former_lane_effective_candidate_count bigint NOT NULL,
    current_lane_effective_ready bigint NOT NULL,
    former_lane_effective_ready bigint NOT NULL,
    source_snapshot_selection_json text NOT NULL,
    selected_snapshot_ids_json text NOT NULL,
    source_path text,
    source_job_id text,
    summary_json text NOT NULL,
    created_at text,
    updated_at text,
    materialization_generation_key text,
    materialization_generation_sequence bigint NOT NULL,
    materialization_watermark text
);

-- Name: organization_execution_profiles_profile_id_seq; Type: SEQUENCE; Schema: -; Owner: -

CREATE SEQUENCE organization_execution_profiles_profile_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Name: organization_execution_profiles; Type: TABLE; Schema: -; Owner: -

CREATE TABLE organization_execution_profiles (
    profile_id bigint DEFAULT nextval('organization_execution_profiles_profile_id_seq'::regclass) NOT NULL,
    target_company text NOT NULL,
    company_key text,
    asset_view text NOT NULL,
    source_registry_id bigint NOT NULL,
    source_snapshot_id text,
    source_job_id text,
    source_generation_key text,
    source_generation_sequence bigint NOT NULL,
    source_generation_watermark text,
    status text NOT NULL,
    org_scale_band text NOT NULL,
    default_acquisition_mode text NOT NULL,
    prefer_delta_from_baseline bigint NOT NULL,
    current_lane_default text NOT NULL,
    former_lane_default text NOT NULL,
    baseline_candidate_count bigint NOT NULL,
    current_lane_effective_candidate_count bigint NOT NULL,
    former_lane_effective_candidate_count bigint NOT NULL,
    completeness_score double precision NOT NULL,
    completeness_band text NOT NULL,
    profile_detail_ratio double precision NOT NULL,
    company_employee_shard_count bigint NOT NULL,
    current_profile_search_shard_count bigint NOT NULL,
    former_profile_search_shard_count bigint NOT NULL,
    company_employee_cap_hit_count bigint NOT NULL,
    profile_search_cap_hit_count bigint NOT NULL,
    reason_codes_json text NOT NULL,
    explanation_json text NOT NULL,
    summary_json text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: person_assertions; Type: TABLE; Schema: -; Owner: -

CREATE TABLE person_assertions (
    assertion_id text NOT NULL,
    person_identity_key text NOT NULL,
    assertion_type text NOT NULL,
    value text NOT NULL,
    normalized_value text NOT NULL,
    authority text NOT NULL,
    verification_status text NOT NULL,
    source_evidence_id text NOT NULL,
    source_crm_event_id text NOT NULL,
    source_run_id text NOT NULL,
    confidence_score double precision NOT NULL,
    valid_from text NOT NULL,
    valid_to text NOT NULL,
    metadata_json text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: person_assets; Type: TABLE; Schema: -; Owner: -

CREATE TABLE person_assets (
    asset_id text NOT NULL,
    person_identity_key text NOT NULL,
    asset_type text NOT NULL,
    source_kind text NOT NULL,
    source_run_id text NOT NULL,
    source_projection_id text NOT NULL,
    content_ref text NOT NULL,
    content_hash text NOT NULL,
    source_url text NOT NULL,
    fetched_at text NOT NULL,
    visibility_scope text NOT NULL,
    status text NOT NULL,
    metadata_json text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: person_evidence; Type: TABLE; Schema: -; Owner: -

CREATE TABLE person_evidence (
    evidence_id text NOT NULL,
    person_identity_key text NOT NULL,
    asset_id text NOT NULL,
    evidence_type text NOT NULL,
    value text NOT NULL,
    normalized_value text NOT NULL,
    source_url text NOT NULL,
    source_domain text NOT NULL,
    confidence_score double precision NOT NULL,
    identity_match_score double precision NOT NULL,
    publishable bigint NOT NULL,
    evidence_excerpt text NOT NULL,
    artifact_refs_json text NOT NULL,
    status text NOT NULL,
    metadata_json text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: person_public_web_assets; Type: TABLE; Schema: -; Owner: -

CREATE TABLE person_public_web_assets (
    asset_id text NOT NULL,
    person_identity_key text NOT NULL,
    linkedin_url_key text NOT NULL,
    latest_run_id text NOT NULL,
    target_candidate_record_id text NOT NULL,
    candidate_name text NOT NULL,
    current_company text NOT NULL,
    status text NOT NULL,
    summary_json text NOT NULL,
    signals_json text NOT NULL,
    source_run_ids_json text NOT NULL,
    artifact_root text NOT NULL,
    metadata_json text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: person_public_web_signals; Type: TABLE; Schema: -; Owner: -

CREATE TABLE person_public_web_signals (
    signal_id text NOT NULL,
    run_id text NOT NULL,
    asset_id text NOT NULL,
    person_identity_key text NOT NULL,
    record_id text NOT NULL,
    candidate_id text NOT NULL,
    candidate_name text NOT NULL,
    current_company text NOT NULL,
    linkedin_url_key text NOT NULL,
    signal_kind text NOT NULL,
    signal_type text NOT NULL,
    email_type text NOT NULL,
    value text NOT NULL,
    normalized_value text NOT NULL,
    url text NOT NULL,
    source_url text NOT NULL,
    source_domain text NOT NULL,
    source_family text NOT NULL,
    source_title text NOT NULL,
    confidence_label text NOT NULL,
    confidence_score double precision NOT NULL,
    identity_match_label text NOT NULL,
    identity_match_score double precision NOT NULL,
    publishable bigint NOT NULL,
    promotion_status text NOT NULL,
    suppression_reason text NOT NULL,
    evidence_excerpt text NOT NULL,
    artifact_refs_json text NOT NULL,
    model_provider text NOT NULL,
    model_version text NOT NULL,
    metadata_json text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: plan_review_sessions_review_id_seq; Type: SEQUENCE; Schema: -; Owner: -

CREATE SEQUENCE plan_review_sessions_review_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Name: plan_review_sessions; Type: TABLE; Schema: -; Owner: -

CREATE TABLE plan_review_sessions (
    review_id bigint DEFAULT nextval('plan_review_sessions_review_id_seq'::regclass) NOT NULL,
    target_company text,
    request_signature text,
    request_family_signature text,
    matching_request_signature text,
    matching_request_family_signature text,
    status text NOT NULL,
    risk_level text NOT NULL,
    required_before_execution bigint NOT NULL,
    request_json text NOT NULL,
    plan_json text NOT NULL,
    gate_json text NOT NULL,
    execution_bundle_json text NOT NULL,
    matching_request_json text NOT NULL,
    decision_json text,
    reviewer text,
    review_notes text,
    approved_at text,
    created_at text,
    updated_at text
);

-- Name: projection_manifest_shards; Type: TABLE; Schema: -; Owner: -

CREATE TABLE projection_manifest_shards (
    shard_id text NOT NULL,
    projection_id text NOT NULL,
    shard_kind text NOT NULL,
    shard_index bigint NOT NULL,
    manifest_ref text NOT NULL,
    row_count bigint NOT NULL,
    content_signature text NOT NULL,
    metadata_json text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: projection_person_search_index; Type: TABLE; Schema: -; Owner: -

CREATE TABLE projection_person_search_index (
    projection_id text NOT NULL,
    candidate_identity_key text NOT NULL,
    person_identity_key text NOT NULL,
    indexed_text text NOT NULL,
    raw_profile_terms_json text NOT NULL,
    evidence_terms_json text NOT NULL,
    assertion_terms_json text NOT NULL,
    indexed_field_sources_json text NOT NULL,
    raw_profile_index_watermark text NOT NULL,
    evidence_index_watermark text NOT NULL,
    count_scope text NOT NULL,
    profile_fetched_at text NOT NULL,
    profile_indexed_at text NOT NULL,
    evidence_indexed_at text NOT NULL,
    metadata_json text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: query_dispatches_dispatch_id_seq; Type: SEQUENCE; Schema: -; Owner: -

CREATE SEQUENCE query_dispatches_dispatch_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Name: query_dispatches; Type: TABLE; Schema: -; Owner: -

CREATE TABLE query_dispatches (
    dispatch_id bigint DEFAULT nextval('query_dispatches_dispatch_id_seq'::regclass) NOT NULL,
    target_company text,
    request_signature text NOT NULL,
    request_family_signature text NOT NULL,
    matching_request_signature text,
    matching_request_family_signature text,
    requester_id text,
    tenant_id text,
    idempotency_key text,
    strategy text NOT NULL,
    status text NOT NULL,
    source_job_id text,
    created_job_id text,
    matching_request_json text NOT NULL,
    payload_json text,
    created_at text,
    updated_at text
);

-- Name: raw_profile_index; Type: TABLE; Schema: -; Owner: -

CREATE TABLE raw_profile_index (
    person_identity_key text NOT NULL,
    indexed_text text NOT NULL,
    raw_profile_terms_json text NOT NULL,
    source_asset_ids_json text NOT NULL,
    indexed_field_sources_json text NOT NULL,
    raw_profile_index_watermark text NOT NULL,
    profile_fetched_at text NOT NULL,
    profile_indexed_at text NOT NULL,
    metadata_json text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: run_projection_links; Type: TABLE; Schema: -; Owner: -

CREATE TABLE run_projection_links (
    run_id text NOT NULL,
    projection_id text NOT NULL,
    link_type text NOT NULL,
    projection_type text NOT NULL,
    collection_id text NOT NULL,
    state text NOT NULL,
    created_by text NOT NULL,
    metadata_json text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: runtime_outbox; Type: TABLE; Schema: -; Owner: -

CREATE TABLE runtime_outbox (
    outbox_id text NOT NULL,
    workflow_run_id text DEFAULT ''::text NOT NULL,
    operation_id text DEFAULT ''::text NOT NULL,
    command_id text DEFAULT ''::text NOT NULL,
    outbox_type text NOT NULL,
    status text DEFAULT 'queued'::text NOT NULL,
    idempotency_key text NOT NULL,
    payload_json text DEFAULT '{}'::text NOT NULL,
    not_before_at text DEFAULT ''::text NOT NULL,
    attempt bigint DEFAULT 0 NOT NULL,
    max_attempts bigint DEFAULT 5 NOT NULL,
    lease_owner text DEFAULT ''::text NOT NULL,
    lease_expires_at text DEFAULT ''::text NOT NULL,
    dispatched_at text DEFAULT ''::text NOT NULL,
    last_error text DEFAULT ''::text NOT NULL,
    schema_version text DEFAULT 'runtime_outbox_v1'::text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: runtime_provider_limiter_leases; Type: TABLE; Schema: -; Owner: -

CREATE TABLE runtime_provider_limiter_leases (
    lease_token text NOT NULL,
    limiter_key text NOT NULL,
    lease_owner text NOT NULL,
    lease_expires_at text NOT NULL,
    metadata_json text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: serving_projection_members; Type: TABLE; Schema: -; Owner: -

CREATE TABLE serving_projection_members (
    projection_id text NOT NULL,
    candidate_identity_key text NOT NULL,
    person_identity_key text NOT NULL,
    profile_url_key text NOT NULL,
    candidate_id text NOT NULL,
    rank_index bigint NOT NULL,
    rank_key text NOT NULL,
    lane text NOT NULL,
    employment_scope text NOT NULL,
    source_shard_key text NOT NULL,
    source_run_id text NOT NULL,
    row_readiness text NOT NULL,
    profile_readiness text NOT NULL,
    card_readiness text NOT NULL,
    visibility_state text NOT NULL,
    public_summary_json text NOT NULL,
    projection_metrics_json text NOT NULL,
    crm_overlay_summary_json text NOT NULL,
    provenance_json text NOT NULL,
    metadata_json text NOT NULL,
    published_at text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: serving_projections; Type: TABLE; Schema: -; Owner: -

CREATE TABLE serving_projections (
    projection_id text NOT NULL,
    projection_type text NOT NULL,
    collection_id text NOT NULL,
    source_run_id text NOT NULL,
    projection_version text NOT NULL,
    state text NOT NULL,
    scope_label text NOT NULL,
    scope_spec_json text NOT NULL,
    candidate_identity_manifest_ref text NOT NULL,
    source_collection_version text NOT NULL,
    raw_profile_index_watermark text NOT NULL,
    evidence_index_watermark text NOT NULL,
    counts_json text NOT NULL,
    readiness_json text NOT NULL,
    provenance_json text NOT NULL,
    manual_overlay_version text NOT NULL,
    metadata_json text NOT NULL,
    published_at text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: snapshot_materialization_runs; Type: TABLE; Schema: -; Owner: -

CREATE TABLE snapshot_materialization_runs (
    run_id text NOT NULL,
    target_company text NOT NULL,
    company_key text,
    snapshot_id text NOT NULL,
    asset_view text NOT NULL,
    status text NOT NULL,
    dirty_candidate_count bigint NOT NULL,
    completed_candidate_count bigint NOT NULL,
    reused_candidate_count bigint NOT NULL,
    summary_json text NOT NULL,
    started_at text,
    completed_at text,
    created_at text,
    updated_at text
);

-- Name: target_candidates; Type: TABLE; Schema: -; Owner: -

CREATE TABLE target_candidates (
    record_id text NOT NULL,
    candidate_id text NOT NULL,
    history_id text NOT NULL,
    job_id text NOT NULL,
    candidate_name text NOT NULL,
    headline text,
    current_company text,
    avatar_url text,
    linkedin_url text,
    primary_email text,
    person_identity_key text NOT NULL,
    candidate_identity_key text NOT NULL,
    source_projection_id text NOT NULL,
    source_run_id text NOT NULL,
    source_collection_id text NOT NULL,
    source_reason text NOT NULL,
    follow_up_status text NOT NULL,
    quality_score double precision,
    comment text NOT NULL,
    metadata_json text NOT NULL,
    added_at text,
    updated_at text
);

-- Name: workflow_activity_attempts; Type: TABLE; Schema: -; Owner: -

CREATE TABLE workflow_activity_attempts (
    attempt_id text NOT NULL,
    workspace_id text DEFAULT 'default'::text NOT NULL,
    activity_run_id text DEFAULT ''::text NOT NULL,
    workflow_run_id text DEFAULT ''::text NOT NULL,
    command_id text DEFAULT ''::text NOT NULL,
    attempt_number bigint DEFAULT 0 NOT NULL,
    status text DEFAULT 'planned'::text NOT NULL,
    provider text DEFAULT ''::text NOT NULL,
    provider_request_ref text DEFAULT ''::text NOT NULL,
    provider_run_ref text DEFAULT ''::text NOT NULL,
    started_at text DEFAULT ''::text NOT NULL,
    completed_at text DEFAULT ''::text NOT NULL,
    next_retry_at text DEFAULT ''::text NOT NULL,
    rate_limit_ref_json text DEFAULT '{}'::text NOT NULL,
    error_json text DEFAULT '{}'::text NOT NULL,
    input_json text DEFAULT '{}'::text NOT NULL,
    output_json text DEFAULT '{}'::text NOT NULL,
    artifact_refs_json text DEFAULT '[]'::text NOT NULL,
    idempotency_key text NOT NULL,
    metadata_json text DEFAULT '{}'::text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: workflow_activity_runs; Type: TABLE; Schema: -; Owner: -

CREATE TABLE workflow_activity_runs (
    activity_run_id text NOT NULL,
    workspace_id text DEFAULT 'default'::text NOT NULL,
    workflow_run_id text DEFAULT ''::text NOT NULL,
    operation_run_id text DEFAULT ''::text NOT NULL,
    acquisition_run_id text DEFAULT ''::text NOT NULL,
    command_id text DEFAULT ''::text NOT NULL,
    parent_activity_run_id text DEFAULT ''::text NOT NULL,
    activity_type text NOT NULL,
    owner text DEFAULT ''::text NOT NULL,
    status text DEFAULT 'planned'::text NOT NULL,
    phase text DEFAULT ''::text NOT NULL,
    idempotency_key text NOT NULL,
    provider_ref_json text DEFAULT '{}'::text NOT NULL,
    input_json text DEFAULT '{}'::text NOT NULL,
    output_json text DEFAULT '{}'::text NOT NULL,
    artifact_refs_json text DEFAULT '[]'::text NOT NULL,
    entity_counts_json text DEFAULT '{}'::text NOT NULL,
    metadata_json text DEFAULT '{}'::text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: workflow_commands; Type: TABLE; Schema: -; Owner: -

CREATE TABLE workflow_commands (
    command_id text NOT NULL,
    workflow_run_id text NOT NULL,
    operation_id text DEFAULT ''::text NOT NULL,
    command_type text NOT NULL,
    owner text NOT NULL,
    stage_id text DEFAULT ''::text NOT NULL,
    causal_group_id text DEFAULT ''::text NOT NULL,
    parent_command_id text DEFAULT ''::text NOT NULL,
    source_event_id text DEFAULT ''::text NOT NULL,
    source_event_type text DEFAULT ''::text NOT NULL,
    input_artifact_refs_json text DEFAULT '[]'::text NOT NULL,
    output_artifact_refs_json text DEFAULT '[]'::text NOT NULL,
    produced_entity_counts_json text DEFAULT '{}'::text NOT NULL,
    no_op_reason text DEFAULT ''::text NOT NULL,
    readiness_effect text DEFAULT ''::text NOT NULL,
    downstream_command_ids_json text DEFAULT '[]'::text NOT NULL,
    causality_schema_version text DEFAULT 'command_causality_v1'::text NOT NULL,
    status text DEFAULT 'queued'::text NOT NULL,
    idempotency_key text NOT NULL,
    payload_json text DEFAULT '{}'::text NOT NULL,
    artifact_refs_json text DEFAULT '[]'::text NOT NULL,
    not_before_at text DEFAULT ''::text NOT NULL,
    attempt bigint DEFAULT 0 NOT NULL,
    max_attempts bigint DEFAULT 5 NOT NULL,
    retry_policy_json text DEFAULT '{}'::text NOT NULL,
    lease_owner text DEFAULT ''::text NOT NULL,
    lease_expires_at text DEFAULT ''::text NOT NULL,
    heartbeat_at text DEFAULT ''::text NOT NULL,
    last_error text DEFAULT ''::text NOT NULL,
    result_json text DEFAULT '{}'::text NOT NULL,
    schema_version text DEFAULT 'workflow_command_v1'::text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: workflow_current_state; Type: TABLE; Schema: -; Owner: -

CREATE TABLE workflow_current_state (
    workflow_run_id text NOT NULL,
    operation_id text DEFAULT ''::text NOT NULL,
    workflow_type text DEFAULT ''::text NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    current_stage_key text DEFAULT ''::text NOT NULL,
    completion_proofs_json text DEFAULT '{}'::text NOT NULL,
    active_command_counts_json text DEFAULT '{}'::text NOT NULL,
    terminal_command_counts_json text DEFAULT '{}'::text NOT NULL,
    read_model_pointers_json text DEFAULT '{}'::text NOT NULL,
    migration_status_json text DEFAULT '{}'::text NOT NULL,
    last_processed_sequence_number bigint DEFAULT 0 NOT NULL,
    reducer_version text DEFAULT ''::text NOT NULL,
    schema_version text DEFAULT 'workflow_current_state_v1'::text NOT NULL,
    metadata_json text DEFAULT '{}'::text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: workflow_entity_deltas; Type: TABLE; Schema: -; Owner: -

CREATE TABLE workflow_entity_deltas (
    delta_id text NOT NULL,
    workspace_id text DEFAULT 'default'::text NOT NULL,
    workflow_run_id text DEFAULT ''::text NOT NULL,
    operation_run_id text DEFAULT ''::text NOT NULL,
    command_id text DEFAULT ''::text NOT NULL,
    activity_run_id text DEFAULT ''::text NOT NULL,
    attempt_id text DEFAULT ''::text NOT NULL,
    acquisition_run_id text DEFAULT ''::text NOT NULL,
    entity_type text DEFAULT ''::text NOT NULL,
    entity_key text DEFAULT ''::text NOT NULL,
    delta_kind text DEFAULT ''::text NOT NULL,
    status text DEFAULT 'recorded'::text NOT NULL,
    reason text DEFAULT ''::text NOT NULL,
    source_ref_json text DEFAULT '{}'::text NOT NULL,
    entity_payload_json text DEFAULT '{}'::text NOT NULL,
    projection_effect_json text DEFAULT '{}'::text NOT NULL,
    artifact_refs_json text DEFAULT '[]'::text NOT NULL,
    idempotency_key text NOT NULL,
    metadata_json text DEFAULT '{}'::text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: workflow_events; Type: TABLE; Schema: -; Owner: -

CREATE TABLE workflow_events (
    event_id text NOT NULL,
    workflow_run_id text NOT NULL,
    operation_id text DEFAULT ''::text NOT NULL,
    command_id text DEFAULT ''::text NOT NULL,
    activity_attempt_id text DEFAULT ''::text NOT NULL,
    event_family text NOT NULL,
    event_type text NOT NULL,
    sequence_number bigint DEFAULT 0 NOT NULL,
    idempotency_key text NOT NULL,
    occurred_at text DEFAULT ''::text NOT NULL,
    recorded_at text DEFAULT ''::text NOT NULL,
    actor text DEFAULT ''::text NOT NULL,
    source text DEFAULT ''::text NOT NULL,
    payload_json text DEFAULT '{}'::text NOT NULL,
    artifact_refs_json text DEFAULT '[]'::text NOT NULL,
    schema_version text DEFAULT 'workflow_event_v1'::text NOT NULL,
    created_at text
);

-- Name: workflow_job_leases; Type: TABLE; Schema: -; Owner: -

CREATE TABLE workflow_job_leases (
    job_id text NOT NULL,
    lease_owner text NOT NULL,
    lease_token text NOT NULL,
    lease_expires_at text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: workflow_recovery_intents; Type: TABLE; Schema: -; Owner: -

CREATE TABLE workflow_recovery_intents (
    job_id text NOT NULL,
    classification text NOT NULL,
    status text NOT NULL,
    requested_at text NOT NULL,
    requested_by text NOT NULL,
    params_json text NOT NULL,
    lease_owner text NOT NULL,
    lease_expires_at text NOT NULL,
    claimed_at text NOT NULL,
    schema_version text NOT NULL,
    created_at text,
    updated_at text
);

-- Name: acquisition_discovery_lanes acquisition_discovery_lanes_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY acquisition_discovery_lanes
    ADD CONSTRAINT acquisition_discovery_lanes_pkey PRIMARY KEY (lane_id);

-- Name: acquisition_discovery_lanes acquisition_discovery_lanes_workspace_id_idempotency_key_key; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY acquisition_discovery_lanes
    ADD CONSTRAINT acquisition_discovery_lanes_workspace_id_idempotency_key_key UNIQUE (workspace_id, idempotency_key);

-- Name: acquisition_runs acquisition_runs_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY acquisition_runs
    ADD CONSTRAINT acquisition_runs_pkey PRIMARY KEY (acquisition_run_id);

-- Name: acquisition_runs acquisition_runs_workspace_id_idempotency_key_key; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY acquisition_runs
    ADD CONSTRAINT acquisition_runs_workspace_id_idempotency_key_key UNIQUE (workspace_id, idempotency_key);

-- Name: acquisition_shard_registry_current acquisition_shard_registry_current_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY acquisition_shard_registry_current
    ADD CONSTRAINT acquisition_shard_registry_current_pkey PRIMARY KEY (shard_key);

-- Name: acquisition_shard_registry_former acquisition_shard_registry_former_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY acquisition_shard_registry_former
    ADD CONSTRAINT acquisition_shard_registry_former_pkey PRIMARY KEY (shard_key);

-- Name: agent_actions agent_actions_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY agent_actions
    ADD CONSTRAINT agent_actions_pkey PRIMARY KEY (action_id);

-- Name: agent_actions agent_actions_workspace_id_idempotency_key_key; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY agent_actions
    ADD CONSTRAINT agent_actions_workspace_id_idempotency_key_key UNIQUE (workspace_id, idempotency_key);

-- Name: agent_runtime_sessions agent_runtime_sessions_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY agent_runtime_sessions
    ADD CONSTRAINT agent_runtime_sessions_pkey PRIMARY KEY (session_id);

-- Name: agent_trace_spans agent_trace_spans_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY agent_trace_spans
    ADD CONSTRAINT agent_trace_spans_pkey PRIMARY KEY (span_id);

-- Name: agent_worker_runs agent_worker_runs_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY agent_worker_runs
    ADD CONSTRAINT agent_worker_runs_pkey PRIMARY KEY (worker_id);

-- Name: asset_default_pointer_history asset_default_pointer_history_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY asset_default_pointer_history
    ADD CONSTRAINT asset_default_pointer_history_pkey PRIMARY KEY (history_id);

-- Name: asset_default_pointers asset_default_pointers_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY asset_default_pointers
    ADD CONSTRAINT asset_default_pointers_pkey PRIMARY KEY (pointer_key);

-- Name: asset_materialization_generations asset_materialization_generations_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY asset_materialization_generations
    ADD CONSTRAINT asset_materialization_generations_pkey PRIMARY KEY (target_company, snapshot_id, asset_view, artifact_kind, artifact_key);

-- Name: asset_membership_index asset_membership_index_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY asset_membership_index
    ADD CONSTRAINT asset_membership_index_pkey PRIMARY KEY (generation_key, member_key);

-- Name: candidate_evidence_index candidate_evidence_index_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY candidate_evidence_index
    ADD CONSTRAINT candidate_evidence_index_pkey PRIMARY KEY (person_identity_key);

-- Name: candidate_materialization_state candidate_materialization_state_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY candidate_materialization_state
    ADD CONSTRAINT candidate_materialization_state_pkey PRIMARY KEY (target_company, snapshot_id, asset_view, candidate_id);

-- Name: candidate_review_registry candidate_review_registry_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY candidate_review_registry
    ADD CONSTRAINT candidate_review_registry_pkey PRIMARY KEY (record_id);

-- Name: candidates candidates_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY candidates
    ADD CONSTRAINT candidates_pkey PRIMARY KEY (candidate_id);

-- Name: cloud_asset_operation_ledger cloud_asset_operation_ledger_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY cloud_asset_operation_ledger
    ADD CONSTRAINT cloud_asset_operation_ledger_pkey PRIMARY KEY (ledger_id);

-- Name: collection_authoritative_pointers collection_authoritative_pointers_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY collection_authoritative_pointers
    ADD CONSTRAINT collection_authoritative_pointers_pkey PRIMARY KEY (collection_id);

-- Name: company_assertions company_assertions_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY company_assertions
    ADD CONSTRAINT company_assertions_pkey PRIMARY KEY (assertion_id);

-- Name: company_assets company_assets_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY company_assets
    ADD CONSTRAINT company_assets_pkey PRIMARY KEY (asset_id);

-- Name: company_evidence company_evidence_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY company_evidence
    ADD CONSTRAINT company_evidence_pkey PRIMARY KEY (evidence_id);

-- Name: company_public_web_asset_runs company_public_web_asset_runs_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY company_public_web_asset_runs
    ADD CONSTRAINT company_public_web_asset_runs_pkey PRIMARY KEY (run_id);

-- Name: company_public_web_assets company_public_web_assets_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY company_public_web_assets
    ADD CONSTRAINT company_public_web_assets_pkey PRIMARY KEY (asset_id);

-- Name: confidence_policy_controls confidence_policy_controls_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY confidence_policy_controls
    ADD CONSTRAINT confidence_policy_controls_pkey PRIMARY KEY (control_id);

-- Name: confidence_policy_runs confidence_policy_runs_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY confidence_policy_runs
    ADD CONSTRAINT confidence_policy_runs_pkey PRIMARY KEY (policy_run_id);

-- Name: criteria_compiler_runs criteria_compiler_runs_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY criteria_compiler_runs
    ADD CONSTRAINT criteria_compiler_runs_pkey PRIMARY KEY (compiler_run_id);

-- Name: criteria_feedback criteria_feedback_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY criteria_feedback
    ADD CONSTRAINT criteria_feedback_pkey PRIMARY KEY (feedback_id);

-- Name: criteria_pattern_suggestions criteria_pattern_suggestions_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY criteria_pattern_suggestions
    ADD CONSTRAINT criteria_pattern_suggestions_pkey PRIMARY KEY (suggestion_id);

-- Name: criteria_patterns criteria_patterns_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY criteria_patterns
    ADD CONSTRAINT criteria_patterns_pkey PRIMARY KEY (pattern_id);

-- Name: criteria_result_diffs criteria_result_diffs_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY criteria_result_diffs
    ADD CONSTRAINT criteria_result_diffs_pkey PRIMARY KEY (diff_id);

-- Name: criteria_versions criteria_versions_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY criteria_versions
    ADD CONSTRAINT criteria_versions_pkey PRIMARY KEY (version_id);

-- Name: crm_engagements crm_engagements_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY crm_engagements
    ADD CONSTRAINT crm_engagements_pkey PRIMARY KEY (engagement_id);

-- Name: crm_events crm_events_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY crm_events
    ADD CONSTRAINT crm_events_pkey PRIMARY KEY (event_id);

-- Name: crm_public_web_batches crm_public_web_batches_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY crm_public_web_batches
    ADD CONSTRAINT crm_public_web_batches_pkey PRIMARY KEY (batch_id);

-- Name: crm_public_web_promotions crm_public_web_promotions_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY crm_public_web_promotions
    ADD CONSTRAINT crm_public_web_promotions_pkey PRIMARY KEY (promotion_id);

-- Name: crm_public_web_runs crm_public_web_runs_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY crm_public_web_runs
    ADD CONSTRAINT crm_public_web_runs_pkey PRIMARY KEY (run_id);

-- Name: crm_records crm_records_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY crm_records
    ADD CONSTRAINT crm_records_pkey PRIMARY KEY (crm_record_id);

-- Name: crm_tasks crm_tasks_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY crm_tasks
    ADD CONSTRAINT crm_tasks_pkey PRIMARY KEY (task_id);

-- Name: evidence evidence_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY evidence
    ADD CONSTRAINT evidence_pkey PRIMARY KEY (evidence_id);

-- Name: frontend_history_links frontend_history_links_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY frontend_history_links
    ADD CONSTRAINT frontend_history_links_pkey PRIMARY KEY (history_id);

-- Name: job_board_visible_patches job_board_visible_patches_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY job_board_visible_patches
    ADD CONSTRAINT job_board_visible_patches_pkey PRIMARY KEY (patch_id);

-- Name: job_events job_events_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY job_events
    ADD CONSTRAINT job_events_pkey PRIMARY KEY (event_id);

-- Name: job_materialization_items job_materialization_items_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY job_materialization_items
    ADD CONSTRAINT job_materialization_items_pkey PRIMARY KEY (item_id);

-- Name: job_progress_event_summaries job_progress_event_summaries_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY job_progress_event_summaries
    ADD CONSTRAINT job_progress_event_summaries_pkey PRIMARY KEY (job_id);

-- Name: job_result_lifecycle job_result_lifecycle_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY job_result_lifecycle
    ADD CONSTRAINT job_result_lifecycle_pkey PRIMARY KEY (job_id);

-- Name: job_result_views job_result_views_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY job_result_views
    ADD CONSTRAINT job_result_views_pkey PRIMARY KEY (view_id);

-- Name: job_results job_results_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY job_results
    ADD CONSTRAINT job_results_pkey PRIMARY KEY (job_id, candidate_id);

-- Name: jobs jobs_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY jobs
    ADD CONSTRAINT jobs_pkey PRIMARY KEY (job_id);

-- Name: linkedin_profile_registry_aliases linkedin_profile_registry_aliases_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY linkedin_profile_registry_aliases
    ADD CONSTRAINT linkedin_profile_registry_aliases_pkey PRIMARY KEY (alias_url_key);

-- Name: linkedin_profile_registry_backfill_runs linkedin_profile_registry_backfill_runs_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY linkedin_profile_registry_backfill_runs
    ADD CONSTRAINT linkedin_profile_registry_backfill_runs_pkey PRIMARY KEY (run_key);

-- Name: linkedin_profile_registry_events linkedin_profile_registry_events_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY linkedin_profile_registry_events
    ADD CONSTRAINT linkedin_profile_registry_events_pkey PRIMARY KEY (event_id);

-- Name: linkedin_profile_registry_leases linkedin_profile_registry_leases_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY linkedin_profile_registry_leases
    ADD CONSTRAINT linkedin_profile_registry_leases_pkey PRIMARY KEY (profile_url_key);

-- Name: linkedin_profile_registry linkedin_profile_registry_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY linkedin_profile_registry
    ADD CONSTRAINT linkedin_profile_registry_pkey PRIMARY KEY (profile_url_key);

-- Name: manual_review_items manual_review_items_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY manual_review_items
    ADD CONSTRAINT manual_review_items_pkey PRIMARY KEY (review_item_id);

-- Name: operation_events operation_events_event_stream_id_idempotency_key_key; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY operation_events
    ADD CONSTRAINT operation_events_event_stream_id_idempotency_key_key UNIQUE (event_stream_id, idempotency_key);

-- Name: operation_events operation_events_event_stream_id_sequence_number_key; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY operation_events
    ADD CONSTRAINT operation_events_event_stream_id_sequence_number_key UNIQUE (event_stream_id, sequence_number);

-- Name: operation_events operation_events_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY operation_events
    ADD CONSTRAINT operation_events_pkey PRIMARY KEY (event_id);

-- Name: operation_runs operation_runs_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY operation_runs
    ADD CONSTRAINT operation_runs_pkey PRIMARY KEY (operation_run_id);

-- Name: operation_runs operation_runs_workspace_id_idempotency_key_key; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY operation_runs
    ADD CONSTRAINT operation_runs_workspace_id_idempotency_key_key UNIQUE (workspace_id, idempotency_key);

-- Name: organization_asset_registry organization_asset_registry_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY organization_asset_registry
    ADD CONSTRAINT organization_asset_registry_pkey PRIMARY KEY (registry_id);

-- Name: organization_execution_profiles organization_execution_profiles_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY organization_execution_profiles
    ADD CONSTRAINT organization_execution_profiles_pkey PRIMARY KEY (profile_id);

-- Name: person_assertions person_assertions_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY person_assertions
    ADD CONSTRAINT person_assertions_pkey PRIMARY KEY (assertion_id);

-- Name: person_assets person_assets_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY person_assets
    ADD CONSTRAINT person_assets_pkey PRIMARY KEY (asset_id);

-- Name: person_evidence person_evidence_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY person_evidence
    ADD CONSTRAINT person_evidence_pkey PRIMARY KEY (evidence_id);

-- Name: person_public_web_assets person_public_web_assets_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY person_public_web_assets
    ADD CONSTRAINT person_public_web_assets_pkey PRIMARY KEY (asset_id);

-- Name: person_public_web_signals person_public_web_signals_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY person_public_web_signals
    ADD CONSTRAINT person_public_web_signals_pkey PRIMARY KEY (signal_id);

-- Name: plan_review_sessions plan_review_sessions_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY plan_review_sessions
    ADD CONSTRAINT plan_review_sessions_pkey PRIMARY KEY (review_id);

-- Name: projection_manifest_shards projection_manifest_shards_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY projection_manifest_shards
    ADD CONSTRAINT projection_manifest_shards_pkey PRIMARY KEY (shard_id);

-- Name: projection_person_search_index projection_person_search_index_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY projection_person_search_index
    ADD CONSTRAINT projection_person_search_index_pkey PRIMARY KEY (projection_id, candidate_identity_key);

-- Name: query_dispatches query_dispatches_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY query_dispatches
    ADD CONSTRAINT query_dispatches_pkey PRIMARY KEY (dispatch_id);

-- Name: raw_profile_index raw_profile_index_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY raw_profile_index
    ADD CONSTRAINT raw_profile_index_pkey PRIMARY KEY (person_identity_key);

-- Name: run_projection_links run_projection_links_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY run_projection_links
    ADD CONSTRAINT run_projection_links_pkey PRIMARY KEY (run_id, link_type);

-- Name: runtime_outbox runtime_outbox_idempotency_key_key; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY runtime_outbox
    ADD CONSTRAINT runtime_outbox_idempotency_key_key UNIQUE (idempotency_key);

-- Name: runtime_outbox runtime_outbox_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY runtime_outbox
    ADD CONSTRAINT runtime_outbox_pkey PRIMARY KEY (outbox_id);

-- Name: runtime_provider_limiter_leases runtime_provider_limiter_leases_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY runtime_provider_limiter_leases
    ADD CONSTRAINT runtime_provider_limiter_leases_pkey PRIMARY KEY (lease_token);

-- Name: serving_projection_members serving_projection_members_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY serving_projection_members
    ADD CONSTRAINT serving_projection_members_pkey PRIMARY KEY (projection_id, candidate_identity_key);

-- Name: serving_projections serving_projections_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY serving_projections
    ADD CONSTRAINT serving_projections_pkey PRIMARY KEY (projection_id);

-- Name: snapshot_materialization_runs snapshot_materialization_runs_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY snapshot_materialization_runs
    ADD CONSTRAINT snapshot_materialization_runs_pkey PRIMARY KEY (run_id);

-- Name: target_candidates target_candidates_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY target_candidates
    ADD CONSTRAINT target_candidates_pkey PRIMARY KEY (record_id);

-- Name: workflow_activity_attempts workflow_activity_attempts_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY workflow_activity_attempts
    ADD CONSTRAINT workflow_activity_attempts_pkey PRIMARY KEY (attempt_id);

-- Name: workflow_activity_attempts workflow_activity_attempts_workspace_id_idempotency_key_key; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY workflow_activity_attempts
    ADD CONSTRAINT workflow_activity_attempts_workspace_id_idempotency_key_key UNIQUE (workspace_id, idempotency_key);

-- Name: workflow_activity_runs workflow_activity_runs_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY workflow_activity_runs
    ADD CONSTRAINT workflow_activity_runs_pkey PRIMARY KEY (activity_run_id);

-- Name: workflow_activity_runs workflow_activity_runs_workspace_id_idempotency_key_key; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY workflow_activity_runs
    ADD CONSTRAINT workflow_activity_runs_workspace_id_idempotency_key_key UNIQUE (workspace_id, idempotency_key);

-- Name: workflow_commands workflow_commands_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY workflow_commands
    ADD CONSTRAINT workflow_commands_pkey PRIMARY KEY (command_id);

-- Name: workflow_commands workflow_commands_workflow_run_id_idempotency_key_key; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY workflow_commands
    ADD CONSTRAINT workflow_commands_workflow_run_id_idempotency_key_key UNIQUE (workflow_run_id, idempotency_key);

-- Name: workflow_current_state workflow_current_state_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY workflow_current_state
    ADD CONSTRAINT workflow_current_state_pkey PRIMARY KEY (workflow_run_id);

-- Name: workflow_entity_deltas workflow_entity_deltas_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY workflow_entity_deltas
    ADD CONSTRAINT workflow_entity_deltas_pkey PRIMARY KEY (delta_id);

-- Name: workflow_entity_deltas workflow_entity_deltas_workspace_id_idempotency_key_key; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY workflow_entity_deltas
    ADD CONSTRAINT workflow_entity_deltas_workspace_id_idempotency_key_key UNIQUE (workspace_id, idempotency_key);

-- Name: workflow_events workflow_events_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY workflow_events
    ADD CONSTRAINT workflow_events_pkey PRIMARY KEY (event_id);

-- Name: workflow_events workflow_events_workflow_run_id_idempotency_key_key; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY workflow_events
    ADD CONSTRAINT workflow_events_workflow_run_id_idempotency_key_key UNIQUE (workflow_run_id, idempotency_key);

-- Name: workflow_events workflow_events_workflow_run_id_sequence_number_key; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY workflow_events
    ADD CONSTRAINT workflow_events_workflow_run_id_sequence_number_key UNIQUE (workflow_run_id, sequence_number);

-- Name: workflow_job_leases workflow_job_leases_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY workflow_job_leases
    ADD CONSTRAINT workflow_job_leases_pkey PRIMARY KEY (job_id);

-- Name: workflow_recovery_intents workflow_recovery_intents_pkey; Type: CONSTRAINT; Schema: -; Owner: -

ALTER TABLE ONLY workflow_recovery_intents
    ADD CONSTRAINT workflow_recovery_intents_pkey PRIMARY KEY (job_id);

-- Name: idx_acquisition_discovery_lanes_acquisition; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_acquisition_discovery_lanes_acquisition ON acquisition_discovery_lanes USING btree (acquisition_run_id, status, updated_at);

-- Name: idx_acquisition_discovery_lanes_source_command; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_acquisition_discovery_lanes_source_command ON acquisition_discovery_lanes USING btree (source_command_id);

-- Name: idx_acquisition_discovery_lanes_workflow; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_acquisition_discovery_lanes_workflow ON acquisition_discovery_lanes USING btree (workflow_run_id, status, updated_at);

-- Name: idx_acquisition_discovery_lanes_workspace_idempotency_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_acquisition_discovery_lanes_workspace_idempotency_unique ON acquisition_discovery_lanes USING btree (workspace_id, idempotency_key);

-- Name: idx_acquisition_runs_company; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_acquisition_runs_company ON acquisition_runs USING btree (workspace_id, target_company, status, updated_at);

-- Name: idx_acquisition_runs_operation; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_acquisition_runs_operation ON acquisition_runs USING btree (operation_run_id, status, updated_at);

-- Name: idx_acquisition_runs_workflow; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_acquisition_runs_workflow ON acquisition_runs USING btree (workflow_run_id, status, updated_at);

-- Name: idx_acquisition_runs_workspace_idempotency_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_acquisition_runs_workspace_idempotency_unique ON acquisition_runs USING btree (workspace_id, idempotency_key);

-- Name: idx_acquisition_shard_registry_current_company; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_acquisition_shard_registry_current_company ON acquisition_shard_registry_current USING btree (target_company, snapshot_id, lane, employment_scope, updated_at);

-- Name: idx_acquisition_shard_registry_current_shard_key; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_acquisition_shard_registry_current_shard_key ON acquisition_shard_registry_current USING btree (shard_key);

-- Name: idx_acquisition_shard_registry_former_company; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_acquisition_shard_registry_former_company ON acquisition_shard_registry_former USING btree (target_company, snapshot_id, lane, employment_scope, updated_at);

-- Name: idx_acquisition_shard_registry_former_shard_key; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_acquisition_shard_registry_former_shard_key ON acquisition_shard_registry_former USING btree (shard_key);

-- Name: idx_agent_actions_owner; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_agent_actions_owner ON agent_actions USING btree (owner_module, action_type, status, updated_at);

-- Name: idx_agent_actions_workspace_idempotency_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_agent_actions_workspace_idempotency_unique ON agent_actions USING btree (workspace_id, idempotency_key);

-- Name: idx_agent_actions_workspace_status; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_agent_actions_workspace_status ON agent_actions USING btree (workspace_id, status, updated_at);

-- Name: idx_agent_runtime_sessions_job_id; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_agent_runtime_sessions_job_id ON agent_runtime_sessions USING btree (job_id);

-- Name: idx_agent_runtime_sessions_session_id_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_agent_runtime_sessions_session_id_unique ON agent_runtime_sessions USING btree (session_id);

-- Name: idx_agent_trace_spans_job_span; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_agent_trace_spans_job_span ON agent_trace_spans USING btree (job_id, session_id, span_id);

-- Name: idx_agent_worker_runs_job_lane_worker_key; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_agent_worker_runs_job_lane_worker_key ON agent_worker_runs USING btree (job_id, lane_id, worker_key);

-- Name: idx_agent_worker_runs_job_updated; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_agent_worker_runs_job_updated ON agent_worker_runs USING btree (job_id, updated_at, worker_id);

-- Name: idx_asset_default_pointer_history_pointer; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_asset_default_pointer_history_pointer ON asset_default_pointer_history USING btree (pointer_key, occurred_at);

-- Name: idx_asset_default_pointers_company; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_asset_default_pointers_company ON asset_default_pointers USING btree (company_key, scope_kind, scope_key, asset_kind);

-- Name: idx_candidate_evidence_index_watermark; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_candidate_evidence_index_watermark ON candidate_evidence_index USING btree (evidence_index_watermark, updated_at);

-- Name: idx_cloud_asset_operation_ledger_ledger_id_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_cloud_asset_operation_ledger_ledger_id_unique ON cloud_asset_operation_ledger USING btree (ledger_id);

-- Name: idx_company_assertions_company; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_company_assertions_company ON company_assertions USING btree (workspace_id, company_key, assertion_type, verification_status, updated_at);

-- Name: idx_company_assertions_evidence; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_company_assertions_evidence ON company_assertions USING btree (source_evidence_id, updated_at);

-- Name: idx_company_assets_company; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_company_assets_company ON company_assets USING btree (workspace_id, company_key, asset_type, status, updated_at);

-- Name: idx_company_assets_source; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_company_assets_source ON company_assets USING btree (source_run_id, source_command_id, activity_run_id);

-- Name: idx_company_evidence_asset; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_company_evidence_asset ON company_evidence USING btree (asset_id, updated_at);

-- Name: idx_company_evidence_company; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_company_evidence_company ON company_evidence USING btree (workspace_id, company_key, evidence_type, status, updated_at);

-- Name: idx_company_public_web_asset_runs_company; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_company_public_web_asset_runs_company ON company_public_web_asset_runs USING btree (company_key, updated_at);

-- Name: idx_company_public_web_asset_runs_status; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_company_public_web_asset_runs_status ON company_public_web_asset_runs USING btree (status, updated_at);

-- Name: idx_company_public_web_assets_company; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_company_public_web_assets_company ON company_public_web_assets USING btree (company_key, source_family, updated_at);

-- Name: idx_company_public_web_assets_url; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_company_public_web_assets_url ON company_public_web_assets USING btree (normalized_url_key, updated_at);

-- Name: idx_confidence_policy_controls_control_id_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_confidence_policy_controls_control_id_unique ON confidence_policy_controls USING btree (control_id);

-- Name: idx_confidence_policy_runs_policy_run_id_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_confidence_policy_runs_policy_run_id_unique ON confidence_policy_runs USING btree (policy_run_id);

-- Name: idx_criteria_compiler_runs_compiler_run_id_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_criteria_compiler_runs_compiler_run_id_unique ON criteria_compiler_runs USING btree (compiler_run_id);

-- Name: idx_criteria_feedback_feedback_id_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_criteria_feedback_feedback_id_unique ON criteria_feedback USING btree (feedback_id);

-- Name: idx_criteria_pattern_suggestions_suggestion_id_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_criteria_pattern_suggestions_suggestion_id_unique ON criteria_pattern_suggestions USING btree (suggestion_id);

-- Name: idx_criteria_patterns_identity_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_criteria_patterns_identity_unique ON criteria_patterns USING btree (target_company, pattern_type, subject, value);

-- Name: idx_criteria_patterns_pattern_id_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_criteria_patterns_pattern_id_unique ON criteria_patterns USING btree (pattern_id);

-- Name: idx_criteria_result_diffs_diff_id_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_criteria_result_diffs_diff_id_unique ON criteria_result_diffs USING btree (diff_id);

-- Name: idx_criteria_versions_version_id_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_criteria_versions_version_id_unique ON criteria_versions USING btree (version_id);

-- Name: idx_crm_engagements_record; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_crm_engagements_record ON crm_engagements USING btree (crm_record_id, updated_at);

-- Name: idx_crm_events_idempotency; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_crm_events_idempotency ON crm_events USING btree (workspace_id, idempotency_key) WHERE (idempotency_key <> ''::text);

-- Name: idx_crm_events_record; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_crm_events_record ON crm_events USING btree (crm_record_id, created_at);

-- Name: idx_crm_public_web_batches_idempotency_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_crm_public_web_batches_idempotency_unique ON crm_public_web_batches USING btree (idempotency_key) WHERE (idempotency_key <> ''::text);

-- Name: idx_crm_public_web_batches_updated; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_crm_public_web_batches_updated ON crm_public_web_batches USING btree (workspace_id, updated_at, status);

-- Name: idx_crm_public_web_promotions_record; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_crm_public_web_promotions_record ON crm_public_web_promotions USING btree (workspace_id, crm_record_id, updated_at);

-- Name: idx_crm_public_web_promotions_run; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_crm_public_web_promotions_run ON crm_public_web_promotions USING btree (run_id, updated_at);

-- Name: idx_crm_public_web_promotions_signal; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_crm_public_web_promotions_signal ON crm_public_web_promotions USING btree (signal_id, updated_at);

-- Name: idx_crm_public_web_runs_batch; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_crm_public_web_runs_batch ON crm_public_web_runs USING btree (batch_id, updated_at);

-- Name: idx_crm_public_web_runs_idempotency_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_crm_public_web_runs_idempotency_unique ON crm_public_web_runs USING btree (idempotency_key) WHERE (idempotency_key <> ''::text);

-- Name: idx_crm_public_web_runs_identity; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_crm_public_web_runs_identity ON crm_public_web_runs USING btree (person_identity_key, updated_at);

-- Name: idx_crm_public_web_runs_record; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_crm_public_web_runs_record ON crm_public_web_runs USING btree (workspace_id, crm_record_id, updated_at);

-- Name: idx_crm_public_web_runs_status; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_crm_public_web_runs_status ON crm_public_web_runs USING btree (workspace_id, status, updated_at);

-- Name: idx_crm_records_collection; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_crm_records_collection ON crm_records USING btree (source_collection_id, updated_at);

-- Name: idx_crm_records_identity; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_crm_records_identity ON crm_records USING btree (workspace_id, person_identity_key);

-- Name: idx_crm_records_projection; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_crm_records_projection ON crm_records USING btree (source_projection_id, updated_at);

-- Name: idx_crm_tasks_idempotency; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_crm_tasks_idempotency ON crm_tasks USING btree (workspace_id, idempotency_key) WHERE (idempotency_key <> ''::text);

-- Name: idx_crm_tasks_record_status_due; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_crm_tasks_record_status_due ON crm_tasks USING btree (workspace_id, crm_record_id, status, due_at);

-- Name: idx_crm_tasks_status_due; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_crm_tasks_status_due ON crm_tasks USING btree (workspace_id, status, due_at);

-- Name: idx_frontend_history_links_job; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_frontend_history_links_job ON frontend_history_links USING btree (job_id, updated_at);

-- Name: idx_frontend_history_links_review; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_frontend_history_links_review ON frontend_history_links USING btree (review_id, updated_at);

-- Name: idx_job_events_event_id_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_job_events_event_id_unique ON job_events USING btree (event_id);

-- Name: idx_job_events_job_id_event_id; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_job_events_job_id_event_id ON job_events USING btree (job_id, event_id);

-- Name: idx_job_progress_event_summaries_updated; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_job_progress_event_summaries_updated ON job_progress_event_summaries USING btree (updated_at);

-- Name: idx_job_result_views_job_id_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_job_result_views_job_id_unique ON job_result_views USING btree (job_id);

-- Name: idx_job_results_job_id_rank_index; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_job_results_job_id_rank_index ON job_results USING btree (job_id, rank_index);

-- Name: idx_linkedin_profile_registry_alias_canonical; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_linkedin_profile_registry_alias_canonical ON linkedin_profile_registry_aliases USING btree (profile_url_key, updated_at);

-- Name: idx_linkedin_profile_registry_event_profile; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_linkedin_profile_registry_event_profile ON linkedin_profile_registry_events USING btree (profile_url_key, created_at);

-- Name: idx_linkedin_profile_registry_event_type; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_linkedin_profile_registry_event_type ON linkedin_profile_registry_events USING btree (event_type, created_at);

-- Name: idx_linkedin_profile_registry_events_event_id_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_linkedin_profile_registry_events_event_id_unique ON linkedin_profile_registry_events USING btree (event_id);

-- Name: idx_linkedin_profile_registry_leases_expires; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_linkedin_profile_registry_leases_expires ON linkedin_profile_registry_leases USING btree (lease_expires_at);

-- Name: idx_linkedin_profile_registry_refill_queue; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_linkedin_profile_registry_refill_queue ON linkedin_profile_registry USING btree (refill_queue_state, refill_not_before_at, last_refill_planned_at, updated_at);

-- Name: idx_linkedin_profile_registry_run; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_linkedin_profile_registry_run ON linkedin_profile_registry USING btree (last_run_id, last_dataset_id, updated_at);

-- Name: idx_linkedin_profile_registry_status; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_linkedin_profile_registry_status ON linkedin_profile_registry USING btree (status, updated_at);

-- Name: idx_manual_review_items_review_item_id_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_manual_review_items_review_item_id_unique ON manual_review_items USING btree (review_item_id);

-- Name: idx_operation_events_action; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_operation_events_action ON operation_events USING btree (action_id, recorded_at);

-- Name: idx_operation_events_stream_idempotency_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_operation_events_stream_idempotency_unique ON operation_events USING btree (event_stream_id, idempotency_key);

-- Name: idx_operation_events_stream_sequence; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_operation_events_stream_sequence ON operation_events USING btree (event_stream_id, sequence_number);

-- Name: idx_operation_runs_action; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_operation_runs_action ON operation_runs USING btree (action_id, status, updated_at);

-- Name: idx_operation_runs_owner; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_operation_runs_owner ON operation_runs USING btree (owner_module, operation_type, status, updated_at);

-- Name: idx_operation_runs_workspace_idempotency_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_operation_runs_workspace_idempotency_unique ON operation_runs USING btree (workspace_id, idempotency_key);

-- Name: idx_organization_asset_registry_registry_id_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_organization_asset_registry_registry_id_unique ON organization_asset_registry USING btree (registry_id);

-- Name: idx_organization_asset_registry_target_snapshot_view; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_organization_asset_registry_target_snapshot_view ON organization_asset_registry USING btree (target_company, snapshot_id, asset_view);

-- Name: idx_organization_execution_profiles_profile_id_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_organization_execution_profiles_profile_id_unique ON organization_execution_profiles USING btree (profile_id);

-- Name: idx_organization_execution_profiles_target_view; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_organization_execution_profiles_target_view ON organization_execution_profiles USING btree (target_company, asset_view);

-- Name: idx_person_assertions_identity; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_person_assertions_identity ON person_assertions USING btree (person_identity_key, assertion_type, verification_status, updated_at);

-- Name: idx_person_assets_identity; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_person_assets_identity ON person_assets USING btree (person_identity_key, asset_type, updated_at);

-- Name: idx_person_evidence_identity; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_person_evidence_identity ON person_evidence USING btree (person_identity_key, evidence_type, updated_at);

-- Name: idx_person_public_web_assets_identity; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_person_public_web_assets_identity ON person_public_web_assets USING btree (linkedin_url_key, updated_at);

-- Name: idx_person_public_web_signals_identity; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_person_public_web_signals_identity ON person_public_web_signals USING btree (person_identity_key, signal_kind, updated_at);

-- Name: idx_person_public_web_signals_record; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_person_public_web_signals_record ON person_public_web_signals USING btree (record_id, updated_at);

-- Name: idx_person_public_web_signals_run; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_person_public_web_signals_run ON person_public_web_signals USING btree (run_id, signal_kind, updated_at);

-- Name: idx_plan_review_sessions_review_id_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_plan_review_sessions_review_id_unique ON plan_review_sessions USING btree (review_id);

-- Name: idx_projection_person_search_index_person; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_projection_person_search_index_person ON projection_person_search_index USING btree (person_identity_key, updated_at);

-- Name: idx_projection_person_search_index_projection; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_projection_person_search_index_projection ON projection_person_search_index USING btree (projection_id, count_scope, updated_at);

-- Name: idx_query_dispatches_dispatch_id_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_query_dispatches_dispatch_id_unique ON query_dispatches USING btree (dispatch_id);

-- Name: idx_raw_profile_index_watermark; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_raw_profile_index_watermark ON raw_profile_index USING btree (raw_profile_index_watermark, updated_at);

-- Name: idx_runtime_outbox_ready; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_runtime_outbox_ready ON runtime_outbox USING btree (outbox_type, status, not_before_at, lease_expires_at, updated_at);

-- Name: idx_runtime_outbox_run; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_runtime_outbox_run ON runtime_outbox USING btree (workflow_run_id, status, updated_at);

-- Name: idx_runtime_provider_limiter_key_expires; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_runtime_provider_limiter_key_expires ON runtime_provider_limiter_leases USING btree (limiter_key, lease_expires_at);

-- Name: idx_target_candidates_candidate; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_target_candidates_candidate ON target_candidates USING btree (candidate_id, updated_at);

-- Name: idx_target_candidates_person; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_target_candidates_person ON target_candidates USING btree (person_identity_key, updated_at);

-- Name: idx_target_candidates_projection; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_target_candidates_projection ON target_candidates USING btree (source_projection_id, updated_at);

-- Name: idx_target_candidates_updated; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_target_candidates_updated ON target_candidates USING btree (updated_at, follow_up_status);

-- Name: idx_workflow_activity_attempts_activity; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_workflow_activity_attempts_activity ON workflow_activity_attempts USING btree (activity_run_id, status, updated_at);

-- Name: idx_workflow_activity_attempts_command; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_workflow_activity_attempts_command ON workflow_activity_attempts USING btree (command_id);

-- Name: idx_workflow_activity_attempts_workflow; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_workflow_activity_attempts_workflow ON workflow_activity_attempts USING btree (workflow_run_id, status, updated_at);

-- Name: idx_workflow_activity_attempts_workspace_idempotency_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_workflow_activity_attempts_workspace_idempotency_unique ON workflow_activity_attempts USING btree (workspace_id, idempotency_key);

-- Name: idx_workflow_activity_runs_acquisition; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_workflow_activity_runs_acquisition ON workflow_activity_runs USING btree (acquisition_run_id, status, updated_at);

-- Name: idx_workflow_activity_runs_command; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_workflow_activity_runs_command ON workflow_activity_runs USING btree (command_id);

-- Name: idx_workflow_activity_runs_workflow; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_workflow_activity_runs_workflow ON workflow_activity_runs USING btree (workflow_run_id, activity_type, status, updated_at);

-- Name: idx_workflow_activity_runs_workspace_idempotency_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_workflow_activity_runs_workspace_idempotency_unique ON workflow_activity_runs USING btree (workspace_id, idempotency_key);

-- Name: idx_workflow_commands_causal_group; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_workflow_commands_causal_group ON workflow_commands USING btree (causal_group_id, source_event_id, updated_at);

-- Name: idx_workflow_commands_ready; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_workflow_commands_ready ON workflow_commands USING btree (owner, command_type, status, not_before_at, lease_expires_at, updated_at);

-- Name: idx_workflow_commands_run; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_workflow_commands_run ON workflow_commands USING btree (workflow_run_id, status, updated_at);

-- Name: idx_workflow_commands_run_idempotency_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_workflow_commands_run_idempotency_unique ON workflow_commands USING btree (workflow_run_id, idempotency_key);

-- Name: idx_workflow_commands_source_event; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_workflow_commands_source_event ON workflow_commands USING btree (source_event_id, command_type, updated_at);

-- Name: idx_workflow_current_state_status; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_workflow_current_state_status ON workflow_current_state USING btree (status, updated_at);

-- Name: idx_workflow_entity_deltas_activity; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_workflow_entity_deltas_activity ON workflow_entity_deltas USING btree (activity_run_id, entity_type, status, updated_at);

-- Name: idx_workflow_entity_deltas_command; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_workflow_entity_deltas_command ON workflow_entity_deltas USING btree (command_id, status, updated_at);

-- Name: idx_workflow_entity_deltas_workflow; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_workflow_entity_deltas_workflow ON workflow_entity_deltas USING btree (workflow_run_id, entity_type, entity_key, updated_at);

-- Name: idx_workflow_entity_deltas_workspace_idempotency_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_workflow_entity_deltas_workspace_idempotency_unique ON workflow_entity_deltas USING btree (workspace_id, idempotency_key);

-- Name: idx_workflow_events_command; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_workflow_events_command ON workflow_events USING btree (command_id, recorded_at);

-- Name: idx_workflow_events_run_idempotency_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_workflow_events_run_idempotency_unique ON workflow_events USING btree (workflow_run_id, idempotency_key);

-- Name: idx_workflow_events_run_sequence; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_workflow_events_run_sequence ON workflow_events USING btree (workflow_run_id, sequence_number);

-- Name: idx_workflow_events_run_sequence_unique; Type: INDEX; Schema: -; Owner: -

CREATE UNIQUE INDEX idx_workflow_events_run_sequence_unique ON workflow_events USING btree (workflow_run_id, sequence_number);

-- Name: idx_workflow_job_leases_expires; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_workflow_job_leases_expires ON workflow_job_leases USING btree (lease_expires_at);

-- Name: idx_workflow_recovery_intents_status; Type: INDEX; Schema: -; Owner: -

CREATE INDEX idx_workflow_recovery_intents_status ON workflow_recovery_intents USING btree (status, lease_expires_at);
