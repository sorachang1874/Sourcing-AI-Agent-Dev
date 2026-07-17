-- Track D D1n S1a: exact Agent tool occurrences and terminal winners.
--
-- This migration is storage only.  It does not populate the Agent catalog,
-- activate a tool, grant provider/model authority, or make a tool served.
-- Pending occurrences are immutable logical calls.  One pending row may move
-- once to accepted; every physical result attempt is append-only and late or
-- stale attempts remain quarantined evidence.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- Historical tool identity is copied into action/run aggregates when present.
-- Brownfield aggregates stay explicitly unpinned until their owning writer is
-- migrated; partial tool pins are impossible.
ALTER TABLE agent_actions
    ADD COLUMN tool_name TEXT NOT NULL DEFAULT '',
    ADD COLUMN tool_spec_version TEXT NOT NULL DEFAULT '',
    ADD COLUMN tool_spec_digest TEXT NOT NULL DEFAULT '',
    ADD CONSTRAINT agent_actions_tool_pin_group_ck CHECK (
        (
            tool_name = ''
            AND tool_spec_version = ''
            AND tool_spec_digest = ''
        )
        OR (
            tool_name ~ '^[A-Za-z][A-Za-z0-9_]{0,63}$'
            AND tool_spec_version ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
            AND tool_spec_digest ~ '^[0-9a-f]{64}$'
        )
    );

ALTER TABLE operation_runs
    ADD COLUMN tool_name TEXT NOT NULL DEFAULT '',
    ADD COLUMN tool_spec_version TEXT NOT NULL DEFAULT '',
    ADD COLUMN tool_spec_digest TEXT NOT NULL DEFAULT '',
    ADD CONSTRAINT operation_runs_tool_pin_group_ck CHECK (
        (
            tool_name = ''
            AND tool_spec_version = ''
            AND tool_spec_digest = ''
        )
        OR (
            tool_name ~ '^[A-Za-z][A-Za-z0-9_]{0,63}$'
            AND tool_spec_version ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
            AND tool_spec_digest ~ '^[0-9a-f]{64}$'
        )
    );

CREATE TABLE agent_tool_result_slots (
    result_slot_id TEXT PRIMARY KEY,
    slot_generation BIGINT NOT NULL,
    workspace_id TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    runtime_namespace TEXT NOT NULL,
    provider_mode TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    step_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    tool_kind TEXT NOT NULL,
    effect_class TEXT NOT NULL,
    tool_spec_version TEXT NOT NULL,
    tool_spec_digest TEXT NOT NULL,
    canonical_args_json JSONB NOT NULL,
    canonical_args_digest TEXT NOT NULL,
    occurrence_ordinal BIGINT NOT NULL,
    logical_occurrence_digest TEXT NOT NULL,
    request_schema_version TEXT NOT NULL,
    request_schema_digest TEXT NOT NULL,
    result_schema_version TEXT NOT NULL,
    result_schema_digest TEXT NOT NULL,
    serializer_owner TEXT NOT NULL,
    serializer_revision TEXT NOT NULL,
    serializer_contract_digest TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    result_attempt_id TEXT NOT NULL DEFAULT '',
    provider_call_id TEXT NOT NULL DEFAULT '',
    tool_call_id TEXT NOT NULL DEFAULT '',
    action_id TEXT NOT NULL DEFAULT '',
    operation_run_id TEXT NOT NULL DEFAULT '',
    workflow_command_id TEXT NOT NULL DEFAULT '',
    activity_run_id TEXT NOT NULL DEFAULT '',
    activity_attempt_id TEXT NOT NULL DEFAULT '',
    command_attempt BIGINT NOT NULL DEFAULT 0,
    command_generation BIGINT NOT NULL DEFAULT 0,
    control_epoch BIGINT NOT NULL DEFAULT 0,
    owner_target_kind TEXT NOT NULL DEFAULT '',
    owner_target_id TEXT NOT NULL DEFAULT '',
    owner_target_revision BIGINT NOT NULL DEFAULT 0,
    owner_target_generation BIGINT NOT NULL DEFAULT 0,
    terminal_winner_id TEXT NOT NULL DEFAULT '',
    owner_result_ref_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    owner_result_digest TEXT NOT NULL DEFAULT '',
    serialized_result_json TEXT NOT NULL DEFAULT '',
    serialized_result_digest TEXT NOT NULL DEFAULT '',
    tool_result_message_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    tool_result_message_digest TEXT NOT NULL DEFAULT '',
    is_error BOOLEAN NOT NULL DEFAULT FALSE,
    schema_version TEXT NOT NULL DEFAULT 'agent_tool_result_slot_v1',
    created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    accepted_at TIMESTAMPTZ,
    UNIQUE (
        workspace_id,
        actor_id,
        runtime_namespace,
        provider_mode,
        turn_id,
        step_id,
        tool_name,
        canonical_args_digest,
        occurrence_ordinal
    ),
    UNIQUE (logical_occurrence_digest),
    CONSTRAINT agent_tool_slots_identity_shape_ck CHECK (
        result_slot_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$'
        AND slot_generation > 0
        AND char_length(workspace_id) BETWEEN 1 AND 1024
        AND workspace_id !~ '[[:cntrl:]]'
        AND char_length(actor_id) BETWEEN 1 AND 1024
        AND actor_id !~ '[[:cntrl:]]'
        AND runtime_namespace ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$'
        AND provider_mode IN ('simulate', 'scripted', 'live', 'replay')
        AND turn_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$'
        AND step_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$'
        AND tool_name ~ '^[A-Za-z][A-Za-z0-9_]{0,63}$'
        AND tool_kind IN ('action', 'query')
        AND effect_class IN ('read_only', 'commandless_action', 'command_backed_action')
        AND (tool_kind <> 'query' OR effect_class = 'read_only')
        AND occurrence_ordinal > 0
    ),
    CONSTRAINT agent_tool_slots_pin_shape_ck CHECK (
        tool_spec_version ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
        AND tool_spec_digest ~ '^[0-9a-f]{64}$'
        AND canonical_args_digest ~ '^[0-9a-f]{64}$'
        AND logical_occurrence_digest ~ '^[0-9a-f]{64}$'
        AND request_schema_version ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
        AND request_schema_digest ~ '^[0-9a-f]{64}$'
        AND result_schema_version ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
        AND result_schema_digest ~ '^[0-9a-f]{64}$'
        AND serializer_owner ~ '^[A-Za-z][A-Za-z0-9_.:-]{0,255}$'
        AND serializer_revision ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
        AND serializer_contract_digest ~ '^[0-9a-f]{64}$'
        AND jsonb_typeof(canonical_args_json) IS NOT DISTINCT FROM 'object'
        AND schema_version = 'agent_tool_result_slot_v1'
    ),
    CONSTRAINT agent_tool_slots_terminal_shape_ck CHECK (
        (
            status = 'pending'
            AND result_attempt_id = ''
            AND provider_call_id = ''
            AND tool_call_id = ''
            AND action_id = ''
            AND operation_run_id = ''
            AND workflow_command_id = ''
            AND activity_run_id = ''
            AND activity_attempt_id = ''
            AND command_attempt = 0
            AND command_generation = 0
            AND control_epoch = 0
            AND owner_target_kind = ''
            AND owner_target_id = ''
            AND owner_target_revision = 0
            AND owner_target_generation = 0
            AND terminal_winner_id = ''
            AND owner_result_ref_json = '{}'::jsonb
            AND owner_result_digest = ''
            AND serialized_result_json = ''
            AND serialized_result_digest = ''
            AND tool_result_message_json = '{}'::jsonb
            AND tool_result_message_digest = ''
            AND is_error = FALSE
            AND accepted_at IS NULL
        )
        OR (
            status = 'accepted'
            AND result_attempt_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$'
            AND char_length(provider_call_id) BETWEEN 1 AND 1024
            AND provider_call_id !~ '[[:cntrl:]]'
            AND char_length(tool_call_id) BETWEEN 1 AND 1024
            AND tool_call_id !~ '[[:cntrl:]]'
            AND owner_target_kind ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
            AND owner_target_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$'
            AND owner_target_revision >= 0
            AND owner_target_generation >= 0
            AND (owner_target_revision > 0 OR owner_target_generation > 0)
            AND terminal_winner_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$'
            AND jsonb_typeof(owner_result_ref_json) IS NOT DISTINCT FROM 'object'
            AND owner_result_ref_json <> '{}'::jsonb
            AND owner_result_digest ~ '^[0-9a-f]{64}$'
            AND char_length(serialized_result_json) > 1
            AND serialized_result_digest ~ '^[0-9a-f]{64}$'
            AND jsonb_typeof(tool_result_message_json) IS NOT DISTINCT FROM 'object'
            AND tool_result_message_json <> '{}'::jsonb
            AND tool_result_message_digest ~ '^[0-9a-f]{64}$'
            AND accepted_at IS NOT NULL
            AND (
                (
                    effect_class = 'command_backed_action'
                    AND action_id <> ''
                    AND operation_run_id <> ''
                    AND workflow_command_id <> ''
                    AND activity_run_id <> ''
                    AND activity_attempt_id <> ''
                    AND command_attempt > 0
                    AND command_generation > 0
                    AND control_epoch > 0
                )
                OR (
                    effect_class = 'commandless_action'
                    AND action_id <> ''
                    AND operation_run_id <> ''
                    AND workflow_command_id = ''
                    AND activity_run_id = ''
                    AND activity_attempt_id = ''
                    AND command_attempt = 0
                    AND command_generation = 0
                    AND control_epoch = 0
                )
                OR (
                    effect_class = 'read_only'
                    AND ((action_id = '' AND operation_run_id = '') OR (action_id <> '' AND operation_run_id <> ''))
                    AND workflow_command_id = ''
                    AND activity_run_id = ''
                    AND activity_attempt_id = ''
                    AND command_attempt = 0
                    AND command_generation = 0
                    AND control_epoch = 0
                )
            )
        )
    )
);

CREATE TABLE agent_tool_result_attempts (
    result_attempt_id TEXT PRIMARY KEY,
    result_slot_id TEXT NOT NULL,
    attempted_slot_generation BIGINT NOT NULL,
    disposition TEXT NOT NULL,
    quarantine_reason TEXT NOT NULL DEFAULT '',
    provider_call_id TEXT NOT NULL,
    tool_call_id TEXT NOT NULL,
    action_id TEXT NOT NULL DEFAULT '',
    operation_run_id TEXT NOT NULL DEFAULT '',
    workflow_command_id TEXT NOT NULL DEFAULT '',
    activity_run_id TEXT NOT NULL DEFAULT '',
    activity_attempt_id TEXT NOT NULL DEFAULT '',
    command_attempt BIGINT NOT NULL DEFAULT 0,
    command_generation BIGINT NOT NULL DEFAULT 0,
    control_epoch BIGINT NOT NULL DEFAULT 0,
    owner_target_kind TEXT NOT NULL,
    owner_target_id TEXT NOT NULL,
    owner_target_revision BIGINT NOT NULL DEFAULT 0,
    owner_target_generation BIGINT NOT NULL DEFAULT 0,
    terminal_winner_id TEXT NOT NULL,
    owner_result_ref_json JSONB NOT NULL,
    owner_result_digest TEXT NOT NULL,
    serialized_result_json TEXT NOT NULL,
    serialized_result_digest TEXT NOT NULL,
    tool_result_message_json JSONB NOT NULL,
    tool_result_message_digest TEXT NOT NULL,
    is_error BOOLEAN NOT NULL,
    schema_version TEXT NOT NULL DEFAULT 'agent_tool_result_attempt_v1',
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    CONSTRAINT agent_tool_attempts_slot_fk
        FOREIGN KEY (result_slot_id) REFERENCES agent_tool_result_slots (result_slot_id) ON DELETE RESTRICT,
    CONSTRAINT agent_tool_attempts_shape_ck CHECK (
        result_attempt_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$'
        AND attempted_slot_generation > 0
        AND disposition IN ('accepted', 'quarantined')
        AND ((disposition = 'accepted' AND quarantine_reason = '')
             OR (disposition = 'quarantined' AND char_length(quarantine_reason) BETWEEN 1 AND 255))
        AND char_length(provider_call_id) BETWEEN 1 AND 1024
        AND provider_call_id !~ '[[:cntrl:]]'
        AND char_length(tool_call_id) BETWEEN 1 AND 1024
        AND tool_call_id !~ '[[:cntrl:]]'
        AND command_attempt >= 0
        AND command_generation >= 0
        AND control_epoch >= 0
        AND owner_target_kind ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
        AND owner_target_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$'
        AND owner_target_revision >= 0
        AND owner_target_generation >= 0
        AND (owner_target_revision > 0 OR owner_target_generation > 0)
        AND terminal_winner_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$'
        AND jsonb_typeof(owner_result_ref_json) IS NOT DISTINCT FROM 'object'
        AND owner_result_ref_json <> '{}'::jsonb
        AND owner_result_digest ~ '^[0-9a-f]{64}$'
        AND char_length(serialized_result_json) > 1
        AND serialized_result_digest ~ '^[0-9a-f]{64}$'
        AND jsonb_typeof(tool_result_message_json) IS NOT DISTINCT FROM 'object'
        AND tool_result_message_json <> '{}'::jsonb
        AND tool_result_message_digest ~ '^[0-9a-f]{64}$'
        AND schema_version = 'agent_tool_result_attempt_v1'
    )
);

CREATE UNIQUE INDEX agent_tool_attempts_one_accepted_idx
    ON agent_tool_result_attempts (result_slot_id)
    WHERE disposition = 'accepted';

CREATE INDEX agent_tool_attempts_slot_recorded_idx
    ON agent_tool_result_attempts (result_slot_id, recorded_at, result_attempt_id);

CREATE TABLE agent_tool_result_journal (
    journal_id TEXT PRIMARY KEY,
    result_slot_id TEXT NOT NULL UNIQUE,
    result_attempt_id TEXT NOT NULL UNIQUE,
    workspace_id TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    runtime_namespace TEXT NOT NULL,
    provider_mode TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    step_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    tool_spec_version TEXT NOT NULL,
    tool_spec_digest TEXT NOT NULL,
    canonical_args_digest TEXT NOT NULL,
    occurrence_ordinal BIGINT NOT NULL,
    request_schema_version TEXT NOT NULL,
    request_schema_digest TEXT NOT NULL,
    result_schema_version TEXT NOT NULL,
    result_schema_digest TEXT NOT NULL,
    serializer_owner TEXT NOT NULL,
    serializer_revision TEXT NOT NULL,
    serializer_contract_digest TEXT NOT NULL,
    action_id TEXT NOT NULL DEFAULT '',
    operation_run_id TEXT NOT NULL DEFAULT '',
    workflow_command_id TEXT NOT NULL DEFAULT '',
    activity_run_id TEXT NOT NULL DEFAULT '',
    activity_attempt_id TEXT NOT NULL DEFAULT '',
    command_attempt BIGINT NOT NULL DEFAULT 0,
    command_generation BIGINT NOT NULL DEFAULT 0,
    control_epoch BIGINT NOT NULL DEFAULT 0,
    owner_target_kind TEXT NOT NULL,
    owner_target_id TEXT NOT NULL,
    owner_target_revision BIGINT NOT NULL,
    owner_target_generation BIGINT NOT NULL,
    terminal_winner_id TEXT NOT NULL,
    owner_result_ref_json JSONB NOT NULL,
    owner_result_digest TEXT NOT NULL,
    serialized_result_json TEXT NOT NULL,
    serialized_result_digest TEXT NOT NULL,
    tool_result_message_json JSONB NOT NULL,
    tool_result_message_digest TEXT NOT NULL,
    is_error BOOLEAN NOT NULL,
    schema_version TEXT NOT NULL DEFAULT 'agent_tool_result_journal_v1',
    accepted_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT agent_tool_journal_slot_fk
        FOREIGN KEY (result_slot_id) REFERENCES agent_tool_result_slots (result_slot_id) ON DELETE RESTRICT,
    CONSTRAINT agent_tool_journal_attempt_fk
        FOREIGN KEY (result_attempt_id) REFERENCES agent_tool_result_attempts (result_attempt_id) ON DELETE RESTRICT,
    CONSTRAINT agent_tool_journal_shape_ck CHECK (
        journal_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$'
        AND runtime_namespace ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$'
        AND provider_mode IN ('simulate', 'scripted', 'live', 'replay')
        AND tool_name ~ '^[A-Za-z][A-Za-z0-9_]{0,63}$'
        AND tool_spec_version ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
        AND tool_spec_digest ~ '^[0-9a-f]{64}$'
        AND canonical_args_digest ~ '^[0-9a-f]{64}$'
        AND occurrence_ordinal > 0
        AND request_schema_version ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
        AND request_schema_digest ~ '^[0-9a-f]{64}$'
        AND result_schema_version ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
        AND result_schema_digest ~ '^[0-9a-f]{64}$'
        AND serializer_owner ~ '^[A-Za-z][A-Za-z0-9_.:-]{0,255}$'
        AND serializer_revision ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
        AND serializer_contract_digest ~ '^[0-9a-f]{64}$'
        AND command_attempt >= 0
        AND command_generation >= 0
        AND control_epoch >= 0
        AND owner_target_kind ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
        AND owner_target_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$'
        AND owner_target_revision >= 0
        AND owner_target_generation >= 0
        AND (owner_target_revision > 0 OR owner_target_generation > 0)
        AND terminal_winner_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$'
        AND jsonb_typeof(owner_result_ref_json) IS NOT DISTINCT FROM 'object'
        AND owner_result_ref_json <> '{}'::jsonb
        AND owner_result_digest ~ '^[0-9a-f]{64}$'
        AND char_length(serialized_result_json) > 1
        AND serialized_result_digest ~ '^[0-9a-f]{64}$'
        AND jsonb_typeof(tool_result_message_json) IS NOT DISTINCT FROM 'object'
        AND tool_result_message_json <> '{}'::jsonb
        AND tool_result_message_digest ~ '^[0-9a-f]{64}$'
        AND schema_version = 'agent_tool_result_journal_v1'
    )
);

CREATE FUNCTION guard_agent_tool_result_slot_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'UPDATE'
       AND OLD.status = 'pending'
       AND NEW.status = 'accepted'
       AND ROW(
            OLD.result_slot_id, OLD.slot_generation, OLD.workspace_id, OLD.actor_id,
            OLD.runtime_namespace, OLD.provider_mode, OLD.turn_id, OLD.step_id,
            OLD.tool_name, OLD.tool_kind, OLD.effect_class, OLD.tool_spec_version,
            OLD.tool_spec_digest, OLD.canonical_args_json, OLD.canonical_args_digest,
            OLD.occurrence_ordinal, OLD.logical_occurrence_digest,
            OLD.request_schema_version, OLD.request_schema_digest,
            OLD.result_schema_version, OLD.result_schema_digest,
            OLD.serializer_owner, OLD.serializer_revision, OLD.serializer_contract_digest,
            OLD.schema_version, OLD.created_at
       ) IS NOT DISTINCT FROM ROW(
            NEW.result_slot_id, NEW.slot_generation, NEW.workspace_id, NEW.actor_id,
            NEW.runtime_namespace, NEW.provider_mode, NEW.turn_id, NEW.step_id,
            NEW.tool_name, NEW.tool_kind, NEW.effect_class, NEW.tool_spec_version,
            NEW.tool_spec_digest, NEW.canonical_args_json, NEW.canonical_args_digest,
            NEW.occurrence_ordinal, NEW.logical_occurrence_digest,
            NEW.request_schema_version, NEW.request_schema_digest,
            NEW.result_schema_version, NEW.result_schema_digest,
            NEW.serializer_owner, NEW.serializer_revision, NEW.serializer_contract_digest,
            NEW.schema_version, NEW.created_at
       )
    THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING
        ERRCODE = 'P0001',
        CONSTRAINT = 'agent_tool_result_slots_immutable',
        MESSAGE = 'agent_tool_result_slots permits only exact pending-to-accepted CAS';
END;
$$;

CREATE TRIGGER agent_tool_result_slots_guard_trg
BEFORE UPDATE OR DELETE ON agent_tool_result_slots
FOR EACH ROW EXECUTE FUNCTION guard_agent_tool_result_slot_mutation();

CREATE FUNCTION reject_agent_tool_result_append_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = 'P0001',
        CONSTRAINT = 'agent_tool_result_append_only',
        MESSAGE = TG_TABLE_NAME || ' rows are append-only';
END;
$$;

CREATE TRIGGER agent_tool_result_attempts_immutable_trg
BEFORE UPDATE OR DELETE ON agent_tool_result_attempts
FOR EACH ROW EXECUTE FUNCTION reject_agent_tool_result_append_mutation();

CREATE TRIGGER agent_tool_result_journal_immutable_trg
BEFORE UPDATE OR DELETE ON agent_tool_result_journal
FOR EACH ROW EXECUTE FUNCTION reject_agent_tool_result_append_mutation();

-- The three accepted records are one aggregate even though their write order
-- is attempt -> slot CAS -> journal.  Deferred constraint triggers inspect the
-- final transaction state and reject any standalone accepted row or cross-slot
-- pairing. Pending slots and quarantined attempts remain independently valid.
CREATE FUNCTION validate_agent_tool_terminal_aggregate()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    checked_slot_id TEXT;
    checked_attempt_id TEXT;
BEGIN
    IF TG_TABLE_NAME = 'agent_tool_result_slots' THEN
        IF NEW.status <> 'accepted' THEN
            RETURN NEW;
        END IF;
        checked_slot_id := NEW.result_slot_id;
        checked_attempt_id := NEW.result_attempt_id;
    ELSIF TG_TABLE_NAME = 'agent_tool_result_attempts' THEN
        IF NEW.disposition <> 'accepted' THEN
            RETURN NEW;
        END IF;
        checked_slot_id := NEW.result_slot_id;
        checked_attempt_id := NEW.result_attempt_id;
    ELSE
        checked_slot_id := NEW.result_slot_id;
        checked_attempt_id := NEW.result_attempt_id;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM agent_tool_result_slots AS slot
        JOIN agent_tool_result_attempts AS attempt
          ON attempt.result_slot_id = slot.result_slot_id
         AND attempt.result_attempt_id = slot.result_attempt_id
         AND attempt.disposition = 'accepted'
        JOIN agent_tool_result_journal AS journal
          ON journal.result_slot_id = slot.result_slot_id
         AND journal.result_attempt_id = attempt.result_attempt_id
        WHERE slot.result_slot_id = checked_slot_id
          AND slot.result_attempt_id = checked_attempt_id
          AND slot.status = 'accepted'
          AND attempt.attempted_slot_generation = slot.slot_generation
          AND attempt.provider_call_id = slot.provider_call_id
          AND attempt.tool_call_id = slot.tool_call_id
          AND attempt.action_id = slot.action_id
          AND attempt.operation_run_id = slot.operation_run_id
          AND attempt.workflow_command_id = slot.workflow_command_id
          AND attempt.activity_run_id = slot.activity_run_id
          AND attempt.activity_attempt_id = slot.activity_attempt_id
          AND attempt.command_attempt = slot.command_attempt
          AND attempt.command_generation = slot.command_generation
          AND attempt.control_epoch = slot.control_epoch
          AND attempt.owner_target_kind = slot.owner_target_kind
          AND attempt.owner_target_id = slot.owner_target_id
          AND attempt.owner_target_revision = slot.owner_target_revision
          AND attempt.owner_target_generation = slot.owner_target_generation
          AND attempt.terminal_winner_id = slot.terminal_winner_id
          AND attempt.owner_result_ref_json = slot.owner_result_ref_json
          AND attempt.owner_result_digest = slot.owner_result_digest
          AND attempt.serialized_result_json = slot.serialized_result_json
          AND attempt.serialized_result_digest = slot.serialized_result_digest
          AND attempt.tool_result_message_json = slot.tool_result_message_json
          AND attempt.tool_result_message_digest = slot.tool_result_message_digest
          AND attempt.is_error = slot.is_error
          AND journal.workspace_id = slot.workspace_id
          AND journal.actor_id = slot.actor_id
          AND journal.runtime_namespace = slot.runtime_namespace
          AND journal.provider_mode = slot.provider_mode
          AND journal.turn_id = slot.turn_id
          AND journal.step_id = slot.step_id
          AND journal.tool_name = slot.tool_name
          AND journal.tool_spec_version = slot.tool_spec_version
          AND journal.tool_spec_digest = slot.tool_spec_digest
          AND journal.canonical_args_digest = slot.canonical_args_digest
          AND journal.occurrence_ordinal = slot.occurrence_ordinal
          AND journal.request_schema_version = slot.request_schema_version
          AND journal.request_schema_digest = slot.request_schema_digest
          AND journal.result_schema_version = slot.result_schema_version
          AND journal.result_schema_digest = slot.result_schema_digest
          AND journal.serializer_owner = slot.serializer_owner
          AND journal.serializer_revision = slot.serializer_revision
          AND journal.serializer_contract_digest = slot.serializer_contract_digest
          AND journal.action_id = slot.action_id
          AND journal.operation_run_id = slot.operation_run_id
          AND journal.workflow_command_id = slot.workflow_command_id
          AND journal.activity_run_id = slot.activity_run_id
          AND journal.activity_attempt_id = slot.activity_attempt_id
          AND journal.command_attempt = slot.command_attempt
          AND journal.command_generation = slot.command_generation
          AND journal.control_epoch = slot.control_epoch
          AND journal.owner_target_kind = slot.owner_target_kind
          AND journal.owner_target_id = slot.owner_target_id
          AND journal.owner_target_revision = slot.owner_target_revision
          AND journal.owner_target_generation = slot.owner_target_generation
          AND journal.terminal_winner_id = slot.terminal_winner_id
          AND journal.owner_result_ref_json = slot.owner_result_ref_json
          AND journal.owner_result_digest = slot.owner_result_digest
          AND journal.serialized_result_json = slot.serialized_result_json
          AND journal.serialized_result_digest = slot.serialized_result_digest
          AND journal.tool_result_message_json = slot.tool_result_message_json
          AND journal.tool_result_message_digest = slot.tool_result_message_digest
          AND journal.is_error = slot.is_error
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            CONSTRAINT = 'agent_tool_terminal_aggregate_incomplete',
            MESSAGE = 'accepted result slot, attempt, and journal must commit as one exact aggregate';
    END IF;
    RETURN NEW;
END;
$$;

CREATE CONSTRAINT TRIGGER agent_tool_slot_terminal_aggregate_trg
AFTER INSERT OR UPDATE ON agent_tool_result_slots
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_agent_tool_terminal_aggregate();

CREATE CONSTRAINT TRIGGER agent_tool_attempt_terminal_aggregate_trg
AFTER INSERT ON agent_tool_result_attempts
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_agent_tool_terminal_aggregate();

CREATE CONSTRAINT TRIGGER agent_tool_journal_terminal_aggregate_trg
AFTER INSERT ON agent_tool_result_journal
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_agent_tool_terminal_aggregate();

SET LOCAL lock_timeout = DEFAULT;
SET LOCAL statement_timeout = DEFAULT;
