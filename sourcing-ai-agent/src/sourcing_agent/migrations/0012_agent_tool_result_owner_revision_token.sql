-- Track D D1n S1c: opaque equality-only physical-owner revision carrier.
--
-- Some owners publish a canonical revision token rather than a numeric revision
-- or generation.  Preserve that token byte-for-byte: the result layer may test
-- equality, but must not parse, order, normalize, truncate, or hash it into an
-- invented numeric revision.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE agent_tool_result_slots
    ADD COLUMN owner_target_revision_token TEXT NOT NULL DEFAULT '';

ALTER TABLE agent_tool_result_attempts
    ADD COLUMN owner_target_revision_token TEXT NOT NULL DEFAULT '';

ALTER TABLE agent_tool_result_journal
    ADD COLUMN owner_target_revision_token TEXT NOT NULL DEFAULT '';

-- Fixed-forward the three physical-owner version-presence checks.  Existing
-- numeric-only aggregates remain valid because the new column defaults empty.
-- A populated token has the same canonical identifier carrier shape as the
-- Python result contract: ASCII, no normalization, and at most 256 bytes.
-- The slot schema remains v1 because it identifies the logical occurrence.
-- Attempt/journal defaults also remain v1 for rolling old writers, while v2
-- explicitly denotes a row that understands the optional opaque carrier.
ALTER TABLE agent_tool_result_slots
    DROP CONSTRAINT agent_tool_slots_terminal_shape_ck,
    ADD CONSTRAINT agent_tool_slots_terminal_shape_ck CHECK (
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
            AND owner_target_revision_token = ''
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
            AND (
                owner_target_revision_token = ''
                OR owner_target_revision_token ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$'
            )
            AND (
                owner_target_revision > 0
                OR owner_target_generation > 0
                OR owner_target_revision_token <> ''
            )
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
    );

ALTER TABLE agent_tool_result_attempts
    DROP CONSTRAINT agent_tool_attempts_shape_ck,
    ADD CONSTRAINT agent_tool_attempts_shape_ck CHECK (
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
        AND (
            owner_target_revision_token = ''
            OR owner_target_revision_token ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$'
        )
        AND (
            (
                schema_version = 'agent_tool_result_attempt_v1'
                AND owner_target_revision_token = ''
                AND (owner_target_revision > 0 OR owner_target_generation > 0)
            )
            OR (
                schema_version = 'agent_tool_result_attempt_v2'
                AND (
                    owner_target_revision > 0
                    OR owner_target_generation > 0
                    OR owner_target_revision_token <> ''
                )
            )
        )
        AND terminal_winner_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$'
        AND jsonb_typeof(owner_result_ref_json) IS NOT DISTINCT FROM 'object'
        AND owner_result_ref_json <> '{}'::jsonb
        AND owner_result_digest ~ '^[0-9a-f]{64}$'
        AND char_length(serialized_result_json) > 1
        AND serialized_result_digest ~ '^[0-9a-f]{64}$'
        AND jsonb_typeof(tool_result_message_json) IS NOT DISTINCT FROM 'object'
        AND tool_result_message_json <> '{}'::jsonb
        AND tool_result_message_digest ~ '^[0-9a-f]{64}$'
    );

ALTER TABLE agent_tool_result_journal
    DROP CONSTRAINT agent_tool_journal_shape_ck,
    ADD CONSTRAINT agent_tool_journal_shape_ck CHECK (
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
        AND (
            owner_target_revision_token = ''
            OR owner_target_revision_token ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$'
        )
        AND (
            (
                schema_version = 'agent_tool_result_journal_v1'
                AND owner_target_revision_token = ''
                AND (owner_target_revision > 0 OR owner_target_generation > 0)
            )
            OR (
                schema_version = 'agent_tool_result_journal_v2'
                AND (
                    owner_target_revision > 0
                    OR owner_target_generation > 0
                    OR owner_target_revision_token <> ''
                )
            )
        )
        AND terminal_winner_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$'
        AND jsonb_typeof(owner_result_ref_json) IS NOT DISTINCT FROM 'object'
        AND owner_result_ref_json <> '{}'::jsonb
        AND owner_result_digest ~ '^[0-9a-f]{64}$'
        AND char_length(serialized_result_json) > 1
        AND serialized_result_digest ~ '^[0-9a-f]{64}$'
        AND jsonb_typeof(tool_result_message_json) IS NOT DISTINCT FROM 'object'
        AND tool_result_message_json <> '{}'::jsonb
        AND tool_result_message_digest ~ '^[0-9a-f]{64}$'
    );

-- The accepted slot, attempt, and journal remain one exact aggregate.  Attempts
-- and journals must also use a matched v1/v1 or v2/v2 physical-result schema;
-- the v1 pair is numeric-only.  Replacing the function updates all three existing
-- deferred constraint triggers without weakening their final-transaction-state
-- check.
CREATE OR REPLACE FUNCTION validate_agent_tool_terminal_aggregate()
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
          AND (
              (
                  attempt.schema_version = 'agent_tool_result_attempt_v1'
                  AND journal.schema_version = 'agent_tool_result_journal_v1'
                  AND slot.owner_target_revision_token = ''
              )
              OR (
                  attempt.schema_version = 'agent_tool_result_attempt_v2'
                  AND journal.schema_version = 'agent_tool_result_journal_v2'
              )
          )
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
          AND attempt.owner_target_revision_token = slot.owner_target_revision_token
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
          AND journal.owner_target_revision_token = slot.owner_target_revision_token
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

SET LOCAL lock_timeout = DEFAULT;
SET LOCAL statement_timeout = DEFAULT;
