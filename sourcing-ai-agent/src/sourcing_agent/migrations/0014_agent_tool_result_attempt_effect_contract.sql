-- Track D D1n S1d fixed-forward: bind every result attempt to the owning
-- slot's effect class as well as its link policy.
--
-- Migration 0013 made the link policy explicit, but its deferred attempt
-- trigger accepted either no-command shape without consulting whether the
-- owning slot was read-only or commandless.  In particular, a quarantined
-- commandless attempt could omit its Action/Operation links.  This migration
-- rejects any such brownfield row before replacing the trigger function.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- Slot effect_class/result_link_policy are immutable.  Blocking attempt
-- writers is therefore sufficient to make the historical scan and function
-- replacement one atomic cutover without taking a reader-blocking table lock.
LOCK TABLE agent_tool_result_attempts IN SHARE ROW EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM agent_tool_result_attempts AS attempt
        JOIN agent_tool_result_slots AS slot
          ON slot.result_slot_id = attempt.result_slot_id
        WHERE attempt.result_link_policy IS DISTINCT FROM slot.result_link_policy
           OR NOT (
                (
                    slot.effect_class = 'commandless_action'
                    AND slot.result_link_policy = 'no_command_v1'
                    AND attempt.action_id <> ''
                    AND attempt.operation_run_id <> ''
                    AND attempt.workflow_command_id = ''
                    AND attempt.activity_run_id = ''
                    AND attempt.activity_attempt_id = ''
                    AND attempt.command_attempt = 0
                    AND attempt.command_generation = 0
                    AND attempt.control_epoch = 0
                )
                OR (
                    slot.effect_class = 'read_only'
                    AND slot.result_link_policy = 'no_command_v1'
                    AND (
                        (attempt.action_id = '' AND attempt.operation_run_id = '')
                        OR (attempt.action_id <> '' AND attempt.operation_run_id <> '')
                    )
                    AND attempt.workflow_command_id = ''
                    AND attempt.activity_run_id = ''
                    AND attempt.activity_attempt_id = ''
                    AND attempt.command_attempt = 0
                    AND attempt.command_generation = 0
                    AND attempt.control_epoch = 0
                )
                OR (
                    slot.effect_class = 'command_backed_action'
                    AND slot.result_link_policy = 'workflow_command_acceptance_v1'
                    AND attempt.action_id <> ''
                    AND attempt.operation_run_id <> ''
                    AND attempt.workflow_command_id <> ''
                    AND attempt.activity_run_id = ''
                    AND attempt.activity_attempt_id = ''
                    AND attempt.command_attempt = 0
                    AND attempt.command_generation = 0
                    AND attempt.control_epoch = 0
                )
                OR (
                    slot.effect_class = 'command_backed_action'
                    AND slot.result_link_policy = 'activity_attempt_terminal_v1'
                    AND attempt.action_id <> ''
                    AND attempt.operation_run_id <> ''
                    AND attempt.workflow_command_id <> ''
                    AND attempt.activity_run_id <> ''
                    AND attempt.activity_attempt_id <> ''
                    AND attempt.command_attempt > 0
                    AND attempt.command_generation > 0
                    AND attempt.control_epoch > 0
                )
           )
        LIMIT 1
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            CONSTRAINT = 'agent_tool_result_attempt_slot_effect_shape_mismatch',
            MESSAGE = 'every result attempt must match its owning slot effect class and link policy';
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION validate_agent_tool_result_attempt_slot_policy()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM agent_tool_result_slots AS slot
        WHERE slot.result_slot_id = NEW.result_slot_id
          AND slot.result_link_policy = NEW.result_link_policy
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            CONSTRAINT = 'agent_tool_result_attempt_slot_policy_mismatch',
            MESSAGE = 'every result attempt must exact-copy its owning slot link policy';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM agent_tool_result_slots AS slot
        WHERE slot.result_slot_id = NEW.result_slot_id
          AND (
                (
                    slot.effect_class = 'commandless_action'
                    AND slot.result_link_policy = 'no_command_v1'
                    AND NEW.action_id <> ''
                    AND NEW.operation_run_id <> ''
                    AND NEW.workflow_command_id = ''
                    AND NEW.activity_run_id = ''
                    AND NEW.activity_attempt_id = ''
                    AND NEW.command_attempt = 0
                    AND NEW.command_generation = 0
                    AND NEW.control_epoch = 0
                )
                OR (
                    slot.effect_class = 'read_only'
                    AND slot.result_link_policy = 'no_command_v1'
                    AND (
                        (NEW.action_id = '' AND NEW.operation_run_id = '')
                        OR (NEW.action_id <> '' AND NEW.operation_run_id <> '')
                    )
                    AND NEW.workflow_command_id = ''
                    AND NEW.activity_run_id = ''
                    AND NEW.activity_attempt_id = ''
                    AND NEW.command_attempt = 0
                    AND NEW.command_generation = 0
                    AND NEW.control_epoch = 0
                )
                OR (
                    slot.effect_class = 'command_backed_action'
                    AND slot.result_link_policy = 'workflow_command_acceptance_v1'
                    AND NEW.action_id <> ''
                    AND NEW.operation_run_id <> ''
                    AND NEW.workflow_command_id <> ''
                    AND NEW.activity_run_id = ''
                    AND NEW.activity_attempt_id = ''
                    AND NEW.command_attempt = 0
                    AND NEW.command_generation = 0
                    AND NEW.control_epoch = 0
                )
                OR (
                    slot.effect_class = 'command_backed_action'
                    AND slot.result_link_policy = 'activity_attempt_terminal_v1'
                    AND NEW.action_id <> ''
                    AND NEW.operation_run_id <> ''
                    AND NEW.workflow_command_id <> ''
                    AND NEW.activity_run_id <> ''
                    AND NEW.activity_attempt_id <> ''
                    AND NEW.command_attempt > 0
                    AND NEW.command_generation > 0
                    AND NEW.control_epoch > 0
                )
          )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            CONSTRAINT = 'agent_tool_result_attempt_slot_effect_shape_mismatch',
            MESSAGE = 'every result attempt must match its owning slot effect class and link policy';
    END IF;
    RETURN NEW;
END;
$$;

SET LOCAL lock_timeout = DEFAULT;
SET LOCAL statement_timeout = DEFAULT;
