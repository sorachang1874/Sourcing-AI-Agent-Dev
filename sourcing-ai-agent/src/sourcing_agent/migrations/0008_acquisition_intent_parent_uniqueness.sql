-- Track D D1i fixed-forward: every acquisition root has exactly one intent child.
--
-- The generic workflow command model permits multiple children per parent, so a
-- global UNIQUE(parent_command_id) would be incorrect.  The trigger serializes
-- acquisition-root creation and every child identity change on the same parent
-- identity, then rejects a non-intent child whenever that identity belongs to an
-- acquisition root.  It also rejects a second child for that actual root,
-- regardless of command type, command id, idempotency key, owner, or workflow.
-- The same identity lock closes the orphan-child/root-create race.

SET LOCAL lock_timeout = '5s';

CREATE FUNCTION enforce_acquisition_root_child_shape()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
DECLARE
    lock_identity text;
    old_command_id text := '';
    old_command_type text := '';
    old_parent_command_id text := '';
    parent_command_type text;
    competing_child_id text;
    existing_child_count integer;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF NEW.command_id IS NOT DISTINCT FROM OLD.command_id
           AND NEW.command_type IS NOT DISTINCT FROM OLD.command_type
           AND NEW.parent_command_id IS NOT DISTINCT FROM OLD.parent_command_id THEN
            RETURN NEW;
        END IF;
        old_command_id := OLD.command_id;
        old_command_type := OLD.command_type;
        old_parent_command_id := OLD.parent_command_id;
    END IF;

    -- Child creation and acquisition-root creation use the same transaction
    -- lock even when neither row was visible when the competing transaction
    -- started.  Sorted acquisition avoids deadlocks for identity-moving UPDATEs.
    FOR lock_identity IN
        SELECT DISTINCT candidate
        FROM unnest(
            ARRAY[
                CASE
                    WHEN NEW.command_type = 'acquisition.run.create' THEN NEW.command_id
                    ELSE ''
                END,
                NEW.parent_command_id,
                CASE
                    WHEN old_command_type = 'acquisition.run.create' THEN old_command_id
                    ELSE ''
                END,
                old_parent_command_id
            ]
        ) AS candidate
        WHERE candidate <> ''
        ORDER BY candidate
    LOOP
        PERFORM pg_advisory_xact_lock(
            hashtext('acquisition-root-child-shape-v1:' || lock_identity)
        );
    END LOOP;

    IF NEW.parent_command_id <> '' THEN
        SELECT command_type
        INTO parent_command_type
        FROM workflow_commands
        WHERE command_id = NEW.parent_command_id;

        IF parent_command_type = 'acquisition.run.create' THEN
            IF NEW.command_type <> 'acquisition.intent.resolve' THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    CONSTRAINT = 'workflow_commands_acquisition_root_child_shape_ck',
                    MESSAGE = 'acquisition.run.create accepts only acquisition.intent.resolve children';
            END IF;

            SELECT command_id
            INTO competing_child_id
            FROM workflow_commands
            WHERE parent_command_id = NEW.parent_command_id
              AND command_id <> NEW.command_id
              AND (old_command_id = '' OR command_id <> old_command_id)
            LIMIT 1;

            IF competing_child_id IS NOT NULL THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23505',
                    CONSTRAINT = 'workflow_commands_acquisition_root_single_child_uk',
                    MESSAGE = 'acquisition.run.create accepts exactly one child';
            END IF;
        END IF;
    END IF;

    -- A child may have been inserted before its parent existed.  Root creation
    -- must therefore validate the reverse edge after acquiring the same identity
    -- lock used by child writers.
    IF NEW.command_type = 'acquisition.run.create' THEN
        IF EXISTS (
            SELECT 1
            FROM workflow_commands
            WHERE parent_command_id = NEW.command_id
              AND command_type <> 'acquisition.intent.resolve'
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                CONSTRAINT = 'workflow_commands_acquisition_root_child_shape_ck',
                MESSAGE = 'acquisition.run.create has a non-intent child';
        END IF;

        SELECT COUNT(*)
        INTO existing_child_count
        FROM workflow_commands
        WHERE parent_command_id = NEW.command_id;

        IF existing_child_count > 1 THEN
            RAISE EXCEPTION USING
                ERRCODE = '23505',
                CONSTRAINT = 'workflow_commands_acquisition_root_single_child_uk',
                MESSAGE = 'acquisition.run.create accepts exactly one child';
        END IF;
    END IF;

    RETURN NEW;
END;
$$;

CREATE INDEX workflow_commands_parent_command_idx
    ON workflow_commands (parent_command_id)
    WHERE parent_command_id <> '';

CREATE TRIGGER workflow_commands_acquisition_root_child_shape_trg
BEFORE INSERT OR UPDATE OF command_id, command_type, parent_command_id
ON workflow_commands
FOR EACH ROW
EXECUTE FUNCTION enforce_acquisition_root_child_shape();

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM workflow_commands AS child
        JOIN workflow_commands AS parent
          ON parent.command_id = child.parent_command_id
        WHERE parent.command_type = 'acquisition.run.create'
          AND child.command_type <> 'acquisition.intent.resolve'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'workflow_commands_acquisition_root_child_shape_ck',
            MESSAGE = 'brownfield acquisition.run.create has a non-intent child';
    END IF;
END;
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM workflow_commands AS child
        JOIN workflow_commands AS parent
          ON parent.command_id = child.parent_command_id
        WHERE parent.command_type = 'acquisition.run.create'
        GROUP BY parent.command_id
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23505',
            CONSTRAINT = 'workflow_commands_acquisition_root_single_child_uk',
            MESSAGE = 'brownfield acquisition.run.create has more than one child';
    END IF;
END;
$$;

SET LOCAL lock_timeout = DEFAULT;
