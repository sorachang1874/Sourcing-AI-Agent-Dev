-- WS7/W7.2 S4 (docs/WS7_AI_BATCH_DIVIDER_DESIGN.md §4.3, OQ6 RATIFIED 2026-07-23):
-- the durable refill wave identity gains the AI-batch division id.
--
-- R6 durable-wave inheritance today claims a recomputed window from three SCALAR
-- registry fields (refill_plan_batch_size/batch_count/window_url_count).  A
-- heterogeneous AI division (per-batch sizes differ) cannot be represented by
-- that scalar identity, so the wave identity is extended with the division id
-- minted at the plan-record moment.  Additive + nullable: absent/empty means
-- "no division" — every pre-S4 row and every ladder-produced wave until the S5
-- flip behaves exactly as before.  Written/cleared in lockstep with the sibling
-- refill_plan_* scalars (repositories/linkedin_profile_registry.py).

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE linkedin_profile_registry
    ADD COLUMN refill_plan_division_id text;
