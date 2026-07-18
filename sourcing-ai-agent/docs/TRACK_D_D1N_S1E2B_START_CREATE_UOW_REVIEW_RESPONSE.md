# Track D D1n S1e2b — formal review response

Status: implementation response to the formal S1e2b `NO-GO` artifact
`runtime/reviews/20260718T060805Z_Track_D_D1n_S1e2b_start_create_UoW_fde4b34.md`.

This batch remains non-live and non-served. It does not close `R-019`, `R-029`, Plan §6#6, `OB-2.2`, `OB-10.3`,
`OB-10.4`, hosted serving, or provider/model invocation gates.

## Fixed-forward scope

- Generic workflow-command controls now reject held `start_acquisition_run` v2 root commands before mutation. The hold is
  still the `not_before_at=9999-12-31 23:59:59` release gate; only result acceptance may clear it.
- Native PG command mutators (`cancel_workflow_command`, `retry_workflow_command`, `resume_workflow_command`) now apply
  the same held-root fence before clearing `not_before_at`, so direct store callers cannot bypass the API preflight.
- Generic operation controls now distinguish non-v2, exact-v2, and mixed/partial v2 identity. Mixed action/operation
  pins fail closed with zero writes instead of falling back to generic mutation.
- Generic action controls now also fail closed for pending `start_acquisition_run` Actions that carry only partial v2
  discriminators: schema version/digest without the exact input shape, one/two preview keys without the full
  `preview_id + preview_revision + preview_digest` tuple, or one exact schema pin paired with a mismatching peer. These
  rows are treated as corrupt start-v2 candidates, not legacy-ready Actions.
- The create UoW uses raw, non-normalizing row reads for authority probes and exact replay comparison. Public PG row
  normalization is no longer used to certify physical owner identity.
- JSON replay comparison rejects blank/malformed carriers, duplicate object keys, and type aliases such as JSON boolean
  versus integer.
- Approval actor and policy revision validation now reuses the canonical pure start-v2 owner/version validators before
  adapter dependency checks or PostgreSQL connection/locking.
- S1e2b documentation now states that create performs no wake; S1e2c owns hold release and post-accept wake.

## New regression evidence

- `tests/test_d1n_start_acquisition_v2_create_uow.py` covers overlong, control-character, surrogate, malformed, and
  overlong policy values before adapter access.
- `tests/test_d1n_start_acquisition_v2_create_pg.py` covers:
  - held command ready-list absence and direct claim zero-write;
  - cancel/retry/resume command-control zero-write;
  - direct PG cancel/retry/resume mutator zero-write for held queued/cancelled/retry-wait root commands;
  - action-type drift, empty schema pair, and alternate schema pair operation-control zero-write;
  - pending partial-v2 approve/reject zero-write for schema-only, preview-key-only, and single-pin mismatch cases;
  - raw text owner corruption replay rejection;
  - blank JSON carrier replay rejection;
  - JSON bool/int alias replay rejection.

## Explicit non-closure

S1e2c has its own formal `NO-GO` and must be fixed separately. S1e2d addresses the first released-root consumer hop, but
that does not by itself close S1e2c result acceptance findings about forged terminal bytes, post-progress replay,
physical owner reconstruction, lock topology, deadline budgeting, or central contract/preflight registration.
