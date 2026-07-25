# Track D D3c2h1 fixed-forward — pinned local advisory

## Classification

- Result: **scope-local advisory `GO 0/0/0/0`**.
- This is a fresh non-author Codex Desktop sub-agent review of exact pinned Git objects.
- It is **not** a canonical runner artifact and **not** a formal highest-effort `GO`.
- It does not authorize a migration, repository/runtime activation, provider call, live validation, or product signoff.

## Pinned scope

- Commit: `fdb3b792c14fa02e986f99decd3bf510173ea6cf`
- Parent: `19a685b2f904ff555211574696bb3cb7e26e00b2`
- Files:
  - `sourcing-ai-agent/docs/NEXT_TODO.md`
  - `sourcing-ai-agent/docs/RESIDUAL_LEDGER.md`
  - `sourcing-ai-agent/docs/TRACK_D_AGENT_RUNTIME_PLAN.md`
  - `sourcing-ai-agent/docs/TRACK_D_D3C2H1_EXACT_EVIDENCE_SURFACE_DECISION_LOCK.md`
  - `sourcing-ai-agent/tests/test_d3c2h1_exact_evidence_surface_decision_lock.py`

## Evidence

- Migration `0003_workflow_command_claim_fence_foundation.sql` already installs
  `workflow_commands.workspace_id text DEFAULT ''::text NOT NULL` and the named
  `workflow_commands_workspace_id_shape_ck ... NOT VALID` check.
- H1 now requires a future combined migration to adopt and validate those installed objects, preserve brownfield
  empty-string sentinels, and never add, drop, rewrite, or reinterpret them.
- The exact oracle reads migration `0003`, verifies the installed column/default/nullability and complete named check,
  and rejects the stale "must first add" requirement.
- The H1 boundary remains seven relations, 52 constraints, 29 foreign keys, and 11 indexes. It does not claim that
  D3c2i's combined `10/77/45/15` boundary is implemented or reviewed.
- `git diff --check fdb3b79^ fdb3b79` was clean. The reviewer inspected author counts but did not rerun working-tree
  tests, so they remain author evidence.

## Findings

- P0: none.
- P1: none.
- P2: none. `[re-raise closed]` The prior stale workspace-column installation requirement is removed and guarded.
- P3: none.
- `[new]` No new findings.
- `[residual]` D3c2i still needs its own matching pinned review before the combined boundary or dormant migration can
  be authorized.
- `[residual]` A formal highest-effort review remains pending.

GO
