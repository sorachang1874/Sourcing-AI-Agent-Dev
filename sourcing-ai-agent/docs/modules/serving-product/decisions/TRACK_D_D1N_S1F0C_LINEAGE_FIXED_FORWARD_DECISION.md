# Track D D1n S1f0c — fixed-forward lineage decision (FF-DI canonization)

> Status: decision-locked author candidate. This batch canonizes the ignored FF-DI integration handoff into a tracked
> machine manifest and executable decision tests. It implements no runtime writer, relation, migration, reader, result
> acceptance, Agent population, provider/model call, or served state. It is author evidence until a fresh pinned
> non-author review accepts the scope; a scope-matched decision `GO` is required before any canonical S1f0c
> implementation packet (FF-SCHEMA, FF-CARRIER, FF-COMPILER, FF-SOURCE) or migration `0015` authoring may start.

## 1. Decision

The fixed-forward integration decision in `.coord/handoffs/s1f0c-ff-di-v1.md` (969 lines, pinned at
`3bd3ed2fd803e428846961982d76f74c0fb4437e`) is adopted as the single design source for canonical S1f0c. This batch
makes that decision tracked and machine-checkable without re-deriving it.

The decision rejects the three reviewed-candidate literals `acquisition_start_lineage_ref.v1`,
`cohort_provider_execution_manifest.v2`, and `filter_projection_publication_terminal.v1` as implementation literals and
adopts 21 new append-only literals instead, including `acquisition_start_authority_carrier.v1`,
`acquisition_execution_authority.v1`, `agent_runtime_namespace_ref.v1`, `cohort_execution_capability.v2`,
`cohort_execution_envelope.v1`, `cohort_execution_result.v2`, `cohort_execution_commit.v1`,
`filter_projection_product_terminal.v1`, `filter_projection_result_v3`, `filter_projection_result_serializer_v3`,
`filter_projection_tool_v3`, and the provider execution command `acquisition.cohort.execute`.

Exactly six new PG relation names are reserved under three owners (namespace registry, Cohort execution aggregate,
serving projection aggregate): `agent_runtime_namespace_refs`, `cohort_execution_attempts`,
`cohort_execution_lane_results`, `cohort_candidate_set_members`, `cohort_execution_commits`, and
`filter_projection_product_terminals`. No SQLite DDL/fallback/mirror is permitted. The next migration slot is reserved
as `src/sourcing_agent/migrations/0015_s1f0c_filter_projection_lineage.sql`; this batch authors no DDL.

The closed machine decision is
[filter_projection_lineage_fixed_forward_decision_v1.json](../contracts/filter_projection_lineage_fixed_forward_decision_v1.json).
Unknown keys, inferred aliases, caller-supplied refs/digests/winners, and partial lane or member output fail closed.

## 2. Source, method, and precedent

- Design source: `.coord/handoffs/s1f0c-ff-di-v1.md` sections 0-16, consumed in full.
- Formal controlling artifact: `runtime/reviews/20260718T113429Z_Track_D_D1n_S1f0c0_lineage_terminal_owner_decision.md`
  (verdict `NO-GO`, new `P0/P1/P2/P3=0/9/1/0`, residuals `R-019` and `R-029`); the handoff's F1-F11 reconciliation
  (its section 2) closes those findings and the Wave-0 unresolved cells (its section 2.1).
- Wave-0 inputs consumed by the handoff: `s1f0c-ff-jc-v1.md`, `s1f0c-ff-cr-v1.md`, `s1f0c-ff-cs-v1.md`,
  `s1f0c-ff-rs-v1.md`, `s1f0c-ff-fr-v1.md`.
- Canonization precedent: commit `3bd3ed2` and the S1f0c0 artifacts
  [TRACK_D_D1N_S1F0C_LINEAGE_TERMINAL_OWNER_DECISION.md](TRACK_D_D1N_S1F0C_LINEAGE_TERMINAL_OWNER_DECISION.md),
  `filter_projection_lineage_terminal_owner_decision_v1.json`, and
  `tests/test_d1n_s1f0c_lineage_terminal_owner_decision.py`. This batch follows that structure exactly: one machine
  manifest, one decision document, one executable decision test, plus the two index/router lines.
- Collision method: every new literal and relation name was checked with exact `git grep -F` against the pinned base
  commit `c3efff2aa07330dee52408ebc9dcc7f1786564f5`; all 23 new literals and 6 relation names had zero matches. The
  executable test re-proves this against the live tree instead of hardcoding the result.

## 3. What the machine manifest pins

The manifest is the single machine-readable pin that later implementation packets consume instead of retyping the
handoff tables:

- handoff section 1.1: all 21 schema/registry literals with contract owner, physical writer/storage owner, and the
  collision/append-only rule, plus the contract digest equation
  `SHA256(UTF8(canonical_json({schema_version, owner, ordered_fields})))` with canonical-JSON rules (sorted keys,
  compact separators, Unicode preserved, `allow_nan=false`, duplicate keys rejected, recursive type-strict equality)
  and the 18 materialized contract digests;
- handoff section 1.2: the six new PG relation names, their three owners, and the reserved migration slot
  `0015_s1f0c_filter_projection_lineage.sql` (`reserved_not_authored`);
- handoff section 3: the closed six-field start carrier, the eleven-field execution authority with its normative
  plan-body digest exclusion, the six-stage writer taxonomy, and the eight-row source join whose every
  miss/duplicate/foreign/malformed/split identity maps publicly to `projection_not_found`;
- handoff section 4: the opaque namespace ref (public subset `schema_version, namespace_ref_id, ref_digest`), the
  19-field capability v2 (`simulate|scripted` only at this boundary), the 12-field execution envelope with its fixed
  recompile sequence, and the lane-result/member/candidate-set/result-v2/execution-commit records;
- handoff section 5: full column and constraint ownership for all six physical relations;
- handoff section 6: the 26-field product terminal, the freshness/readiness refs, the exact eight-item readiness
  prerequisite set, the five-step deferred reason precedence, and the five-way terminal-digest equality;
- handoff section 7: the V3 success/deferred/masked public roots, the commandless full terminal tuple
  (`no_command_v1`, both-empty Action/Operation, zero attempt/generation/epoch), the deterministic
  `filter_projection_terminal_winner.v1` equation, the four closed internal owner refs, and the replay/quarantine
  retention rules;
- handoff section 8: the 13-edge end-to-end owner graph;
- handoff section 9: the seven-group global lock order and the 18 locked write/race outcomes;
- handoff section 10: lane/count, member-set, digest, cross-surface, and V3 page equations, with
  `missing_required_lane_count` pinned as role-intersection identity loss, never lane coverage;
- handoff section 11: retained v1/v2 literals as exact history, the three rejected literals, and the eight
  retained/replay/migration/deletion gates;
- handoff section 12: the 16 hostile-mutation oracle families plus concurrency coverage;
- handoff section 13: the 8-mechanism x 10-invariant matrix with no deferred cells;
- handoff section 14: the FF-SCHEMA/FF-CARRIER/FF-COMPILER/FF-SOURCE/FF-ACQ-INTEGRATE/FF-PG-SCHEMA/FF-PG-UOW/
  FF-RESULT/FF-XO implementation DAG with exclusive write paths and review edges;
- handoff section 15: the seven transition states and their first permitted change conditions.

## 4. Retained history and rejected literals

Retained exactly as history, with exact version + contract/result digest lookup and no shape inference,
lexicographic-latest alias, mutable current alias, or auto-upgrade: `acquisition_root_command_payload.v2`,
`cohort_execution_capability.v1`, `cohort_provider_manifest.v1`, planning `cohort_provider_manifest.v2`,
`cohort_execution_result.v1`, `filter_projection_result_v2`, and `filter_projection_tool_v2`. V3 registration creates
retained history only; the current tool alias remains V2 and the public/default served population remains zero.

Rejected as implementation literals (they remain referenced as reviewed history only):
`acquisition_start_lineage_ref.v1`, `cohort_provider_execution_manifest.v2`, and
`filter_projection_publication_terminal.v1`.

## 5. Release boundary and residuals

This decision creates no DDL, no migration, no runtime writer, no reader, no result acceptance, no backfill, and no
served transition. `served=0`, provider/model/live invocation counts are `0`, and `R-019`/`R-029` remain open; the
scope-local Cohort attempt fence never claims global `R-019` closure. Author evidence is not a formal review: a fresh
scope-matched non-author decision review is required before canonical S1f0c implementation or migration `0015`
authoring, and unrelated S1e2b/fake work may continue while that review waits.
