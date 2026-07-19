# Track D D1n S1f0c — fixed-forward lineage decision (FF-DI canonization)

> Status: decision-locked author candidate. This batch canonizes the ignored FF-DI integration handoff into a tracked
> machine manifest and executable decision tests. It implements no runtime writer, relation, migration, reader, result
> acceptance, Agent population, provider/model call, or served state. It is author evidence until a fresh pinned
> non-author review accepts the scope; a scope-matched decision `GO` is required before any canonical S1f0c
> implementation packet (FF-SCHEMA, FF-CARRIER, FF-COMPILER, FF-SOURCE) or migration `0015` authoring may start.

## 1. Decision

The fixed-forward integration decision in `.coord/handoffs/s1f0c-ff-di-v1.md` (969 lines, pinned at
`3bd3ed2fd803e428846961982d76f74c0fb4437e`) is adopted as the single design source for canonical S1f0c. This batch
makes that decision tracked and machine-checkable without re-deriving it. The FF-DI handoff, the five Wave-0 handoffs,
the FF-DI lane decision handoff, and the controlling prior review artifact are content-hash-bound inside the
manifest's `obligation_basis.design_evidence_closure` (exact SHA-256 per source file); the source files themselves
remain untracked and are never committed, so every adoption claim is independently checkable by recomputation.

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
- Collision method: the executable test derives the scan from `git ls-tree`/`git grep` at the pinned `HEAD` commit
  over tracked content only, excluding exactly the three candidate blobs by exact path; all 23 new literals and 6
  relation names must have zero matches and the 7 retained literals must remain byte-referenced. The author-side
  pre-check used exact `git grep -F` against the pinned base commit `c3efff2aa07330dee52408ebc9dcc7f1786564f5` with
  zero matches; the test re-proves the property against tracked Git content instead of the ambient filesystem.

## 3. What the machine manifest pins

The manifest is the single machine-readable pin that later implementation packets consume instead of retyping the
handoff tables:

- handoff section 1.1: all 21 schema/registry literals with contract owner, physical writer/storage owner, and the
  collision/append-only rule, plus the contract digest equation `SHA256(UTF8(canonical_json(schema)))` where `schema`
  is each row's full closed schema object (exact field descriptors binding types, constants, enums, optionality,
  bounds, item bounds, closed nested structure, exact-version-and-digest references, provenance/value-role pins, and
  derivation rules; adding an enum value or deleting a derivation rule changes the digest), the canonical-JSON rules
  (sorted keys, compact separators, Unicode preserved, `allow_nan=false`, duplicate keys rejected, recursive
  type-strict equality with descriptor constants type-checked against their declared types and every numeric
  descriptor field rejecting boolean/float aliases), and the 22 materialized contract digests — every adopted literal
  plus `filter_projection_product_ref.v1`, with no registry literal left undigested — plus the two
  `retained_contract_pins` digests for the retained root payload and owner-ref schemas;
- the `digest_dependency_dag`: every digest construction edge as machine data. The candidate-set digest depends only
  on source/member inputs and the execution result then binds the completed candidate-set digest; the product terminal
  has an acyclic `terminal_core_digest` (fields 1-23), the freshness/readiness refs derive from that core, and the
  envelope `terminal_digest` is derived last; `commit_contract_digest` is a constant self-binding, not a construction
  edge. A topological test rejects any cycle, including hostile reintroduction of either reviewed back-edge;
- handoff section 1.2: the six new PG relation names, their three owners, and the reserved migration slot
  `0015_s1f0c_filter_projection_lineage.sql` (`reserved_not_authored`);
- handoff section 3: the closed six-field start carrier, the eleven-field execution authority with its normative
  plan-body digest exclusion, the six-stage writer taxonomy, and the eight-row source join as structured machine data
  (all eight rows, the five command identities with expected command types/stages/owners, structured alternate keys,
  and 100 executed join predicates in nine closed kinds — row/path field equality, constants, idempotency formulas,
  schema validation against the retained pins and adopted contracts, digest recomputation, carrier consensus, and
  deterministic command-id recompute — covering approval state, the requested `acquisition_run_id`, owner-ref/root
  rebuilding, physical causality, source events, carrier equality, plan/review identity, and AcquisitionRun bundle
  equality; the exact digest-pinned external `acquisition_plan_preview_record_v2` validator executes against every
  root snapshot preview, and every workspace/requester/provider-mode/runtime-namespace comparison in
  `requester_bindings` — base-row scope, nested preview scope, owner-ref scope, and every carrier copy — is bound
  to executed predicate ids; the test evaluator executes every encoded predicate with a witness proof and one
  hostile mutation per predicate); every miss/duplicate/foreign/malformed/split identity maps publicly to
  `projection_not_found`;
- the `retained_contract_pins` section: the retained nine-field `acquisition_root_command_payload.v2` root and the
  18-field `acquisition_start_command_acceptance_owner_result_ref.v1` owner ref materialized as fully closed schemas
  with recomputed pin digests; the carrier binds them as immutable exact-version-and-digest references, every adopted
  contract ref carries the exact target `contract_digest` as a `ref_digest` constant, and the external
  `acquisition_plan_preview_record_v2` preview spec is pinned by its exact live schema digest;
- handoff section 4: the opaque namespace ref (public subset `schema_version, namespace_ref_id, ref_digest`), the
  19-field capability v2 (`simulate|scripted` only at this boundary), the 12-field execution envelope with its fixed
  recompile sequence, and the lane-result/member/candidate-set/result-v2/execution-commit records; the commit record
  persists `schema_version` and `commit_contract_digest`, both included in `commit_digest`, so no future schema can
  produce indistinguishable commit bytes;
- handoff section 5: full column and constraint ownership for all six physical relations plus a typed PG descriptor
  per relation (column types, nullability/defaults, primary/unique/check/foreign keys with targets, actions, and
  deferrability, and index definitions including partial unique indexes), the reserved creation/rollback order, and
  63-byte identifier validation; every relation also carries `invariant_enforcement`: an enforcement owner for every
  declared invariant, with prose constraints mapped one-to-one, canonical row-local invariants encoded as exact SQL
  checks (exact contract-digest constants for `commit_contract_digest`/`ref_contract_digest`, exact bounds such as
  commit `candidate_count` `0..1000` and terminal counts `1..1000`, nonempty identity and state-dependent attempt
  checks), tenant/mode and digest equality across references encoded as composite scoped foreign keys, predecessor
  id/digest parity as an exact check plus composite self-FK, and transition/UoW-only invariants explicitly marked
  repository-enforced; `membership_revision` is a non-empty opaque equality token (PG `text`, never
  `bigint`/positive/numeric) per `docs/CANONICAL_SERVING_PROJECTION_CONTRACT.md` — equality/inequality only, never
  numerically, lexically, or chronologically ordered; lane provider evidence
  (`provider_exposure_id`/`provider_call_id`/`provider_response_digest`) equals the owning execution attempt's exact
  tuple through an explicit repository `uow_rule` compared before any lane write; product-terminal append-only
  behavior is split — SQL keeps `terminal_generation > 0` while insert-once/no-UPDATE/no-DELETE is owned by an
  explicit repository `transition_guard`; the decision test proves one-to-one coverage between canonical schemas,
  prose constraints, and DDL descriptors;
- handoff section 6: the 27-field product terminal with the acyclic `terminal_core_digest`, the freshness/readiness
  refs carrying the core digest, the exact eight-item readiness prerequisite set bound inside the readiness schema
  derivation, the five-step deferred reason precedence, and the core/envelope terminal-digest equalities;
- handoff section 7: the V3 success/deferred/masked public roots bound by the `filter_projection_result_v3` schema
  with exact union discrimination (the success discriminator and success/deferred `status` are exact constants/enums,
  never open unions), closed lane-summary, candidate, requested-target-ref, and requested-lane-coverage item schemas
  with exact v2 enums, display/URL policies, and paging/item bounds, per-descriptor provenance and value-role pins,
  and the masked constants; V3 `cohort_selection` retains the exact closed Cohort selection object
  (`schema_version`/`role_bucket_ids`/`employment_statuses`/`role_match`/`source` per
  `agent_projection_query._cohort_selection_schema`) bound to `cohort_selection_digest`, never an unbound string;
  the serializer contract decision-locks the complete `ActionResultSpec.to_fingerprint_record()` equivalent
  (validator/interpretation contract with the exact interpretation contract digest, closed serializer semantics,
  the exact five externally controlled identifier paths, empty artifact-ref schemes, and the exact
  `max_serialized_bytes=65536` (64 KiB)/`max_items=8192`/`max_depth=10` limits); the tool contract decision-locks
  the complete `AgentToolSpec.to_fingerprint_record()` equivalent (tool name/kind, request pin, route
  binder/adapter, simulate fixture, release owner, execution subject, budget/capability, approval, command
  exposure, and control policy, with every constant owner-pin digest recomputed from its pinned contract bytes)
  and binds the exact result and serializer contract digests as constants;
  the commandless full terminal tuple (`no_command_v1`, both-empty Action/Operation, zero attempt/generation/epoch),
  the deterministic `filter_projection_terminal_winner.v1` equation, the four closed internal owner refs with exact
  field manifests, and the replay/quarantine retention rules;
- handoff section 8: the 13-edge end-to-end owner graph using only canonical owner IDs from the closed `owners`
  registry, with descriptive labels kept in the separate `owner_label` field;
- handoff section 9: the seven-group global lock order and the 18 locked write/race outcomes;
- handoff section 10: lane/count, member-set, digest, cross-surface, and V3 page equations, with
  `missing_required_lane_count` pinned as role-intersection identity loss, never lane coverage;
- handoff section 11: retained v1/v2 literals as exact history, the three rejected literals, and the eight
  retained/replay/migration/deletion gates;
- handoff section 12: the 16 hostile-mutation oracle families plus concurrency coverage;
- handoff section 13: the 8-mechanism x 10-invariant matrix with no deferred cells;
- handoff section 14: the FF-SCHEMA/FF-CARRIER/FF-COMPILER/FF-SOURCE/FF-ACQ-INTEGRATE/FF-PG-SCHEMA/FF-PG-UOW/
  FF-RESULT/FF-XO implementation DAG with exclusive write paths and review edges, exact integer waves in the closed
  set {1, 2, 3} with packet uniqueness, dependency/wave-order validation, and every manifest-level numeric field
  covered by the hostile bool/float alias sweep;
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
scope-local Cohort attempt fence never claims global `R-019` closure. The central owner matrix in
`docs/PRE_AGENT_CONTRACT_REVIEW.md` records the same `decision_locked_not_implemented` owner state for the product,
terminal, and freshness/readiness owners in this reviewed scope, preserving every `served=0` and no-live boundary.
Author evidence is not a formal review: a fresh scope-matched non-author decision review is required before canonical
S1f0c implementation or migration `0015` authoring, and unrelated S1e2b/fake work may continue while that review
waits.
