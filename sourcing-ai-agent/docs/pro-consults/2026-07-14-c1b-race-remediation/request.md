# C1b race-remediation consultation request

> Status: Reference (consult transcript, 2026-07-14 era). Decisions were promoted into their owning docs; do not treat dated numbers here as current.

Purpose: approach_review
Secondary question sets: architecture
Authority: ADVISORY_ONLY
Surface required: Chat
Model required: GPT-5.6 Sol
Mode required: Pro
Browser required: Codex in-app browser
Local state: dirty
Connector sees dirty scope: false
Dirty scope provided to Pro: false
Branch: governance-phase0-ttl-20260611
Branch requirement: provenance_only
Repository: sorachang1874/Sourcing-AI-Agent-Dev
Commit authority: 9c3987a904bc8e38f523388c526b0e1d0a61d735
Consultation status: planned
Connector status: attached_pending
Redaction status: verified

ADVISORY_ONLY — not an independent-review artifact or formal GO.

CONNECTOR_SCOPE_JSON: {"commit_sha":"9c3987a904bc8e38f523388c526b0e1d0a61d735","mode":"files","repository":"sorachang1874/Sourcing-AI-Agent-Dev","required_files":["/AGENTS.md","/sourcing-ai-agent/AGENTS.md","/sourcing-ai-agent/docs/TRACK_C_C1_DURABLE_PLAN_TASK_DESIGN.md","/sourcing-ai-agent/docs/TRACK_C_C1_HEAVY_OPS_DESIGN.md","/sourcing-ai-agent/frontend-demo/src/lib/historyRecovery.ts","/sourcing-ai-agent/src/sourcing_agent/api.py","/sourcing-ai-agent/src/sourcing_agent/async_task_contract.py","/sourcing-ai-agent/src/sourcing_agent/orchestrator.py","/sourcing-ai-agent/src/sourcing_agent/plan_submit_contract.py","/sourcing-ai-agent/tests/test_frontend_history_recovery.py","/sourcing-ai-agent/tests/test_plan_submit_contract.py"]}

## Objective

Advise on the smallest correct forward remediation for the confirmed C1b process-local legacy hydration races and
identity gaps, while preserving the explicit owner gate that forbids C1c-e durable schema/repository/202 cutover work.
The consultation must distinguish bounded C1b fixes from issues that must remain explicit D-C1-3/C1d blockers.

## Connector handshake and evidence contract

Use the attached GitHub app read-only. Retrieve `sorachang1874/Sourcing-AI-Agent-Dev` at exact full commit
`9c3987a904bc8e38f523388c526b0e1d0a61d735` and read every path in the manifest. The immutable commit is content
authority. The branch is provenance-only; preserve the raw lookup outcome but do not block when the repository,
commit, and every requested file are proven.

For every requested file, include its exact immutable GitHub blob URL as unfenced text. If the commit or any file is
unavailable, return `CONNECTOR_BLOCKED` without a verdict and stop. Do not answer from memory, web search, or local
state. Do not modify the repository, create issues/PRs, execute code, contact people, or use credentials.

## Confirmed non-author findings at the pinned commit

These are observations to reason about, not claims that current dirty work has fixed them:

1. A same-signature consumer joining between an owner's final consumer snapshot and cleanup can remain in both
   process-local maps with no owner thread.
2. Same-history supersession is checked only after side-effecting `plan_workflow`; the stale compile can create/reuse a
   review, publish history, and persist criteria before the generation fence. The current local direction may remove
   `history_id` from the compiler payload and generation-fence the final history write, but review/criteria side effects
   still require compute/publish separation or an equivalent publication UoW.
3. The source AST ratchet misses callable aliases plus direct thread/executor targets.
4. Authenticated clients can select and read/replace legacy unlinked Plan histories unless ownership provenance is
   made explicit and fail-closed.
5. Shared async artifacts with empty handles and stale HTTP `202` comments are smaller P2 cleanup items.

## Hard boundaries

- C1b keeps HTTP `200` plus top-level `pending`; it does not switch server behavior to `202`.
- No schema, migration, provider/model call, durable repository, TTL/replay, live validation, or external write.
- Only one legacy hydration-thread owner remains until the C1e cutover.
- Reviews are scoped: a pending or negative verdict freezes this scope's promotion/live/W6/manual/product signoff, not
  unrelated implementation.
- Current uncommitted fixes are deliberately invisible to the Connector and must not be reviewed or claimed complete.

## Questions

1. What lock-protected owner lifecycle/state machine prevents late-consumer stranding without starting parallel
   compilers for the same signature?
2. Which generation checks and payload changes are safe in C1b, and which stale side effects cannot be solved without
   the owner-gated compute/publish separation or publication UoW?
3. What deterministic barrier tests distinguish true closure from a test that merely replaces the worker method?
4. What is the minimum fail-closed identity/provenance rule for legacy unlinked Plan histories without a schema change?
5. How should the AST ratchet model aliases, `Thread(target=...)`, executor submits, and indirect callable invocation?
6. Give a commit sequence, rollback boundary, acceptance gates, and one-wave lookahead that keeps C1c-e blocked.

## Required response

Return exactly one Markdown artifact between these markers:

BEGIN_ARTIFACT path=sourcing-ai-agent/docs/pro-consults/2026-07-14-c1b-race-remediation/response.md
END_ARTIFACT

The exact advisory label must be the line immediately after `BEGIN_ARTIFACT`. Use this ordered shape outside fences:

# Verdict
Raw Pro verdict: keep|adjust|pivot

# Scope understood

State repository, exact observed commit, raw branch lookup outcome, and all eleven immutable file citations.

# Assumptions and missing information

# Findings
## P0
## P1
## P2

# Recommended sequence

# Validation and failure modes

# Deferred decisions

# Owner decisions required

Do not add another verdict directive, modifier, formal-GO claim, or text outside the marker envelope.
