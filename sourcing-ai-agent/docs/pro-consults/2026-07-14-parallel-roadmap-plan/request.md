# Parallel roadmap consultation request

Purpose: architecture
Secondary question sets: none
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
Commit authority: 0cfaec6c48b95e57f55f1bce06fe5b3abae0e297
Consultation status: planned
Connector status: attached_pending
Redaction status: verified

ADVISORY_ONLY — not an independent-review artifact or formal GO.

CONNECTOR_SCOPE_JSON: {"commit_sha":"0cfaec6c48b95e57f55f1bce06fe5b3abae0e297","mode":"files","repository":"sorachang1874/Sourcing-AI-Agent-Dev","required_files":["/AGENTS.md","/sourcing-ai-agent/AGENTS.md","/sourcing-ai-agent/docs/NEXT_TODO.md","/sourcing-ai-agent/docs/TRACK_C_C1_DURABLE_PLAN_TASK_DESIGN.md","/sourcing-ai-agent/docs/TRACK_D_AGENT_RUNTIME_PLAN.md","/sourcing-ai-agent/docs/TRACK_D_D0_MODEL_TOOL_RUNTIME_DESIGN.md","/x-first-researcher-sourcing/AGENTS.md","/x-first-researcher-sourcing/docs/GROK_CAPABILITY_GATE.md","/x-first-researcher-sourcing/docs/X_FIRST_FIXTURE_CONTRACT.md","/x-first-researcher-sourcing/docs/advisories/2026-07-14-grok-stage1-capability-plan.md"]}

## Objective

Produce an execution-grade, parallel roadmap for three independent lanes: Track C C1b, Track D D0a, and the X-first
capability-contract line. Optimize for safe parallel throughput without allowing one scoped review to freeze unrelated
work.

## Connector handshake and evidence contract

Use the attached GitHub app read-only. Retrieve repository `sorachang1874/Sourcing-AI-Agent-Dev` at exact full commit
`0cfaec6c48b95e57f55f1bce06fe5b3abae0e297` and read every path in the manifest. The full commit is content authority.
The branch is provenance-only: preserve the actual branch lookup outcome, but do not block if a separate mutable branch
lookup is unavailable or returns no result after the repository, exact commit, and all files are proven.

For every requested file, include its exact immutable GitHub blob URL in the response. These ten URLs must appear
verbatim:

- https://github.com/sorachang1874/Sourcing-AI-Agent-Dev/blob/0cfaec6c48b95e57f55f1bce06fe5b3abae0e297/AGENTS.md
- https://github.com/sorachang1874/Sourcing-AI-Agent-Dev/blob/0cfaec6c48b95e57f55f1bce06fe5b3abae0e297/sourcing-ai-agent/AGENTS.md
- https://github.com/sorachang1874/Sourcing-AI-Agent-Dev/blob/0cfaec6c48b95e57f55f1bce06fe5b3abae0e297/sourcing-ai-agent/docs/NEXT_TODO.md
- https://github.com/sorachang1874/Sourcing-AI-Agent-Dev/blob/0cfaec6c48b95e57f55f1bce06fe5b3abae0e297/sourcing-ai-agent/docs/TRACK_C_C1_DURABLE_PLAN_TASK_DESIGN.md
- https://github.com/sorachang1874/Sourcing-AI-Agent-Dev/blob/0cfaec6c48b95e57f55f1bce06fe5b3abae0e297/sourcing-ai-agent/docs/TRACK_D_AGENT_RUNTIME_PLAN.md
- https://github.com/sorachang1874/Sourcing-AI-Agent-Dev/blob/0cfaec6c48b95e57f55f1bce06fe5b3abae0e297/sourcing-ai-agent/docs/TRACK_D_D0_MODEL_TOOL_RUNTIME_DESIGN.md
- https://github.com/sorachang1874/Sourcing-AI-Agent-Dev/blob/0cfaec6c48b95e57f55f1bce06fe5b3abae0e297/x-first-researcher-sourcing/AGENTS.md
- https://github.com/sorachang1874/Sourcing-AI-Agent-Dev/blob/0cfaec6c48b95e57f55f1bce06fe5b3abae0e297/x-first-researcher-sourcing/docs/GROK_CAPABILITY_GATE.md
- https://github.com/sorachang1874/Sourcing-AI-Agent-Dev/blob/0cfaec6c48b95e57f55f1bce06fe5b3abae0e297/x-first-researcher-sourcing/docs/X_FIRST_FIXTURE_CONTRACT.md
- https://github.com/sorachang1874/Sourcing-AI-Agent-Dev/blob/0cfaec6c48b95e57f55f1bce06fe5b3abae0e297/x-first-researcher-sourcing/docs/advisories/2026-07-14-grok-stage1-capability-plan.md

If the exact commit or any file cannot be retrieved, return a precise `CONNECTOR_BLOCKED` response with no verdict and
stop. Do not answer from memory, web search, or inferred local state. Do not write the repository, create issues or
Epics, push, contact people, use credentials, or claim access to uncommitted files.

## Verified execution context

- Track C C1a is already committed and validated. C1b implementation is starting locally but is not visible to the
  Connector and must not be reviewed as completed work.
- C1b may preserve current HTTP 200 plus pending behavior, remove the synchronous compile fallback, centralize the
  plan-submit hydration owner, fail unknown public task status closed, and add source/AST ratchets. It may not add
  schema, provider/model calls, durable storage, TTL/replay, or switch the service to 202.
- Track C C1c and later remain behind the recorded owner decisions.
- Track D D0a may build provider-neutral model/tool runtime and a draft-only route registry in simulate/scripted mode.
  It may not enable live transport or change existing business semantics.
- The X-first Stage 0 fixture is committed. Grok's offline advisory proposes only a future account-level capability
  probe; live X access and researcher mapping remain NO-GO pending explicit owner/legal/privacy/model/access/cost/rate/
  deadline/kill-switch/account/retention decisions.
- Reviews are scoped and asynchronous. A pending or negative review freezes promotion/live/W6/manual/product signoff
  only for its scope; it does not freeze unrelated implementation.
- Formal review remains separate and repository-defined. This consultation cannot satisfy it.

## Population and safety boundary

The future discovery population may use only lab, current professional affiliation, and pretraining relevance. Never
infer or proxy ethnicity, nationality, race, citizenship, religion, gender, or any protected identity from name,
language, region, school, community, biography, post, mention, or graph position. Stage 1 must not identify or rank
people at all.

## Questions

1. Should the three-lane direction be kept, adjusted, or pivoted?
2. What is the safest dependency graph and commit sequence for C1b and D0a to run in parallel without shared-contract
   races?
3. Which exact acceptance gates and rollback boundaries make each batch independently shippable?
4. Which assumptions in C1b or D0a still conflict with the pinned contracts?
5. What is the smallest bounded offline X-first Stage 1 schema/validator/test slice that can proceed while live access
   stays NO-GO, and which owner decisions must remain unresolved rather than guessed?
6. Which old heuristic, fallback, ambiguous owner, or manual step can one more bounded step retire in each lane?
7. Provide a two-wave lookahead after the immediate batches so Codex can continue useful work while scoped reviews run.

## Required response

Return exactly one complete Markdown artifact between these markers:

BEGIN_ARTIFACT path=sourcing-ai-agent/docs/pro-consults/2026-07-14-parallel-roadmap-plan/response.md
END_ARTIFACT

The advisory label must be the line immediately after `BEGIN_ARTIFACT`. Use this exact ordered shape outside code
fences, with exactly one lower-case unmodified verdict directive inside Verdict:

# Verdict
Raw Pro verdict: keep|adjust|pivot

# Scope understood

State repository, exact observed full commit, raw branch lookup outcome, and all ten immutable file citations.

# Assumptions and missing information

# Findings
## P0
## P1
## P2

# Recommended sequence

Give an explicit lane/dependency/commit table, immediate batches, and two-wave lookahead.

# Validation and failure modes

# Deferred decisions

# Owner decisions required

Do not add a second verdict directive, modifier, blockquoted verdict, formal-GO claim, or text outside the marker
envelope.
