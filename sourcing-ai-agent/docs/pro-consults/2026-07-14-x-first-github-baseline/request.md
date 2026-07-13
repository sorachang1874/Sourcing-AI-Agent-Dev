# X-first GitHub baseline consultation request

ADVISORY_ONLY — not an independent-review artifact or formal GO.

## Initial commit-pinned request

Use the GitHub app attached to this FIRST message in read-only mode.

Repository: sorachang1874/Sourcing-AI-Agent-Dev
Branch: governance-phase0-ttl-20260611
Pinned baseline commit: 5ab9798b5e8e1935cc1520abc82204c9261f5fef

First perform a connector handshake. Return CONNECTOR_OK only if you actually retrieve this repository at the exact full commit SHA and can cite connector-backed evidence from all requested files. If the repository, commit, or callable connector is unavailable, return CONNECTOR_BLOCKED with the precise reason and stop. Do not answer from memory, web search, or inferred local state.

Read:
- /AGENTS.md
- /sourcing-ai-agent/AGENTS.md
- /sourcing-ai-agent/docs/HANDOFF_CODEX_DESKTOP_AFTER_D3_2026-07-13.md
- /sourcing-ai-agent/docs/TRACK_C_C1_DURABLE_PLAN_TASK_DESIGN.md
- /sourcing-ai-agent/docs/QUERY_GUARDRAILS.md
- /sourcing-ai-agent/docs/MODEL_NATIVE_SEARCH_PROVIDER_CONTRACT.md

The pinned commit is a remote baseline and does not contain the current local C1a dirty diff. Never imply that you reviewed that local implementation.

If and only if CONNECTOR_OK, emit one self-contained Markdown artifact between exactly these markers:
BEGIN_ARTIFACT path=sourcing-ai-agent/docs/pro-consults/2026-07-14-x-first-github-baseline/response.md
END_ARTIFACT

The artifact must start with:
ADVISORY_ONLY — not an independent-review artifact or formal GO.

Include:
1. Connector evidence: repo, branch, full commit SHA, exact file citations.
2. Source-neutral contracts/modules the X-first fixture lane should reuse.
3. LinkedIn-specific or canonical-writer modules it must not couple to.
4. Conflicts between an X-first plan and actual repository contracts.
5. Smallest fixture-only sibling folder/doc/schema/test slice, with no provider/model call and no writes.
6. Owner/source-of-truth matrix, fail-closed state/identity rules, and local validation checklist.
7. Explicit protected-trait guardrail: discovery/ranking population is lab + current affiliation + pretraining relevance only; do not infer or proxy ethnicity/nationality via name, language, region, school, community, bio, post, mention, or graph.
8. Decisions accepted, deferred, and still requiring owner approval.

Do not execute code, modify files, create issues/Epics/PRs, push, contact people, use credentials, or claim access to local dirty files.

## Commit-authority correction

Correction to the handshake contract: the immutable full commit SHA is the content authority. A connector branch-list lookup is not a blocker when the connector has already verified the repository, retrieved the exact full commit SHA, and retrieved all six requested files at that SHA.

Reclassify the prior result as CONNECTOR_OK_COMMIT_PINNED. Record branch_verification=connector_unavailable and do not claim the connector verified the mutable branch ref. For separate operator context only, a local read-only git check reports HEAD and cached refs/remotes/origin/governance-phase0-ttl-20260611 both equal 5ab9798b5e8e1935cc1520abc82204c9261f5fef; this is not connector evidence.

Now use the attached GitHub app and the already retrieved commit-pinned files to emit the requested artifact exactly between:
BEGIN_ARTIFACT path=sourcing-ai-agent/docs/pro-consults/2026-07-14-x-first-github-baseline/response.md
END_ARTIFACT

Preserve every prior content, safety, citation, no-write, advisory-only, and dirty-local-state constraint. If any of the six files was not actually retrieved at that SHA, fail closed and identify only that missing file.
