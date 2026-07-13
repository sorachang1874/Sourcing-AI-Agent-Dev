# Track C C1a approach-review request

ADVISORY_ONLY — not an independent-review artifact or formal GO.

Use the GitHub app already active in this conversation for a read-only, commit-pinned approach review.

Purpose: approach_review
Authority: ADVISORY_ONLY — not an independent-review artifact or formal GO.
Repository: sorachang1874/Sourcing-AI-Agent-Dev
Base commit: 5ab9798b5e8e1935cc1520abc82204c9261f5fef
Head commit: bc9037b7c3e962ed542031eab1d11c5a55a391e6
Branch label: governance-phase0-ttl-20260611

First verify the exact head commit and the base..head diff through GitHub. The full SHAs are authoritative; a mutable branch-list lookup is optional metadata. If the head commit or diff is unavailable, return connector_blocked and stop. Do not inspect or infer local untracked files.

Review only Track C C1a:
- exact light-lane classification for POST /api/projections/export, POST /api/crm/records/public-web-export, and GET /api/exports/{single-segment}; artifact/wrong-method/near-miss paths remain shared, with global OPTIONS as the documented exception;
- owner-supplied export task_id and artifact.handle fail-closed validation, separate submit/wait/download stages, immediate and queued success, and no binary fetch after invalid input;
- one frontend terminal-total status registry, aliases, unknown/missing fail-closed, completed-with-active-tail behavior, empty terminal timelines, history hydration ordering/error preservation, Excel/timeline/dashboard consumers;
- plan submit client accepts pending|queued without changing server HTTP semantics;
- tests and contract docs changed in the diff.

Author validation, to challenge rather than trust:
- targeted: 38 passed + 17 subtests;
- exact legacy classifier node: 1 passed;
- frontend TypeScript/Vite build passed (81 modules; existing >500 kB warning);
- make lint passed;
- mypy ratchet unchanged at 81 errors / 4 files; make typecheck is non-zero by baseline;
- final ci-pre-agent-contract: 349+2+11+1+2 passed and dry_run_ready;
- two independent read-only audits found two P1 issues (empty terminal timeline and history error race); both were fixed before the head commit and the re-audit found no remaining P0/P1.

Return one Markdown artifact exactly between:
BEGIN_ARTIFACT path=sourcing-ai-agent/docs/pro-consults/2026-07-14-c1a-approach-review/response.md
END_ARTIFACT

The artifact must contain:
1. connector evidence for base, head, and diff scope;
2. raw verdict and normalized recommendation keep|adjust|pivot;
3. P0/P1/P2 findings with file/contract evidence;
4. second-order consumers or states still missed;
5. exact tests or fixes for every P0/P1;
6. explicit statement that this is advisory and cannot satisfy the repository formal review gate.

Do not modify files, create issues/PRs, push, execute code, contact anyone, use credentials, or claim formal GO.
