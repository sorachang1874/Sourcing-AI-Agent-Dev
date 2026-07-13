BEGIN_ARTIFACT path=sourcing-ai-agent/docs/pro-consults/2026-07-14-c1a-approach-review/response.md

ADVISORY_ONLY — not an independent-review artifact or formal GO.

Track C C1a approach review
1. Connector evidence: base, head, and diff scope
Commit authority

Repository:

s orachang1874/Sourcing-AI-Agent-Dev

Base commit:

5ab9798b5e8e1935cc1520abc82204c9261f5fef

Head commit:

bc9037b7c3e962ed542031eab1d11c5a55a391e6

Connector verification:

Head commit was retrieved through GitHub connector.

Retrieved commit metadata confirms:

SHA: bc9037b7c3e962ed542031eab1d11c5a55a391e6

Message: feat: close Track C C1a transport contracts

Base/head comparison was retrieved through GitHub connector.

Comparison result:

base: 5ab9798b5e8e1935cc1520abc82204c9261f5fef

head: bc9037b7c3e962ed542031eab1d11c5a55a391e6

status: ahead

ahead_by: 1

total_commits: 1

Diff scope

Connector diff reports C1a touched:

backend:

src/sourcing_agent/api.py

frontend:

frontend-demo/src/lib/workflowStatus.ts

frontend-demo/src/lib/api.ts

frontend-demo/src/lib/sourcingBackend.ts

frontend-demo/src/lib/workflow.ts

frontend-demo/src/pages/SearchPage.tsx

frontend-demo/src/components/ExcelWorkflowIntakePanel.tsx

frontend-demo/src/components/ExecutionTimeline.tsx

frontend-demo/src/types.ts

contracts/docs:

docs/TRACK_C_C1_DURABLE_PLAN_TASK_DESIGN.md

docs/WORKFLOW_PROGRESS_CONTRACT.md

docs/FRONTEND_API_CONTRACT.md

PROGRESS.md

NEXT_TODO.md

tests:

tests/test_c1a_transport_contract.py

tests/test_frontend_c1a_contract.py

tests/test_frontend_candidate_filters.py

tests/test_frontend_plan_contract.py

No local worktree, untracked files, or dirty diff were inspected.

2. Raw verdict and normalized recommendation
Raw verdict

KEEP — with deferred C1b follow-up explicitly preserved.

The approach is structurally correct for the declared C1a boundary:

storage-free;

no provider/model/live validation;

transport/client contract correction only;

frontend terminal semantics centralized;

export validation fails closed.

The commit itself records that C1a is intended to be limited to:

exact export light lane;

owner-supplied artifact.handle validation;

terminal-total frontend status registry;

Plan pending|queued bridge;

no schema/provider/model changes.

Normalized recommendation

keep

Rationale:

The implementation direction matches the repository's contract-first model. The remaining known debt (async_task_contract.py unknown → running behavior) is correctly isolated as C1b rather than silently expanded into C1a.

3. Findings
P0 findings

None found from commit-pinned scope review.

P1 findings

None remaining in the reviewed head scope.

Previously identified P1 class: empty terminal timeline

Status:

Resolved before head commit.

Evidence:

The C1a diff introduces a single frontend workflow-status registry and documents terminal handling:

completed

failed

cancelled aliases

missing/unknown fail-closed

The frontend contract update states that missing/unknown statuses become terminal failed states and that terminal timeline behavior is centralized.

Required validation:

tests/test_frontend_c1a_contract.py

timeline rendering coverage through shared status registry

Result:

Accepted as fixed based on commit evidence and supplied validation record.

Previously identified P1 class: history hydration error race

Status:

Resolved before head commit.

Evidence:

The head commit changes frontend history/dashboard/timeline consumers to consume shared workflow status semantics instead of local terminal assumptions.

The commit summary states:

“Frontend workflow status now has one terminal-total registry across API mapping, launch/reuse, SearchPage, Excel intake, timeline rendering, history recovery, and dashboard caching.”

Required validation:

history recovery regression coverage;

frontend contract tests;

dashboard cache terminality checks.

Result:

Accepted as fixed based on commit evidence and supplied validation record.

P2 findings
P2-1: async_task_contract authority mismatch remains

Status:

Deferred correctly, but still open.

Evidence:

The commit explicitly records:

frontend terminal registry is closed;

Python async_task_contract.py unknown→running behavior remains C1b debt.

Risk:

There are temporarily two semantic layers:

frontend fail-closed projection;

backend async task contract with older fallback behavior.

This is acceptable only because:

it is explicitly documented;

it is not extended into new frontend fallback logic;

C1b owns closure.

Recommendation:

Keep deferred.

Do not silently normalize this in unrelated C1a follow-up work.

4. Contract evidence review
Export light-lane classification

Accepted.

The diff contract states:

Light lane:

POST /api/projections/export

POST /api/crm/records/public-web-export

exact single-segment GET /api/exports/{single-segment-id}

Shared lane:

binary artifact download;

wrong methods;

trailing segments;

extra segments.

Exception:

global CORS OPTIONS.

The commit records this exact classification and warns that frontend must not infer lane from URL prefixes.

Assessment:

KEEP.

The separation avoids accidentally making binary transfer a light-lane operation.

Export task_id and artifact.handle validation

Accepted.

Required behavior:

owner supplies task handle;

client treats it as opaque;

no alternate derivation;

invalid handles fail before polling/download.

The commit records:

missing/invalid handles reject;

near-miss artifact paths do not trigger binary fetch;

fallback URL construction is prohibited.

Assessment:

KEEP.

This matches fail-closed transport design.

Frontend terminal-total status registry

Accepted.

The commit introduces:

frontend-demo/src/lib/workflowStatus.ts

and centralizes:

status normalization;

terminal classification;

aliases;

unknown handling.

Diff summary states:

cancel aliases stop as cancelled;

missing/unknown stop as failed;

completed jobs with active background work retain intentional effective-running tail.

Assessment:

KEEP.

This removes duplicated frontend terminal logic.

Plan submit pending|queued bridge

Accepted.

The commit preserves server HTTP behavior and changes only client interpretation:

accepts pending|queued;

does not change server HTTP semantics.

Assessment:

KEEP.

5. Second-order consumers or states still missed

The reviewed approach is complete for C1a, but the following remain future attention points:

1. Backend/public task semantic convergence

Still deferred:

async_task_contract.py

durable consumer handle semantics

C1b characterization

The C1 design states durable task ownership and consumer lifecycle are future work, not C1a scope.

2. Provider/live validation boundary

No concern in C1a.

The design boundary remains:

no provider call;

no model call;

no live environment.

Future provider validation must continue through the independent review gate.

3. Artifact lifecycle ownership

Current approach correctly avoids client-side ownership.

Future work should preserve:

export owner owns artifact lifetime;

client only observes task/artifact contracts.

4. Additional frontend consumers

The registry approach is correct, but future additions must not recreate local terminal sets in:

new dashboard widgets;

export history views;

timeline variants;

new workflow surfaces.

6. P0/P1 fixes and validation mapping
Issue	Fix	Validation
Empty terminal timeline	Centralized terminal-total status registry and terminal projection rules	frontend C1a contract tests; timeline consumers
History hydration error race	Shared status semantics across history recovery/dashboard/timeline consumers	frontend contract regression tests
Unknown status fallback risk	Deferred backend authority cleanup to C1b; no new frontend fallback	documented debt; C1b ownership
7. Validation evidence considered

Author validation was treated as evidence, not as formal approval.

Recorded validation:

targeted C1a group:

11 passed + 17 subtests

adjacent frontend contracts:

26 passed

combined targeted regression:

38 passed + 17 subtests

exact legacy classifier node:

passed

frontend TypeScript/Vite build:

passed

make lint:

passed

mypy:

unchanged baseline 81 errors / 4 files

contract lane:

349+2+11+1+2

dry_run_ready

The commit itself records that these are author-side evidence and that independent review remains separate.

8. Formal review gate statement

This artifact is:

advisory only;

read-only;

commit-pinned;

not an independent-review artifact;

not a formal GO.

It cannot satisfy the repository independent review gate.

The repository's review model requires an independent non-author review artifact before promotion/signoff. The C1a commit itself also states that independent scope review remains pending and that author evidence is not formal GO.

END_ARTIFACT
