BEGIN_ARTIFACT path=x-first-researcher-sourcing/docs/pro-consults/2026-07-14-x-live-transport/response.md
ADVISORY_ONLY — not an independent-review artifact or formal GO.

# Verdict

Raw Pro verdict: adjust

# Scope understood

The reviewed objective is limited to deciding the transport and evidence architecture before consuming one user-approved Stage 1 X capability probe. The current design is not a sourcing system, enrichment pipeline, identity resolver, outreach system, or production workflow.

The architecture separates:

- capability verification from discovery;
- observed platform evidence from inferred conclusions;
- raw X account identifiers from canonical person identity;
- subject-authored claims from corroborated affiliations;
- regional-professional ecosystem signals from physical region experience.

The current implementation prioritizes bounded execution, evidence minimization, fail-closed behavior, and offline validation before any live provider interaction.

# Assumptions and missing information

The assessment assumes:

- the installed Grok CLI remains the only available live transport for the Stage 1 handshake;
- no supported xAI API credential is currently available for a Responses/Batch implementation;
- the first live probe is intended only to answer "can the approved transport produce the expected native X evidence shape?";
- local author validation remains authoritative for implementation facts.

Missing information:

- actual first-call structured output shape from native X search;
- whether the CLI OAuth session remains valid;
- whether one assistant turn can produce multiple parallel native X calls before local monitoring reacts;
- whether future production access will use supported API credentials;
- baseline evaluation datasets and human-review protocols for Stage 2.

# Findings

## P0

### Keep the bounded capability handshake, but tighten the transport contract

The current CLI runner is acceptable for a single low-cost capability handshake because:

- execution is isolated;
- approval is atomically consumed;
- generic tools are disabled;
- unexpected tool receipts fail;
- evidence retention is minimized;
- no downstream product state depends on the result.

However, the decisive risk is not local process isolation. The decisive risk is that local kill-on-observation cannot prove that a second server-side provider action did not begin before termination.

Smallest discriminating validation:

- run exactly one approved native X query;
- capture every structured session event;
- verify that the observed event stream contains exactly one `x_search_call`;
- verify no additional provider-side call identifiers appear after completion;
- record this limitation explicitly if transport-level cancellation cannot be proven.

The contract should therefore describe this as:

"single-call observed capability probe with post-hoc reconciliation and bounded interruption"

rather than implying hard prevention of every remote-side parallel action.

### Do not promote CLI OAuth into a batch architecture

The current inference is correct: OAuth-backed CLI execution is suitable for a narrow handshake but is not a clean service boundary.

Future scaling should require a supported API credential and an explicit Responses/Batch adapter.

## P1

### Evidence model should distinguish observation layers

Minimal receipt/state distinctions:

| Capability | Minimum evidence |
| --- | --- |
| Native post retrieval | Structured native X tool receipt containing call id, source/post id, canonical URL, and retrieval result |
| Stable account identity | Numeric X user id plus observed handle history from provider response |
| Profile/Bio availability | Explicit profile fields returned by provider contract, versioned by field capability |
| Subject-claimed organization mention | Subject-authored Bio/profile text containing organization mention |
| Corroborated affiliation | Independent evidence sources or trusted records linking account/person/organization |
| Regional-professional ecosystem lead | Directly observed professional ecosystem activity; stored only as a review lead |
| Explicit physical region experience | Direct evidence explicitly indicating experience in that region, not inferred from language, name, location guesses, or ecosystem signals |

State transitions should prevent collapsing these levels:

Observed account signal

→ Proposed evidence item

→ Review candidate

→ Corroborated assertion

No transition should directly convert:

language → nationality,

name → ethnicity,

ecosystem → physical presence,

Bio mention → verified employment.

### Stage 2 minimum slice

The smallest useful Responses/Batch experiment should prove only:

"Given multiple query families, can the system retrieve unique stable X accounts with auditable evidence?"

Required scope:

- one supported Responses API path;
- two or three query families;
- one or two target labs plus one non-target comparison group;
- stable numeric account IDs as the deduplication primitive;
- no ranking;
- no outreach;
- no CRM/export;
- no identity resolution.

Required outputs:

- query receipt;
- account-id set;
- evidence receipt;
- deduplication statistics;
- human review queue.

Avoid optimizing around:

- one lab;
- one prompt;
- one Bio format;
- one provider response schema.

## P2

### Evaluator architecture needs independent truth sources

The evaluator should not accept:

- caller-supplied recovered ids;
- model-generated "found/not found" claims;
- self-reported guardrail incidents.

Mechanical evaluation design:

Precision:

- compare retrieved accounts against an independently created reviewed relevance set.

Conditional recall:

- measure recovery against a bounded golden set where inclusion criteria were established before retrieval.

Marginal query-family yield:

- calculate additional unique reviewed-valid accounts contributed by each query family after deduplication.

Efficiency:

- measure:
  - API/tool cost;
  - wall-clock latency;
  - human review minutes;
  - valid accounts per dollar;
  - valid accounts per reviewer hour.

Guardrails:

- instrument negative tests:
  - prohibited inference attempts;
  - unsupported fields;
  - missing evidence;
  - duplicate identities;
  - retention violations;
  - unauthorized writes.

## Missing P0/P1 risks and ownership gaps

Missing or requiring explicit ownership:

### P0

- Define owner for provider credential lifecycle if Responses/Batch is introduced.
- Define owner for evidence schema versioning.
- Define owner for retention enforcement and purge verification.
- Define owner for live budget limits and emergency shutdown.

### P1

- Add concurrency tests around:
  - multiple local runner instances;
  - approval receipt races;
  - duplicate batch submissions;
  - partial batch failures.
- Add idempotency contract:
  - request fingerprint;
  - provider request id;
  - retry semantics;
  - duplicate result handling.
- Add field-level capability tests:
  - post ids;
  - URLs;
  - author ids;
  - handle history;
  - Bio fields;
  - organization mentions.
- Add retention adversarial tests:
  - failed execution cleanup;
  - interrupted process cleanup;
  - expired artifact purge;
  - receipt tampering.

# Recommended sequence

1. Execute the single approved CLI capability handshake only.
2. Preserve the current bounded evidence model.
3. Update documentation to explicitly state the interruption limitation.
4. Evaluate actual structured X output shape.
5. Decide whether supported Responses API access is available.
6. Build only the smallest Responses/Batch slice after credential and contract decisions.
7. Introduce evaluation infrastructure before any researcher-scale expansion.

# Validation and failure modes

Required validations:

- exactly one native X tool call observed;
- stable numeric ids extracted from raw tool output;
- canonical URLs validated;
- no generic web/local tool execution;
- no unexpected provider action;
- artifact TTL and purge behavior verified.

Known failure modes:

- OAuth expiration;
- provider output schema drift;
- incomplete profile fields;
- hidden parallel provider execution before local kill;
- accidental retention expansion;
- query-family overfitting;
- treating weak evidence as identity truth.

# Deferred decisions

Deferred:

- production search transport;
- API credential strategy;
- batch scheduling;
- ranking models;
- golden-set construction;
- canonical identity layer;
- researcher discovery coverage;
- outreach workflows;
- product integration.

# Owner decisions required

Owner must decide:

- whether the CLI probe result is sufficient to justify a supported API adapter investigation;
- what evidence levels are allowed to progress from observation to review;
- what retention duration is acceptable;
- what independent review process governs Stage 2;
- what constitutes acceptable precision/recall/cost tradeoffs before broader experimentation.

END_ARTIFACT
