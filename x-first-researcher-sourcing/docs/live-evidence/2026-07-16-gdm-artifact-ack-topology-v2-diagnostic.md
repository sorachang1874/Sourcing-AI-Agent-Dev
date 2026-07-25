# GDM artifact/ack topology v2 diagnostic

## Decision

The unhooked v2 arm is useful for recall exploration but is not a clean discovery shard. It returned 45
model-mediated proposals with 74 source references from 61 completed native-X calls in 174.228 seconds. Against the
frozen 96-lead / 199-reference-signature baseline it overlapped eight leads and proposed 37 marginal leads, for a
potential union of 133.

That potential union is not accepted. The private query audit found one person-seeded exact-name query containing two
known display-name seeds. The entire arm is therefore classified
`mixed_phase_model_mediated_unverified`; the accepted pure-discovery union remains 96. The 37 marginal proposals are
stored only in a private exact-profile + Post/Reply evidence-hydration queue and require a later Luna judgment before
any further promotion.

This document and its companion receipt contain aggregate metrics only. Handles, names, Bios, Posts, Replies,
locations, URLs, and raw query text remain in owner-private artifacts.

## Execution evidence

| Metric | Result |
|---|---:|
| Grok CLI / model / effort | 0.2.101 / `grok-4.5` / high |
| Exit / elapsed | 0 / 174.228s |
| Technical deadline | 300s; no timeout, TERM, or KILL |
| Native-X calls | 61 |
| Keyword / semantic / thread | 39 / 6 / 16 |
| Completed call records | 61/61 |
| Duplicate call signatures | 0 |
| Generic web / `x_user_search` | 0 / 0 |

The session ledger contains 61 `tool_call` records in progress and 61 matching completed updates. The final assistant
payload occurred after the last completed tool update.

## Query-family decomposition

The private query audit applies one exclusive, deterministic family per call. Precedence is acknowledgement,
topology, artifact, project, and other; `x_thread_fetch` has its own thread owner.

| Family | Calls |
|---|---:|
| Acknowledgement / credit | 17 |
| Research artifact | 17 |
| Thread fetch | 16 |
| Project / launch | 3 |
| Reply / mention topology | 1 |
| Other | 7 |

Per-family references and marginal leads are unavailable because the compact terminal contract does not retain a
query-to-reference or query-to-lead provenance edge. Assigning those outcomes to a family would be model guesswork.
The aggregate gain is nevertheless consistent with the arm spending 50/61 calls on acknowledgement, artifact, and
thread paths instead of repeating a broad Bio/function sweep.

## Terminal boundary

The wrapper reported a structured-output error because the model emitted three empty schema-shaped progress JSON
objects followed by one final JSON object. This violates the one-terminal-object transport contract. The fourth and
last object was emitted after all native-X calls completed, used the pinned strategy ID, and passed the pinned compact
runtime validator with zero errors.

Accordingly, the final object is retained as private diagnostic evidence, not treated as a wrapper-valid or qualified
result. Its aggregate composition was:

| Surface | References |
|---|---:|
| Official Post | 59 |
| Thread | 7 |
| Mention | 6 |
| Reply | 2 |
| Quote / Bio / profile / self-Post | 0 |

The terminal status was `X_DISCOVERY_PARTIAL` with only the three model-owned limitations:
`native_x_search_incomplete`, `profile_hydration_incomplete`, and `thread_hydration_incomplete`.

## Frozen-baseline comparison

| Metric | Result |
|---|---:|
| Frozen leads / ref signatures | 96 / 199 |
| Arm leads / refs | 45 / 74 |
| Lead overlap / marginal | 8 / 37 |
| Potential union | 133 |
| Ref-signature overlap / new | 1 / 73 |
| New refs on marginal / existing leads | 57 / 16 |
| Marginal leads per call | 0.606557 |
| References per call | 1.213115 |
| Marginal leads per minute | 12.741924 |

The potential union and efficiency values describe diagnostic yield only. Because the arm is mixed-phase and its
wrapper terminal was invalid, they do not enter the pure-discovery accepted union or its compliance efficiency
denominator.

## Relative performance

| Arm | Calls | Marginal leads | Marginal/call | Marginal/minute |
|---|---:|---:|---:|---:|
| Function negative-space v2 | 23 | 3 | 0.130435 | 1.278 |
| Artifact/ack topology v1 | 57 | 20 | 0.350877 | 5.441313 |
| Artifact/ack topology v2 | 61 | 37 | 0.606557 | 12.741924 |

V2 was 4.650x higher than function v2 on marginal/call and 9.970x higher on marginal/minute. It was 1.729x higher
than artifact v1 on marginal/call and 2.342x higher on marginal/minute. Artifact v1 was also excluded for a phase
crossing, so these are method-performance comparisons rather than accepted-shard comparisons.

## Hook capability result

The process-scoped/plugin and private project-hook experiments are excluded. Grok CLI discovered the project hook,
but native X search executes as backend tool calls that did not emit `PreToolUse` events. A synthetic deny canary was
therefore executed with zero hook decisions. Grok CLI 0.2.101 cannot currently prove native-X query blocking through
this hook path.

Debug logging was removed after it was found to contain OAuth material. The private credential-bearing debug artifact
was purged, it did not enter Git or tracked docs, and the temporary private-folder trust entry was removed exactly.

## Next step

Run exact profile plus Post/Reply evidence hydration for the 37 private marginal proposals, retain only X-native,
ledger-bound but still model-mediated profile and status observations, and then run Luna judgment. No field becomes
source-bound unless a future transport exposes a replayable provider payload. No row is qualified, merged into a
canonical person, ranked, exported, or used for outreach by this diagnostic.

The candidate-free binding receipt is
`2026-07-16-gdm-artifact-ack-topology-v2.aggregate-receipt.v1.json`.
