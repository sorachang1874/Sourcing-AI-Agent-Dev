# GDM marginal-37 profile and status hydration

## Decision

The two-stage live hydration is technically complete for the frozen private queue. Phase A reconciled all 37 unique
account lookups; Phase B reconciled all 20 distinct stable status IDs and all 57 source references. Every accepted
batch used only its allowed native-X tool, completed exactly one call per input in order, emitted its terminal object
after the last completed call, and passed exact row and identity binding. Failed batches contributed zero records and
were deterministically bisected.

This is a transport and field-availability result, not a candidate qualification result. The hydrated observations
remain `model_mediated_unverified`; no row is promoted, ranked, merged into a canonical person, exported, or used for
outreach. Luna was not called. Candidate handles, names, profile fields, Post text, URLs, and per-candidate decisions
remain only in owner-private artifacts.

## Frozen scope and runtime

| Item | Result |
|---|---:|
| Private queue SHA-256 | `8a6463c5f9437d06459738bb288616236f1ac2bb6a9498f77b4a95f56f0b78f7` |
| Account inputs / casefold-unique | 37 / 37 |
| Source references / distinct status IDs | 57 / 20 |
| Grok CLI / model / effort | 0.2.101 / `grok-4.5` / high |
| Allowed Phase A tool | `x_user_search` only |
| Allowed Phase B tool | `x_thread_fetch` only |
| Combined attempt elapsed | 847.696s |
| Technical attempt deadline / output ceiling | 300s / 2,000,000 bytes |
| Business result cap | none; the entire frozen queue was consumed |

The execution used single model invocations with no fallback, no generic web, no subagents, and owner-private raw
artifacts. Neither phase reached its campaign deadline or attempt/output kill condition.

## Phase A: exact account hydration

| Metric | Result |
|---|---:|
| Exact accepted rows | 37/37 |
| Matched / not found / ambiguous / tool error | 33 / 4 / 0 / 0 |
| Stable platform IDs resolved | 33 |
| Remaining handle-keyed rows | 4 |
| Duplicate stable IDs / identity-count delta | 0 / 0 |
| Attempts accepted / rejected | 7 / 5 |
| Native calls useful / rejected / total | 37 / 41 / 78 |
| Other native-X calls | 0 |
| Rejected records committed | 0 |

The input queue carried no expected platform IDs. Expected-ID comparison was therefore unavailable on all 37 inputs,
and the receipt records `expected_platform_id_input_count=0`; it does not present that unavailable check as positive
evidence. Handle binding, returned-handle casefold equality, accepted-row sequence, and cross-batch stable-ID collision
checks all passed. The identity subject count remains 37: 33 rows upgraded to stable-ID identity, four remain bound
only by their requested handle, and no two rows collapsed to one stable ID.

Five non-singleton attempts were rejected and then bisected. Three contained profile availability-ledger
inconsistencies, three violated terminal ordering, and one returned a model-only answer with zero native calls; reason
counts overlap by attempt. The observed batch outcome was non-monotonic: a 19-row batch passed while 18-, 9-, and
5-row batches also failed. Batch size alone is therefore not a sufficient controller; exact ledger gates plus adaptive
bisection are required.

### Profile field availability

| Field | All 37 | Matched 33 |
|---|---:|---:|
| Stable platform ID | 33 (89.2%) | 33 (100.0%) |
| Display name | 33 (89.2%) | 33 (100.0%) |
| Bio | 31 (83.8%) | 31 (93.9%) |
| Affiliation | 27 (73.0%) | 27 (81.8%) |
| Organization-affiliation signal | 27 (73.0%) | 27 (81.8%) |
| Verification | 12 (32.4%) | 12 (36.4%) |
| External URL | 2 (5.4%) | 2 (6.1%) |
| Location | 0 (0.0%) | 0 (0.0%) |
| Professional category | 0 (0.0%) | 0 (0.0%) |

The live result supports Bio and explicit affiliation enrichment, but not a claim that `x_user_search` reliably
returns location, professional category, or external links. Those fields remain unavailable when the tool did not
expose them; nothing was inferred to fill the gaps.

## Phase B: exact status evidence hydration

| Metric | Result |
|---|---:|
| Exact status rows accepted | 20/20 |
| Source references reconciled | 57/57 |
| Matched / not found / ambiguous / tool error | 20 / 0 / 0 / 0 |
| Attempts accepted / rejected | 4 / 2 |
| Native calls useful / rejected / total | 20 / 0 / 20 |
| Other native-X calls | 0 |
| Rejected records committed | 0 |

The 57 references comprised 47 official-Post, six thread, three mention, one reply, and zero quote references. Each
reference retained its private source-ref binding after its status ID was hydrated. Full text, exact author handle,
and published time were available for all 20 statuses. An explicit structural relationship was available for only one
status (5.0%); the other 19 remain null rather than being inferred from prose or thread order.

Both initial 10-ID batches returned model-only answers with zero native calls and were rejected. All four deterministic
5-ID children completed their five exact `x_thread_fetch` calls and passed. In this run, five IDs was a reliable
execution size for exact-status hydration; it is an observed operating point, not a hard product limit. The adaptive
controller remains necessary so future payload size or model variance can split further without dropping inputs.

## End-to-end reconciliation

| Gate | Result |
|---|---:|
| Accepted completed calls equal accepted inputs | PASS (37/37 and 20/20) |
| Terminal after last native completion | PASS for all 11 accepted attempts |
| Exact input and output sequence | PASS |
| Handle / post-ID binding errors | 0 |
| Expected platform-ID binding errors | 0, with 0 expected IDs available |
| Failed batch records committed | 0 |
| Failed leaves | 0 |
| Generic web / disallowed native-X calls | 0 / 0 |
| OAuth, transport, timeout, or output-ceiling failures | 0 |

Across both phases, 57 accepted native calls required 98 completed native calls because Phase A discarded 41 calls
from contract-invalid batches. Useful-call efficiency was 58.16% overall and 47.44% for profile hydration. The
performance improvement target is therefore protocol adherence and adaptive batch control, not adding arbitrary
candidate or observation caps.

All 20 exact-text status rows, all 57 references, and all 37 distinct queue candidates satisfy the mechanical input
rule for a future Luna semantic pass: exact status-ID match plus non-empty full text. That is an eligibility count, not
a semantic judgment. Luna remains a separate next stage and was not called here.

The candidate-free binding receipt is
`2026-07-16-gdm-marginal37-profile-status-hydration.aggregate-receipt.v1.json`.
