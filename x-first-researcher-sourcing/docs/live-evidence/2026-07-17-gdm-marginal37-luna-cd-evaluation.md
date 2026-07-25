# GDM marginal-37 Luna semantic evaluation

Date of evaluation: 2026-07-16

Date of report: 2026-07-17

Status: candidate-free diagnostic live evidence. This is not a formal review artifact, a candidate handoff, a
population-exhaustion claim, or promotion authority.

## Decision

The Luna Phase C/D transport and adjudication method completed the frozen 37-candidate input without dropping a row.
Phase C accepted 37/37 inputs and produced a four-candidate evidence-supported recall pool. Two were unambiguous
`current lab / current pretraining`; two were `historical lab / ambiguous pretraining`. Phase D consumed exactly those
four accepted recall-pool rows and returned four terminal `none / none` China/Asia professional-experience proxy
judgments.

These are evidence-availability judgments over the supplied observation bundle, not ground-truth labels. Phase B had
hydrated only the 20 status IDs already attached to the discovery references. It had not run a candidate-scoped
technical Post/Reply search for each of the 37 accounts. Therefore `4/37` must not be reported as population precision,
recall, or the final number of useful GDM people. Likewise, Phase D `none` means no qualifying regional professional
signal was present in the reviewed observations; it does not prove absence of such experience.

Candidate handles, names, Bios, Post/Reply text, URLs, and per-candidate decisions remain only in the owner-private
artifacts. This document contains aggregate metrics only.

## Frozen input and runtime

| Item | Result |
|---|---:|
| Frozen queue SHA-256 | `8a6463c5f9437d06459738bb288616236f1ac2bb6a9498f77b4a95f56f0b78f7` |
| Candidate inputs / observations | 37 / 88 |
| Profile rows exactly matched | 33/37 |
| Hydrated status IDs / reconciled source refs | 20 / 57 |
| Model / effort | `gpt-5.6-luna` / low |
| Responses model calls / catalog calls | 7 / 1 |
| Exact returned-model calls | 7/7 |
| HTTP-200 accepted calls | 7/7 |
| Fallback / same-intent retry | 0 / 0 |
| Tools / store | none / false |
| Input / output / total tokens | 35,254 / 12,486 / 47,740 |
| Accepted-attempt elapsed | 250.650s |
| Secret-bearing files detected | 0 |
| Business candidate cap | none |

Each request bound the candidate set, candidate input hashes, target descriptor or regional policy hash, strict JSON
Schema, instructions, requested model, low reasoning effort, empty tools, `store=false`, and no fallback. Raw model
outputs were retained immutably in the private directory. Failed compatibility experiments contributed no accepted
record to this result.

## Phase C: independent lab and pretraining dimensions

| Lab state / pretraining state | current | historical | ambiguous | unsupported |
|---|---:|---:|---:|---:|
| current | 2 | 0 | 0 | 15 |
| historical | 0 | 0 | 2 | 7 |
| ambiguous | 0 | 0 | 0 | 4 |
| unsupported | 0 | 0 | 4 | 3 |

Mechanical outcomes:

- accepted / failed / unprocessed: 37 / 0 / 0;
- evidence-supported recall pool: 4;
- unambiguous qualified rows: 2;
- current/current precision tranche: 2;
- current/historical, historical/current, historical/historical: 0 / 0 / 0;
- historical/ambiguous retained for verification: 2.

The absence of a cross-temporal combination in this bundle is not a filter rule. The classifier still represents lab
affiliation and pretraining relevance independently and retains every supported current, historical, or ambiguous
combination. The result instead shows that the discovery-attached evidence was sparse for this mixed-phase marginal
queue.

## Phase D: China/Asia professional-experience proxies

Phase D was built only from the four Phase C rows where neither dimension was `unsupported`. It omitted subject name,
display name, and author handle from the regional-review payload. It disallowed identity, ethnicity, and nationality
inference and did not use name or handle signals.

| China proxy / Asia proxy | strong | weak | none |
|---|---:|---:|---:|
| strong | 0 | 0 | 0 |
| weak | 0 | 0 | 0 |
| none | 0 | 0 | 4 |

All four rows were accepted with zero validation error. The closed policy still permits explicit China/Asia
professional experience, China professional digital-ecosystem activity, and Chinese professional content at their
configured strengths. None of those signals appeared in the four supplied evidence bundles.

## Technical batching result

The initial whole-batch experiment used 36 candidates in one 70,616-byte request. It produced no HTTP response after
63.996 seconds and committed zero records. The accepted campaign replaced that shape with a request-byte-primary
controller. Eight candidates were only the observed operating-point sample, not a business cap.

| Phase | Planned input counts | Planned request bytes | Result |
|---|---|---|---|
| C after canary | 8, 8, 8, 8, 4 | 20,316; 20,107; 19,514; 19,714; 15,612 | all accepted |
| D | 4 | 13,125 | accepted |

The Phase C batch attempts took 27.353-53.772 seconds. No adaptive child or transport-control intent was needed in the
accepted run. The controller has no population ceiling: it partitions the entire input by measured serialized bytes
and output capacity, then uses new child intents only when a batch-local terminal/size failure requires it.

## Citation projection and replay audit

Luna returned 39 citations. Twenty-five raw citations had a wrong character offset even though the cited excerpt
occurred exactly once in the bound observation. The operator did not edit the raw output. It created a separate
deterministic projection only when all of the following held:

1. candidate, input hash, observation ID, surface, and source-text hash were exact;
2. the excerpt matched the bound source text byte-for-byte;
3. the excerpt occurred exactly once in that observation;
4. the projected start/end selected that unique occurrence.

Zero matches, multiple matches, source-hash drift, or binding drift fail closed. Each projection has an independent
private receipt containing opaque candidate/observation refs and original/new span hashes. The high projection rate
(`25/39`, 64.1%) is a concrete interface finding: future semantic schemas should ask the model for observation ID plus
exact excerpt and let deterministic code own span derivation, instead of asking a language model to count offsets.

The post-run replay checked all seven accepted calls and returned:

| Gate | Result |
|---|---:|
| Strict Phase C/D validator errors | 0 |
| Candidate sequence or identity errors | 0 |
| Phase D input rebuild errors | 0 |
| Repair binding errors | 0 |
| Canonical request-hash errors | 0 |
| Response-hash errors | 0 |
| Repair-receipt hash errors | 0 |
| Usage reconciliation errors | 0 |
| Rejected-batch records committed | 0 |

## What this validates

The experiment validates a scalable semantic stage after X-native retrieval:

- Luna can judge lab affiliation and technical relevance independently from open Bio/Post/Reply text under one strict,
  configurable contract;
- request-byte batching completes a 37-input campaign without imposing a business population cap;
- deterministic citation projection converts fragile model offsets into exact observation-bound spans without modifying raw
  model output;
- the recall pool and precision tranche remain separate, so ambiguous and historical combinations are not silently
  discarded;
- a separate regional professional-experience stage can review only the evidence-supported pool without changing the
  base lab/pretraining population.

## What remains unvalidated

This run does not prove GDM recall, precision against human labels, population exhaustion, source-bound X truth,
regional-experience absence, canonical-person identity, ranking quality, export readiness, or outreach readiness. In
particular, the 37-person input needs a new technical-evidence pass that searches each candidate's public Posts and
Replies rather than relying only on discovery-attached references.

The next controlled experiment therefore reuses the 37 cached profiles, performs no new `x_user_search`, searches
candidate-scoped technical Post/Reply surfaces across the configured pretraining function families, hydrates every new
stable status ID exactly, and reruns this same Luna Phase C contract. Results should report state transitions from the
frozen matrix, marginal qualified rows per native-X call, and unchanged/unsupported strata without exposing candidate
content.

The companion machine binding is
`2026-07-17-gdm-marginal37-luna-cd-evaluation.aggregate-receipt.v1.json`.
