# Compact discovery and profile hydration pinned independent review

Date: 2026-07-16

## Evidence header

- Reviewed commit: `e7d9e0458f6b16664602532eaab8fff45e15c4f3`
- Parent: `28e7d8b7510f633abd2b26c1090bfc97b5af3712`
- Pinned tree: `cda6f91d43de46eb28cc9ce086da08fe6b0ef3c9`
- Exact scope: the nine files changed by this commit: two schemas, two runtime modules, two test modules,
  `README.md`, the reusable workflow document, and the aggregate GDM live-evidence document.
- Scope size: 9 files, 3,184 insertions, 6 deletions.
- Scoped binary-diff SHA-256: `7e2d09fef794374a960bba56a711feebae60c2900e761e1c0904f564b5860dad`.
- Inspection source: a read-only `git archive` of the pinned tree plus pinned `git show` / `git diff`; mutable
  working-tree implementation files and owner-only candidate artifacts were not used.
- Privacy boundary: no real handle, Bio, Post, Reply, location, URL, organization name, or private evidence body was
  read or reproduced. Adversarial probes used synthetic fixture identities only and report aggregate outcomes below.

## Validation

| Check | Pinned result |
|---|---|
| Targeted compact + hydration tests | 51/51 passed |
| `python -m x_first.contracts` | exit 0; fixture contract result `valid` |
| Ruff on the two runtime and two test modules | passed |
| Scoped `git diff --check` | passed |
| JSON Schema/runtime spot cross-validation | ordinary fixtures passed; deliberate cross-field invalids failed at runtime as designed |
| No-business-cap probes | 137 leads, 123 refs, and 137 hydration rows/calls passed the authored tests |
| Candidate-free aggregation probe | evaluator output contained no synthetic handle, Bio, location, or organization value |

The green checks establish syntax, deterministic behavior for the authored cases, and absence of fixed business array
caps. They do not close the trust and identity defects below.

## Findings totals

| Severity | Count |
|---|---:|
| P0 | 0 |
| P1 | 4 |
| P2 | 4 |
| P3 | 1 |

## Findings

### P1-1 — `new` — A shard is not bound to its campaign or target descriptor

The workflow says every campaign has a versioned target descriptor that owns lab aliases, official handles, project
families, time shards, role scope, enabled strategies, and desired fields
(`docs/GROK_COMPACT_DISCOVERY_AND_PROFILE_HYDRATION.md:23-38`). The compact schema instead requires only `status`,
`strategy_id`, leads, coverage, and limitations
(`contracts/x.grok.compact_discovery.result.v1.schema.json:7-14`). The merge accepts any sequence of otherwise valid
objects and an independently supplied output `strategy_id`; it checks no campaign id, target id, descriptor digest,
prompt-policy digest, or schema-policy compatibility (`src/x_first/compact_grok_discovery.py:591-617`).

Consequently, shards produced for different labs or different target-descriptor revisions are mechanically
indistinguishable and can be unioned. The relative field `target_lab_affiliation_state` then refers to different targets
inside one result, and every overlap, marginal-yield, and state-matrix metric becomes unsound. Add immutable campaign and
target-descriptor identities to the result and require exact equality before merge; bind the same identity through
profile-hydration batches and add a cross-campaign rejection test.

### P1-2 — `new` — Handle-only union both false-merges recycled accounts and double-counts renamed accounts

Validation enforces case-folded handle uniqueness but does not enforce platform-id uniqueness across leads
(`src/x_first/compact_grok_discovery.py:295-319`). Merge groups solely by case-folded handle
(`src/x_first/compact_grok_discovery.py:619-626`). When one handle has two non-null platform ids, the implementation
sets the id to null but still combines temporal states and every source reference into one lead
(`src/x_first/compact_grok_discovery.py:634-670`). Conversely, the same stable platform id under two handles remains
two unique leads. This conflicts with the README's existing identity behavior, which says conflicting ids and handle
rename quarantine (`README.md:125-132`).

Two candidate-free synthetic probes demonstrate both directions:

- one case-folded handle with two stable ids produced one runtime-valid merged lead with a null id and combined refs;
- one stable id under two handles produced two runtime-valid merged leads and `unique_lead_count=2`.

This can attach one person's evidence to another account after handle reuse and inflate recall after a rename. Conflicts
must be quarantined rather than evidence-merged. Stable-id aliases should become an explicit reversible handle-history
proposal; missing/model-mediated ids must not silently weaken that identity fence.

### P1-3 — `new` — The “completed native call” gate accepts an unbound call-shaped dictionary

The workflow requires exactly one **completed** `x_user_search` per expected handle and says zero, missing, duplicate,
or extra calls discard the batch (`docs/GROK_COMPACT_DISCOVERY_AND_PROFILE_HYDRATION.md:123-128`). The evaluator only
checks that each caller-supplied mapping has `tool_name == x_user_search` and a bare-handle `arguments.query`, then
compares counts (`src/x_first/grok_profile_hydration.py:476-523`). It does not require a completion event, successful
terminal state, call id, ledger sequence integrity, session/run/batch binding, result-event pairing, or proof that the
selected assistant terminal occurred after the last completion.

A synthetic batch with one result row and one dictionary containing only tool name and query—no completion or session
facts—returned `status=valid`. The same shape could be fabricated, taken from a call start that never completed, or
borrowed from another session. Replace the loose mapping list with a typed, receipt-bound reconciliation artifact that
proves start/completion pairing, exact batch/session identity, call lifecycle, and transcript ordering. A model result
plus unbound call arguments must remain invalid.

### P1-4 — `new` — Operator-owned limitation projection is optional at both consumption boundaries

The workflow requires projection before discovery union and before hydration ledger reconciliation
(`docs/GROK_COMPACT_DISCOVERY_AND_PROFILE_HYDRATION.md:68-74,130-133`). However, merge accepts ordinary validated
mappings rather than `CompactDiscoveryOperatorProjection` (`src/x_first/compact_grok_discovery.py:604-617`), and the
hydration evaluator validates the raw result directly (`src/x_first/grok_profile_hydration.py:424-438`). There is no
receipt digest or projected-state marker that a consumer can verify.

A synthetic hydration result containing a model-authored `execution_deadline_reached` claim plus an exact call-shaped
mapping was accepted as valid without calling the projection function. Compact merge has the equivalent bypass. In
addition, record-level `model_output_repaired` is allowed by the hydration schema/runtime but is never replaced by the
batch projection, so a model-authored repair claim survives even when the operator's repair fact is false
(`src/x_first/grok_profile_hydration.py:309-319,343-403`).

Integrate receipt-backed projection into merge/evaluation or require a typed projection envelope whose receipt and
terminal digests are verified. Remove record-level repair from model-owned input or project it from operator facts too.

### P2-1 — `new` — Projection validates model-owned status coherence before replacing model-owned technical claims

Compact projection first calls the full validator (`src/x_first/compact_grok_discovery.py:391`), and hydration
projection does the same (`src/x_first/grok_profile_hydration.py:353-355`). Therefore a structurally sound model output
with `status=OK` plus a false `result_truncated` claim is rejected for status incoherence before the operator can remove
that false claim. Candidate-free probes reproduced `status_coverage_invalid` in compact discovery and
`status_coherence_invalid` in hydration.

This is fail-closed but needlessly loses usable live results and makes the normalization behavior depend on whether the
model happened to keep its untrusted status consistent with its untrusted technical claim. Split validation into a
pre-projection structural/domain pass and a post-projection full coherence pass.

### P2-2 — `new` — An `ambiguous` axis may have no evidence while its reason code claims a signal

The compact validator requires source-dimension coverage only for non-ambiguous axes
(`src/x_first/compact_grok_discovery.py:355-361`), and the test deliberately accepts a lead with no source supporting
the ambiguous axis (`tests/test_compact_grok_discovery.py:216-238`). A candidate-free probe accepted a current-lab lead
with no pretraining-supporting reference while still serializing `pretraining_relevance_ambiguous_signal`.

Keeping unresolved candidates for recall is reasonable, but `unknown/unassessed` is not an evidence signal. As written,
the recall pool and “percentage with axis support” can conflate no evidence with conflicting or weak temporal evidence.
Either require at least one source supporting every `*_ambiguous_signal`, or introduce a separate unsupported/unknown
state and keep it out of evidence-supported denominators and precision tranches.

### P2-3 — `new` — Lookup status permits mutually contradictory failure limitations

For a non-matched row, validation only requires that the status-specific limitation be present; it does not reject the
other lookup-failure codes (`src/x_first/grok_profile_hydration.py:313-325`). A synthetic `not_found` row carrying
`lookup_not_found`, `lookup_blocked`, and `lookup_error` simultaneously passed schema and runtime validation. These
states imply different recovery actions and can misroute adaptive splitting or observability. Require exactly the one
lookup-failure code implied by `lookup_status`, plus only explicitly orthogonal limitations.

### P2-4 — `new` — The merge output cannot itself preserve the promised per-shard membership audit

The workflow promises that per-shard membership remains inspectable so marginal contribution can be recomputed
(`docs/GROK_COMPACT_DISCOVERY_AND_PROFILE_HYDRATION.md:89-90`). The merged contract contains no shard origin on a lead
or source ref, while `CompactDiscoveryMergeSummary` retains only totals
(`src/x_first/compact_grok_discovery.py:124-145,699-711`). Once the input shard objects are not co-retained, the merged
artifact cannot reconstruct which shard supplied a lead or whether a source ref was cross-shard.

Persist a candidate-free membership sidecar or bind the merged artifact to immutable input-shard digests and retain the
inputs under the same lifecycle. This is required for repeatable marginal-yield and strategy-failure analysis.

### P3-1 — `new` — Live metrics are internally consistent but not pinned-source reproducible

The aggregate document is careful to label itself diagnostic-only and candidate-free. Its arithmetic checks out:
`46+39+48=133`, `53+44+45=142`, `93/142=0.655`, `161.050+191.115+200.791=552.956`, `139/181=76.8%`, and the profile
coverage rates reconcile to the stated 42 matched records. However, its source is an absolute owner-only directory and
the tracked document binds no aggregate receipt hash, transcript digest, or machine-readable metric artifact
(`docs/live-evidence/2026-07-16-gdm-compact-strategy-matrix.md:14-23`). A pinned reviewer can verify arithmetic and code
compatibility, not provenance. Keep the diagnostic label; before any formal performance claim, add a candidate-free,
hash-bound aggregate receipt derived from the private ledgers.

## Confirmed properties and residuals

- The two schemas are closed and intentionally contain no business `maxItems` cap for leads, records, references,
  profile links, or affiliations; authored tests exercise populations above 100.
- Post, Reply, quote, mention, official-Post, and thread surfaces are first-class. The candidate-free GDM aggregates are
  arithmetically consistent with a status-surface majority, so the design is not Bio-only.
- Runtime source binding correctly rejects root/search/placeholder profiles, mismatched subjects, status-author
  mismatches, duplicate case-folded handles, and missing support on concrete axes.
- Hydration aggregate output is candidate-text-free and exact row/call set reconciliation rejects zero, missing,
  duplicate, extra, wrong-family, and non-bare-query cases that are represented by the current loose input shape.
- Source content and profile fields remain `model_mediated_unverified`; Grok CLI still lacks replayable native result
  bodies and query-to-lead attribution. The README correctly keeps source-bound hydration and formal promotion outside
  this slice (`README.md:232-239`).
- The diagnostic live figures remain useful for strategy iteration, but not for a source-bound candidate handoff or
  population-exhaustion claim.

## Final verdict

NO-GO
