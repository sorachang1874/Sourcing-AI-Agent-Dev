# Source-neutral mapping pinned re-review

Date: 2026-07-17

## Evidence header

- Reviewed commit: `b6593c86ca8b1d67e746d55232eb815308c1af7d`
- Exact parent: `fa43632790ddd377de712945018947f86ee3123c`
- Pinned tree: `d436c2a531c3200c27fd1b14df23de8f98312f9a`
- Commit subject: `fix(x-first): bind source-neutral mapping evidence`
- Relationship: the reviewed commit has exactly the requested parent.
- Exact scope: the 11 changed files under `x-first-researcher-sourcing`; no sibling-repository changes were reviewed.
- Scope size: 11 files, 1,795 insertions, 363 deletions.
- Scoped binary-diff SHA-256: `a2cebcdd311a92a34f5ee1be2b0d38dfe53108940906212d32e92dc1d419c499`
- Scoped name/status SHA-256: `fed08e4700f73214ac6ca0c6c04368d0f3b6fdc53c739976d406ca96e45bcd2e`
- Scoped numstat SHA-256: `9510e4eba58ee311d51587d3e91e2bdef0e67a51fee3b3322879b72723ffb6f7`
- Inspection source: clean detached worktree `/private/tmp/xfirst-b6593c8-review.Ba3Hiq` at the exact pinned commit plus pinned Git objects.
- Review boundary: no network, provider, model, credential, or live X call was made. Mutable working-tree implementation files were not used as review evidence, and the review did not modify the pinned worktree.

## Scoped blob identities

| File | Pinned blob |
|---|---|
| `README.md` | `73d1d8ff1aa57a0065f53d2dc9b2e0e2cffc223d` |
| `configs/source_neutral_mapping_policy.v1.json` | `669ffbcc8cfc96d8d80a9c2b20ed09e4c3a7928d` |
| `contracts/source_neutral_mapping_contract_registry.v1.json` | `27838416c5e65b24f22e64354562c3c15a0b2a3b` |
| exact-Post-hydration schema | `0833bb35ef7448b674f1f01aacba0ba6bd8e650f` |
| Luna-state-review schema | `53b95fdd76048fb5327d4b75eff4632a5a2c3e31` |
| `docs/SOURCE_NEUTRAL_MAPPING_CONTROLLER.md` | `008a1cddc3b217267b7e2b53ddb3c88eaf1eef7f` |
| `src/x_first/source_neutral_mapping.py` | `197a9eadfbaa81912a6353a6eb98b6611254725a` |
| `tests/test_source_neutral_mapping.py` | `7461f8dc682a7e59ace7746c63c5ef33339c4212` |

## Validation

| Check | Pinned result |
|---|---|
| `PYTHONPATH=src .../.venv/bin/python -m unittest tests.test_source_neutral_mapping -v` | 22/22 passed in 5.791s |
| `PYTHONPATH=src .../.venv/bin/python -m unittest discover -s tests -v` | 503/503 passed in 119.888s |
| `PYTHONPATH=src .../.venv/bin/python -m x_first.source_neutral_mapping` | exit 0; `errors=[]`; `status=valid` |
| `PYTHONPATH=src .../.venv/bin/python -m x_first.contracts` | exit 0; `errors=[]`; precision/recall both 1.0 |
| Repository-wide Ruff lint | passed |
| Ruff format for the two changed Python files | passed; 2/2 already formatted |
| Scoped `git diff --check` | passed |
| Whole-repository Ruff format | 19 pre-existing files outside this diff would be reformatted; neither changed Python file is among them |
| Rejected-work denominator probe | plan denominator 12, completed numerator 0, one retry item, one pending item |
| Caller-Luna-authority probe | caller-created `unsupported/unsupported + qualified=true` result for a `current/current` prior was accepted and counted 1/1 without a provider receipt |
| Structural-lineage probe | the same execution produced 12 new IDs with an empty prior, zero after the caller listed those same IDs as prior, and two such facts stopped the campaign |
| Strategy-distinctness probe | byte-equivalent query/tool/mode arguments with only plan/run identity changed were classified materially distinct |
| Replay-identity probe | identical session and request IDs were accepted for incompatible mapping and hydration transcripts; 12 hydration and 12 Luna projections were reusable across both trailing waves |
| Upgrade-multiplicity probe | two reviewed Posts for one already-`current/current` candidate were counted as two qualified state upgrades |

## Prior-finding disposition

1. Exact manifest/policy Wave P call reconstruction is fixed. Validation reconstructs the full expected plan and the authored mutation matrix rejects candidate, author, ordinal, champion cell, alias group, aliases, tool, query, limit, mode, and batch-placement drift.
2. Raw mapping-session authority and exact Post hydration are fixed at their immediate boundaries through retained typed projections and replayed source bytes. The new Luna boundary, however, remains caller-authored and is finding P1-1 below.
3. Unexecuted and rejected waves no longer satisfy structural convergence, but the convergence claim remains forgeable through caller-owned prior lineage, result authority, and strategy identity. Findings P1-1 through P1-3 cover the remaining mechanisms.
4. Missing execution coverage is now separated into pending and retry/split queues; sparse classification requires a fully attested champion batch, and seedless thread challengers are omitted. This prior finding is fixed.
5. Planned-call denominators now come from the bound plan, rejected work remains visible, and impossible zero-propagation states are rejected. The arithmetic finding is fixed, while P2-2 covers the separate semantic-transition counting defect.

## Findings totals

| Severity | Count |
|---|---:|
| P0 | 0 |
| P1 | 3 |
| P2 | 2 |
| P3 | 0 |

## Findings

### P1-1 — Caller-created Luna output is treated as qualified semantic authority

`build_luna_state_review_projection` validates only the JSON schema plus hydration/text digests and then self-hashes caller-provided JSON (`src/x_first/source_neutral_mapping.py:1265-1307`). The projection contains no outer Luna execution projection, model/prompt/policy binding, retained raw response, transport receipt, or deterministic transition adjudication. `build_executed_wave_facts` and `build_candidate_free_aggregate` then consume the caller-provided `qualified_state_upgrade` Boolean directly (`src/x_first/source_neutral_mapping.py:1455-1470,1502-1506,1718-1741`).

An independent negative probe started from a manifest candidate whose frozen prior was `current/current`, supplied a caller result of `unsupported/unsupported` with `qualified_state_upgrade=true`, and passed it through the public builder. The candidate-free aggregate reported one qualified upgrade from one terminal review, without any provider or model execution receipt. The same boundary can hide a real upgrade by supplying `false`, which makes the projection eligible to support a structural zero.

The result schema itself makes the Boolean caller-owned (`contracts/x.source_neutral.mapping.luna_state_review.v1.schema.json:19-41`). A typed dataclass and unkeyed content digest make mutation detectable after construction; they do not prove how the result was produced or whether its state transition is valid.

Use a receipt-first Luna artifact projection that binds approval, exact model and route, prompt/schema/policy versions, retained raw response, transport execution receipt, and deterministic semantic replay. Until that exists, this projection must remain explicitly unverified and must not authorize qualified-transition KPIs or structural stopping.

### P1-2 — Caller-owned prior IDs and absent campaign lineage can erase genuinely new Posts

`prior_stable_post_ids` enters `build_executed_wave_facts` as an independent caller sequence (`src/x_first/source_neutral_mapping.py:1421,1431-1435`) and is subtracted directly from replayed results (`src/x_first/source_neutral_mapping.py:1500-1501`). It is not derived from a predecessor frontier, campaign ledger, or prior-wave artifact. The positive authored test itself constructs the supposed prior set from the current wave's hydration queue (`tests/test_source_neutral_mapping.py:802-819`).

With exactly the same retained mapping, hydration, and review evidence, an independent probe produced 12 new IDs when the prior list was empty and zero new IDs when those same current-wave IDs were supplied as prior. Two such facts, with all other queues empty, returned `structural_convergence`.

The list supplied to `structural_stop` is also the sole owner of recency. The function takes its last two elements as consecutive without a campaign ID, sequence ordinal, predecessor digest, append-only history, or omitted-wave check (`src/x_first/source_neutral_mapping.py:1598-1604,1638`). A caller can therefore omit an intervening nonzero or failed wave and present any two otherwise valid facts as consecutive.

Derive the prior stable-ID frontier from a predecessor-bound campaign snapshot. Each wave fact needs a campaign ID, monotonic ordinal, exact predecessor-fact digest, prior-frontier digest, and append-only adjacency validation. The new-ID difference must be recomputed from that predecessor rather than an independent caller list.

### P1-3 — Run identifiers make an unchanged query strategy appear materially distinct

The strategy payload requires `strategy_id == plan_id` and includes `plan_sha256` (`src/x_first/source_neutral_mapping.py:1383-1408`). Both contain execution identity rather than retrieval semantics. `strategy_signature_sha256` hashes that complete object (`src/x_first/source_neutral_mapping.py:1521`), and structural distinctness requires only that the two signatures differ (`src/x_first/source_neutral_mapping.py:1651-1657`).

An independent probe built two plans whose ordered tool names, queries, limits, modes, aliases, candidate coverage, and time windows were byte-equivalent. Changing only plan/run identifiers changed the two strategy signatures; the controller classified them as materially distinct and stopped.

Compute a semantic strategy digest from normalized query surface, native tools, alias partitions, modes, time windows, relationship/topology dimension, and other actual retrieval choices. Exclude plan IDs, batch IDs, request IDs, and execution hashes. Two semantically identical strategies must retain one signature regardless of how many times they are scheduled.

### P2-1 — Replay identities are not globally unique across pipeline stages

Mapping session and request IDs are checked only against other mapping projections (`src/x_first/source_neutral_mapping.py:998-1022`). Hydration session and request IDs are independently checked only against other hydration projections (`src/x_first/source_neutral_mapping.py:1329-1347`). No wave-global or campaign-global proof-identity registry compares the two sets.

An independent probe assigned the same session ID and request ID to one mapping transcript containing `x_keyword_search` and a different hydration transcript containing `x_thread_fetch`. Both raw replays passed and `build_luna_input_queue` accepted the pair. Structural stopping checks cross-wave uniqueness only for mapping session/request/receipt identities (`src/x_first/source_neutral_mapping.py:1639-1650`); the probe reused all 12 exact-hydration projections and all 12 Luna-result projections in both trailing waves without rejection.

One execution identity cannot independently prove incompatible raw transcripts. Enforce one proof-identity registry across mapping and hydration lanes. If exact hydration or semantic output is intentionally reused as a cache, represent it through an explicit cache projection bound to the original execution, immutable source, predecessor frontier, and cache-consumer wave; do not count it as a second execution.

### P2-2 — “State upgrades” are counted per review Boolean, not as legal candidate/axis transitions

The Luna schema carries proposed axis states, a caller Boolean, and an arbitrary upgrade ID, but no prior-state binding, changed axis, transition rule, evidence-set reducer, or state-version owner. The aggregate simply sums every true Boolean (`src/x_first/source_neutral_mapping.py:1740-1741`). Executed-wave facts require unique upgrade IDs, but do not restrict one transition per candidate and axis (`src/x_first/source_neutral_mapping.py:1502-1506`).

An independent probe reviewed two Posts for one candidate already frozen as `current/current`. Both caller results claimed an upgrade, and the aggregate reported two qualified state upgrades for one candidate even though neither axis could legally improve. The authored aggregate test also permits one reviewed item out of two exact hydrations (`tests/test_source_neutral_mapping.py:932-956`), so the reported qualified rate can describe a selected subset without an explicit review-completion field.

Model output should propose evidence-linked axis states only. Deterministic code must reduce the complete review set against the candidate's versioned prior/current state, derive the legal transition and upgrade ID, and allow at most one transition per candidate/axis/state version. The candidate-free aggregate should expose semantic-review coverage or an explicit incomplete-review flag so selected subsets cannot be mistaken for end-to-end performance.

## Confirmed properties and accepted residuals

- Exact Wave P plan reconstruction now prevents self-consistent rehashing from changing policy-owned calls or batch placement.
- Mapping raw-session replay retains the six required source files and rechecks the canonical plan-owned precommit before frontier admission.
- Exact `x_thread_fetch` hydration retains raw artifacts, replays the queued Post ID, and binds descriptor host, author, requested/returned stable ID, URL, and UTF-8 source bytes.
- Pending and retry/split work is distinct from sparse evidence; seed-free official/project challengers remain available while thread challengers require at least one stable Post ID.
- Planned/completed execution counts and zero propagation are mechanically derived rather than accepted as independent integers.
- Partial semantic review may remain useful in an explicitly diagnostic artifact, but it cannot support structural convergence or a qualified-transition claim without explicit coverage and a deterministic reducer.
- Reusing an exact hydration or semantic result may be a valid future cache optimization only through a declared cache contract; silent execution-identity reuse is not accepted.
- The tracked calibration aggregate remains diagnostic, candidate-free method evidence. Owner-private calibration files were not opened or revalidated in this review.
- Historical exploratory Wave P sessions remain non-promotable because they lack the typed precommit required by this controller; this review does not change that boundary.
- The 19 repository-wide formatting differences are pre-existing and outside the pinned diff. The two changed Python files pass scoped Ruff lint and format checks.

## Final verdict

NO-GO
