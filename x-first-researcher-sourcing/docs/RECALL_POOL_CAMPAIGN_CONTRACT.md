# Recall-pool campaign replay and merge contract

Status: offline author implementation. Independent review is still required before milestone promotion. This lane
performs no Grok, X, network, product, CRM, export, ranking or outreach call.

## Product invariant

One model answer is not a population boundary. The campaign accepts an ordered, extensible series of independently
planned X-search waves and merges every valid candidate. It has no 20/25-person target, no observation budget, no
per-wave business cap, and no automatic stop. `business_candidate_limit=null`, `stop_advisory_enforced=false`, and
`candidate_count_triggered=false` are closed contract fields.

The policy's large byte/row ceilings are process kill switches for malformed input. They are not research targets or
search budgets. Raising or lowering a kill switch cannot change which candidate is valuable.

## Contract owners

| Owner | File |
|---|---|
| Ordered source bindings | `contracts/x.recall_pool.campaign.request.v1.schema.json` |
| Per-wave target, prompt, execution-context and strategy bindings | `contracts/x.recall_pool.campaign.wave_request.v1.schema.json` |
| Technical ceilings and non-enforcing stop policy | `contracts/x.recall_pool.campaign.policy.v1.schema.json`; `configs/recall_pool_campaign_policy.v1.json` |
| Raw-session replay receipt | `contracts/x.recall_pool.campaign.wave_mechanical_receipt.v1.schema.json` |
| Persisted merged artifact | `contracts/x.recall_pool.campaign.result.v1.schema.json` |
| Runtime validation and deterministic merge | `src/x_first/recall_pool_campaign.py`; `src/x_first/recall_pool_schema.py` |
| Private atomic CLI | `scripts/merge_recall_pool_campaign.py` |
| Regression suite | `tests/test_recall_pool_campaign.py` |

Every generated request, policy, upstream wave request, mechanical receipt and result executes against a no-dependency
Draft 2020-12 subset at runtime. Handwritten validators additionally enforce cross-field semantics that JSON Schema
cannot express, including X URL/author binding, exact hashes, state-summary recomputation and identity reverse-index
recomputation.

## Evidence and source-of-truth matrix

| Field | Source of truth | Never promoted from |
|---|---|---|
| Target, wave order, source paths | SHA-bound campaign request | filenames or model prose |
| Wave target/result version/prompt/model/session/request | SHA-bound upstream wave request plus replayed sources | operator memory |
| Completed native-X calls and distribution | replay of `updates.jsonl` with exact start/completed pairing | result self-report |
| CLI turn completion | first `turn_started` and last `turn_ended/outcome=completed` in `events.jsonl` | final text |
| Wave result body | terminal JSON reconstructed from ordered assistant chunks | result file plus manifest hash alone |
| Provider `end_turn` and token usage | unavailable in these CLI ledgers | CLI `turn_ended` |
| Model candidate/evidence/query/tool counts | model result, diagnostics only | mechanical namespace |
| Parsed candidates | strict result parser | model count fields |
| Candidate identity key | casefolded model-reported X handle, `model_mediated_unverified` | display name, Bio, name inference or reported platform ID |
| Reported platform ID | model-mediated diagnostic reverse index, always explicitly unverified | automatic person/handle merge or stable-ID claim |
| Temporal lab/pretraining state | model-reported wave observations, explicitly unverified | last-write wins, evidence-supported resolution or confidence precedence |
| Evidence-supported state | unavailable until raw source payloads bind dimension plus asserted value | model excerpts, model URLs or `supports` labels |
| Stop suggestion | comparable-wave call productivity | response length, candidate count or within-wave ratio |

### Raw-session replay

The direct API accepts bytes, not caller-supplied counts or caller-supplied receipt hashes. For each wave it reopens,
hashes and replays:

- `summary.json`
- `updates.jsonl`
- `events.jsonl`
- `chat_history.jsonl`
- `system_prompt.txt`
- `prompt_context.json`

Replay requires exactly one system chat message whose UTF-8 bytes equal `system_prompt.txt`. A v3 upstream request
also precommits the canonical hash of every ordered non-system chat row visible to the model or user, including all
user content outside the prompt envelope and any prior assistant context. An extra user row or text item therefore
fails; legacy requests without this binding are retained only as
`operator_asserted`. Replay binds session UUID, request UUID, model, tracked user prompt, prompt context, allowed
native-X tool registry, strict tool arguments, unique
provider/call IDs, exact start/completed sets and CLI terminal state. Every `agent_message_chunk` must bind the same
session and prompt/request ID and have ordered, unique chunk/event metadata. The replay concatenates chunk text in
ledger order plus the CLI's final LF, extracts exactly one terminal JSON object, and canonical-compares that object to
the wave result. A coherently changed result and manifest SHA therefore still fail unless the assistant actually emitted
that result. The receipt binds the raw assistant-output SHA, terminal canonical JSON SHA,
`assistant_output_binding_status=terminal_json_canonical_match`, the terminal object's exact UTF-8 byte start/end,
and the assistant chunk ordinal plus updates-ledger index containing each boundary. The chunk containing the terminal
JSON's opening `{` must occur strictly after every counted native-X start and completion update. A JSON object that
starts before tool use and merely finishes in a later assistant chunk fails closed; checking only the final chunk is
not sufficient.

The receipt states `provider_terminal_verified=false` and `provider_terminal_usage_verified=false`; it never upgrades
CLI telemetry into provider proof. The replayed `rawOutput` contains call identity and arguments but no returned X
payload. Consequently `source_payload_replay_status=unavailable_model_mediated_only`: handles, profile URLs, reported
IDs, Bios, excerpts, temporal states and evidence records remain `model_mediated_unverified` even when structurally
valid and repeated across waves.

Wave 2+ legacy runs supplied prior handles through CLI `--rules`. Their system prompt is hash-bound, but the exclusion
set was not emitted as a separate canonical machine-readable artifact. Those waves must therefore declare
`prior_exclusion_binding_status=system_prompt_hash_bound_operator_asserted`; their request-context replay status is
`operator_asserted`, not `complete`. This is enough to preserve/merge candidates, but insufficient for a formal stop
comparison. A future runner should emit a canonical prior-handle digest and exact membership count as its own bound
source.

## Merge and identity semantics

The manifest order is authoritative. The merge:

1. replays and validates every source before trusting mechanical counts;
2. de-duplicates only capitalization variants of the same X handle;
3. preserves model-reported profile URLs, IDs, Bio excerpts, wave IDs and every lab/pretraining state observation;
4. labels candidate identity, profile URL, ID, Bio, state and evidence excerpt fields
   `model_mediated_unverified`;
5. always emits empty evidence-supported values, a null evidence-supported resolution and
   `evidence_support_status=model_mediated_unverified` while source payload replay is unavailable;
6. de-duplicates evidence records globally while separately counting candidate-to-evidence associations;
7. quarantines an evidence association whose declared author does not match the author encoded in its X URL, so it
   cannot be retained; all other malformed evidence fails the wave closed;
8. normalizes each model-reported support label to `{dimension, asserted_value, source_status}`. Legacy labels have a
   null asserted value. A future source-bound evidence lane must supply both the dimension and asserted value and pass
   a separate raw-payload binding before it can influence a state;
9. keeps two handles as two candidate rows even when they share a model-reported platform ID; a diagnostic reverse
   index emits a
   review-required, reversible handle-history proposal with `auto_merge_authorized=false`.

All current/historical combinations across the two independent axes remain in the recall pool. This lane does not
select the business precision tranche.

## Stop semantics

The only stop metric is:

```text
new unique handles / replayed completed native-X calls
```

The latest wave may be compared only with waves that have complete user-context replay, a versioned full strategy
definition, precommitted runner-bound family attribution, and the same strategy ID, strategy digest, query-family set,
per-family completed-call counts and mechanically replayed per-family call profiles. Each profile compares tool name,
`limit`/`count`, mode, semantic threshold, thread argument shape and count. The strategy digest is:

```text
SHA256(canonical JSON {
  comparability_rule_version: versioned_strategy_and_precommitted_family_call_profile.v2,
  strategy_id,
  strategy_definition: {
    schema_version: x.recall_pool.campaign.strategy_definition.v1,
    query_families: ordered complete family definitions
  }
})
```

Before execution, the runner hashes each planned call from its zero-based completion ordinal, tool name and exact
normalized arguments, maps every planned-call hash to one family, and binds that map plus the strategy digest as
`precommitted_runner_family_attribution.v1`. The same attribution-plan digest must be present in the raw
`prompt_context.json`. Replay reconstructs the planned-call hashes and derives the family call profiles. Legacy
post-hoc call-identity maps never become comparable evidence.

The configured advisory requires a minimum number of comparable waves, a low latest productivity, a material
productivity decline and non-increasing lookback. A matching strategy label with a different call mix or call shape is
not comparable. In particular, a 50/1/50 completed-call sequence leaves only two comparable waves and must remain
`insufficient_proof`.

If any proof is missing, `evaluation_status=insufficient_proof` and `recommendation=continue_expansion`. The old
within-wave `new / unique` ratio is intentionally absent: with prior handles excluded it is often mechanically 1.0 and
does not measure platform marginal yield. Even a valid plateau only produces
`consider_stopping_after_manual_review`; it cannot cancel another Grok wave.

## Private atomic execution

The manifest binds the result, upstream request, tracked prompt and raw-session directory for every wave. Manifest,
result, upstream request, persisted result and every raw-session file must be current-user-owned regular files with
exact mode `0600`; their private containing directories and each raw-session directory must be exact mode `0700`.
Hardlinks, symlinks, traversal and unexpected symlink ancestors fail closed. Every prompt must be mode `0600` in a
private `0700` directory. The policy's public-prompt allowlist is deliberately fixed to `[]`; generic, bare-handle and
Bio-bearing `0644` prompts all fail. The repository policy itself may remain tracked `0644` because it contains no
candidate data.

```bash
install -d -m 700 /private/path/x-first-campaign-run

PYTHONPATH=src python3 scripts/merge_recall_pool_campaign.py \
  --manifest /private/path/x-first-campaign-run/manifest.json \
  --policy configs/recall_pool_campaign_policy.v1.json \
  --output /private/path/x-first-campaign-run/result.json

PYTHONPATH=src python3 scripts/merge_recall_pool_campaign.py \
  --manifest /private/path/x-first-campaign-run/manifest.json \
  --policy configs/recall_pool_campaign_policy.v1.json \
  --validate-existing /private/path/x-first-campaign-run/result.json
```

The output directory must be current-user-owned and mode `0700` or stricter. Under a persistent `0600` file lock, the
writer scans directory entries using a literal destination prefix and removes only exact 32-hex orphan temps, writes a
random same-directory `0600` temp, flushes and `fsync`s it, publishes
with a no-replace hard link, `fsync`s the directory, removes the temp, and `fsync`s again. A crash can leave an orphan
temp, but never a partial final artifact. Re-running against an existing final file fails without replacing it.
`--validate-existing` replays every source and requires canonical equality with the persisted result.

CLI failures return a stable redacted error and never echo queries, posts, Bios, handles or private paths.

## Read-only seven-wave replay snapshot (2026-07-14)

No provider call was made. The owner-only v3 materialization replayed the seven existing local wave results and raw
Grok CLI sessions into a new no-replace private artifact; a second read-only validation reproduced it, yielding:

- 7 waves, 99 candidate rows, 98 unique handles;
- new unique handles by wave: `29, 12, 6, 20, 25, 5, 1`;
- 702 completed native-X calls: 417 keyword, 43 semantic, 233 user and 9 thread calls;
- 275 raw model-mediated evidence associations, 274 structurally valid associations, 1 quarantined author/URL
  mismatch;
- 273 unique evidence records and 274 unique candidate/evidence associations;
- all 7 reconstructed assistant outputs had `terminal_json_canonical_match`, and each terminal JSON start chunk was
  the first update after the last native-X start/completion event;
- all candidate, Bio, ID, excerpt, state and evidence values remain `model_mediated_unverified` because no raw tool
  source payload was captured;
- stop evaluation remains `insufficient_proof`, recommendation `continue_expansion`, because the seven legacy waves
  lack complete user-context binding, versioned strategy definitions and precommitted runner-bound family attribution.

This snapshot proves the generic replay/merge path over real data; it is not a formal review GO and does not authorize
another live batch, product write or outreach action.

## Validation commands

```bash
PYTHONPATH=src python3 -m unittest -v tests.test_recall_pool_campaign
python3 -m py_compile \
  src/x_first/recall_pool_campaign.py \
  src/x_first/recall_pool_schema.py \
  scripts/merge_recall_pool_campaign.py \
  tests/test_recall_pool_campaign.py
```

The 20-test focused suite covers strict schema execution, complete user-chat/system/assistant/result binding, forged
result+manifest
rejection, split-terminal-before-tools rejection, raw source/session/model/prompt/context/terminal binding, model vs.
mechanical count drift, global query
uniqueness, evidence records vs. associations, evidence quarantine, missing evidence support, stable-ID reverse
conflicts, target/strategy relabel prevention, persisted replay, 1,500 handles without a business cap, operational kill
ceilings, versioned strategy and runner-bound family call-profile comparisons, 50/1/50 fail-closed behavior,
model-mediated provenance, owner-only PII boundaries, all-public-prompt rejection, literal orphan cleanup, atomic
no-replace publication and replay validation.
