# Source-neutral mapping controller v1

## Outcome and boundary

The v1 controller freezes the empirically useful part of the large-lab calibration without hard-coding one lab,
candidate, provider, or model. It is provider-free: it plans Wave P, replays retained artifacts, derives frontier
queues, evaluates structural convergence, and emits candidate-free metrics. It never launches Grok/X/Luna and never
writes product state.

The lab descriptor supplies `lab_id`, display/affiliation/project aliases, exact official handles, and the X URL host.
The candidate manifest independently freezes target-lab-affiliation and pretraining-experience priors. Current and
historical values on either axis remain valuable; neither axis is derived from the other.

## Calibrated default, not a business cap

The default policy uses an observed grain of three candidates and two champion calls per candidate:

1. authored direct-core aliases in `Top`;
2. authored data/objective aliases in `Top`.

Every manifest row is planned exactly once. The final batch may be smaller. The values `3` and `2` are versioned,
configurable empirical scheduling grain, not candidate/reference/business limits. A rejected batch commits zero rows,
then splits `3 -> 2 + 1`; larger batches use a binary split. Tests cover more than 100 candidates and references.

Each call is precommitted with candidate/batch/global ordinal, expected author, alias group, tool name, and exact
arguments. The prompt must carry the exact `MAPPING_BATCH_SHA256=<sha256>` line. The terminal is literal flat text:

```text
BEGIN_CALL_0001_URLS
https://x.invalid/fixturemap001/status/10001
END_CALL_0001_URLS
```

There is one ordered block for every call. Only exact author-bound status URLs are accepted. Stable Post IDs dedupe
within a candidate; one ID appearing under two candidates rejects the whole batch.

## Raw replay authority

A normalized ledger or flat terminal supplied by a caller is never execution proof. An accepted receipt is produced
only after `replay_grok_operator_session` replays the canonical six immutable files:

- `summary.json`
- `updates.jsonl`
- `events.jsonl`
- `chat_history.jsonl`
- `system_prompt.txt`
- `prompt_context.json`

That replay binds the typed Grok precommit, exact prompt, session/request identity, every tool start/completion and
argument, chat/update/event order, and the exact post-tool literal terminal bytes. Operator input/schema/prompt,
stdout/stderr, copied updates, and process receipts may be retained separately, but they are not substitutes for this
six-file authority. Missing or invalid raw replay returns `operator_projected_unverified`, zero committed references,
and zero proven execution.

The retained private Wave P calibration controller is still exploratory and its URLs remain
`model_mediated_unverified`. It did not emit the typed `GrokOperatorSessionPrecommit` required by this controller, so
its historical literal-flat sessions cannot be retroactively marked `commit_allowed`. Possessing the six files alone
is insufficient: a future private explorer must precommit the prompt/session/request identity and exact batch marker
before execution, retain that typed precommit beside the canonical six files, and then use this replay path. This v1
therefore freezes the method and a compatible future input contract; it does not claim end-to-end promotion
compatibility for the already-retained calibration sessions.

## Frontier, hydration, and stopping

`returned URL count == request limit` means only a saturation lower bound. It is not evidence of completeness. A
saturated lineage expands in the configured order `topic -> mode -> time`; only saturated children continue.

Accepted stable IDs enter an exact thread-hydration queue. Only exact source-bound hydration enters the explicit Luna
input queue. Candidates with sparse authored evidence or an `ambiguous|unsupported` axis receive
`official_exact_mention`, `project_alias`, and `thread` challengers. Challenger evidence is always marked
`official_or_third_party_non_self` and can never be relabelled as self-authored evidence.

The campaign stops only when all four queues are empty and two consecutive, materially distinct strategy waves each
produce both zero new stable Post IDs and zero Luna-qualified state upgrades. Answer length, one zero-yield call, or a
model claim that the search is complete cannot stop the campaign.

## Metrics and calibration binding

The candidate-free aggregate carries four explicit denominators:

1. execution compliance / planned native-X calls;
2. stable Post-ID retrieval / raw-replay-attested completed native-X calls;
3. exact hydration / unique stable Post IDs enqueued;
4. Luna-qualified upgrades / terminal Luna reviews.

It binds policy, manifest, plan, session-receipt, hydration-receipt, and Luna-result manifests by SHA-256 and contains
no model-call count. The tracked 46-call calibration aggregate is diagnostic method evidence only: it binds the
owner-private candidate-free summary and receipt by hash, retains only counts/hashes, and explicitly does not claim a
global recall plateau. Its calibrated frontier moved `17 -> 37` over 31 authored expansion calls; the three-candidate,
six-call flat canary passed ledger/format checks and added four marginal stable references.

`private_source_binding_manifest_sha256` is not a free-form label. It is SHA-256 over the receipt's
`.source_bindings` value serialized as canonical JSON with `ensure_ascii=True`, `allow_nan=False`, sorted keys, compact
`(',', ':')` separators, UTF-8 bytes, and no trailing newline. `private_source_binding_count` must equal the array
length; the raw private receipt and summary file bytes must independently match `private_receipt_sha256` and
`private_summary_sha256`. Owners with the private files can mechanically recheck all four bindings:

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m x_first.source_neutral_mapping \
  --private-calibration-receipt <owner-private-receipt.json> \
  --private-calibration-summary <owner-private-summary.json>
```

## Offline validation

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m x_first.source_neutral_mapping
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m unittest tests.test_source_neutral_mapping -v
```
