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

The plan hash is diagnostic rather than authority. Validation reconstructs the complete plan from the validated
manifest plus policy and compares the exact typed value: batch grain and placement, candidate and call ordinals,
expected author, champion cell, alias group and aliases, tool, query, limit, and mode. Session precommit construction
reruns that reconstruction before selecting a batch, so a self-consistent rehashed caller plan cannot change the
calibrated calls.

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
and zero proven execution. When the mapping precommit itself is valid, a rejected receipt preserves the trusted
planned-call denominator; only an invalid precommit records that denominator as unknown (`null`). Frontier derivation
does not accept serialized receipt dictionaries. It consumes immutable typed projections retaining the mapping and
Grok precommits, literal terminal, lab descriptor, and all six source files, then replays them and checks exact plan
coverage again.

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

Unexecuted Wave P batches enter `pending_wave_p_queue`; raw-replay rejection enters `retry_split_queue`. Neither state
is classified as sparse evidence. Only candidates whose complete champion batch is replay-attested are eligible for
sparse/ambiguity challengers. `official_exact_mention` and `project_alias` remain seed-free strategies, while `thread`
is omitted unless at least one stable Post ID exists. Challenger evidence is always marked
`official_or_third_party_non_self` and can never be relabelled as self-authored evidence.

Accepted stable IDs use canonical positive decimal text (no zero or leading-zero aliases) and enter an exact
thread-hydration queue. Luna input accepts only a typed `x_thread_fetch`
projection that retains the six raw files and operator precommit, replays exactly one call for the queued Post ID, and
binds descriptor host, queued URL, expected author, requested/returned ID, and exact UTF-8 full-text bytes. Luna state
reviews are separately typed and bind the hydration projection and source-text digest. Their current contract is
explicitly `diagnostic_only_unattested`: the model may propose the two axis states, but it does not emit an upgrade
Boolean or transition ID and cannot change product or campaign state.

Hydration coverage is keyed by the exact queued task digest. A second projection for the same task is rejected even
when it has fresh session, request, and projection identities; a complete wave compares the exact task set rather than
projection-row counts. This prevents duplicate hydration of Post A from substituting for missing Post B.

Deterministic code reduces the full available review set to exactly one row per candidate, axis, and manifest-bound
state version. Every row binds the frozen prior, expected/reviewed evidence manifests, proposal-set digest, and
coverage. An incomplete review set has no reduced proposal; conflicting complete proposals reduce to `ambiguous`.
Because the current Luna boundary is unattested, `resolved_state` always remains the frozen prior,
`transition_status=not_authorized`, and there are zero authorized transitions. This prevents multiple Posts from being
counted as multiple candidate-state upgrades or a single model run from downgrading a prior.

`ExecutedWaveFacts` is a replayable **diagnostic predecessor projection**, not an admitted append-only campaign truth.
The public builder accepts a campaign ID and optional replayable predecessor; it does not accept prior stable IDs, an
ordinal, or a strategy payload. It recursively replays that selected predecessor and derives its ordinal, fact digest,
prior frontier, new IDs, cumulative frontier, and current fact digest. The cumulative authored frontier binds each
stable Post ID to its candidate and source author, so cross-wave reassignment fails closed. The strategy signature is
derived from the validated plan's native tool, handle-neutral query template, NFKC/casefold/whitespace-normalized,
sorted unique OR aliases, mode, limit, time window, and relationship topology. Normalized alias collisions reject
rather than silently collapsing. Plan, batch, wave, session, request, candidate, and execution identities are excluded,
so rescheduling an identical strategy cannot pretend to be a new challenger.

The diagnostic stopping helper replays the two supplied trailing facts and requires exact predecessor adjacency;
passing `[F1,F3]` while omitting `F2` fails closed. It is not campaign-global authority: two callers can still build
sibling projections from the same predecessor, and independent roots cannot see one another's proof identities. The
2026-07-17 pinned review therefore remains `NO-GO` for campaign closeout. A durable store-global append journal,
canonical head, successor compare-and-swap, global proof registry, and explicit source/consumer-bound cache projection
must land before any campaign frontier, zero-wave, or stop result is promotable.

### Durable campaign store Phase 1

`x_first.source_neutral_campaign_store.CampaignStore` now provides the durable admission substrate: one private
store-global lock, a content-addressed global journal, replay-derived per-campaign heads, successor compare-and-swap,
mutation idempotency, and a campaign-global direct-proof registry. Journal publication is the commit point; materialized
head files are repairable caches. Replay fails closed on sequence gaps, hash-chain corruption, unknown/forked
materialized frontiers, and tail rollback witnessed by a previously materialized global head. Crash tests cover both
orphan bundles before journal publication and committed journal entries before head materialization.

The store-global lock uses the private store-root directory inode as the serialization anchor and independently locks
the canonical `.store.lock` inode. After acquisition and immediately before replay or publication, both descriptors
must still bind their canonical pathnames. Replacing `.store.lock` therefore cannot open a second writer lane, and the
active writer fails closed before journal publication. Existing lock files are never permission-repaired: only an
`O_EXCL`-created lock receives `0600` umask normalization. Every store directory must be owned by the effective UID and
remain exactly `0700`; every manifest, journal, object, materialized head, temporary file, and lock must be a regular
effective-UID-owned, single-link `0600` inode. Directory link counts are not fixed because POSIX derives them from
subdirectory topology; canonical device/inode binding closes directory-path substitution instead.

Phase 1 deliberately accepts an explicit `ValidatedWaveBundle` plus caller-supplied `DirectProof` inventory. It does
not yet derive either value from raw mapping/hydration/Luna projections, and current `structural_stop` does not read the
store. Therefore the store foundation has its own scoped test/review result, but it does **not** clear the mapping
controller's promotion blocker. Phase 2 must replay `ExecutedWaveFacts` inputs inside the store lock, derive proof keys
from raw transcripts/session/request/projection/review identities, add explicit cache-consumer projections, and make
authoritative stop read only the canonical journal tail.

All six queues must still be empty and two adjacent, materially distinct strategies must produce zero new stable Post
IDs and zero **authorized** semantic transitions. Diagnostic Luna output cannot prove the latter. Therefore a campaign
with hydrated semantic evidence deliberately remains `continue_mapping` even when diagnostic coverage is complete and
all queues appear empty. Structural stopping for that lane remains unavailable until a receipt-first contract binds
approval, exact route/model, request/prompt/schema/policy, retained raw response, transport receipt, and deterministic
semantic replay. This is fail-closed, not a recall plateau claim.

## Metrics and calibration binding

The candidate-free aggregate carries four explicit denominators:

1. execution compliance / planned native-X calls;
2. stable Post-ID retrieval / raw-replay-attested completed native-X calls;
3. exact hydration / unique stable Post IDs enqueued;
4. Luna diagnostic-review coverage / exact source-bound hydrations.

The aggregate builder derives every count and binding from the exact plan plus replayed typed session, hydration, and
Luna projections; independent caller integers are not accepted. It exposes
`not_applicable|not_started|incomplete|complete` review coverage,
`semantic_transition_authority=diagnostic_only_unattested`, and an authorized transition count fixed at zero. It
enforces one projection per queued hydration task, zero propagation, and legal cross-stage cardinalities; binds policy,
manifest, plan, session-receipt,
hydration-receipt, Luna-result, and candidate-axis-reduction manifests by SHA-256, and contains no model-call count. The
tracked 46-call calibration aggregate is diagnostic method evidence only: it binds the
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

## Contract correction status

The first v1 source-neutral mapping contract was pinned for review but never promoted. The `da396e6` rereview returned
`NO-GO` with three P1 and four P2 findings. The current fixed-forward closes duplicate hydration substitution,
candidate/author frontier loss, canonical decimal identity, and commutative alias normalization; durable campaign
admission, temporal candidate-snapshot semantics, and general saturation/challenger strategy v2 remain explicit
promotion blockers. No historical exploratory artifact is reinterpreted as promoted evidence.

## Offline validation

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m x_first.source_neutral_mapping
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m unittest tests.test_source_neutral_mapping -v
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m unittest tests.test_source_neutral_campaign_store -v
```
