# Reported profile-text semantic contract v1

> Status: pure offline, synthetic-fixture-only implementation. It performs no Luna, provider, network, credential,
> X, filesystem-write, ranking, eligibility, canonical, outreach, or product action. A future provider-costing batch
> runner requires separate contracts, owner approval, and pinned independent review.

## Purpose and trust boundary

The recall campaign currently contains model-mediated Bio-like text. That text is useful for early semantic review,
but it is not a field-level X profile receipt. This lane therefore accepts only:

```text
source_kind = grok_model_mediated_unverified_text
source_trust = model_mediated_unverified
```

It must never be wrapped in a `.invalid` URL and passed to profile-Bio semantic v2.2. V2.2 intentionally supports
only synthetic `profile_source_mode=offline_fixture` snapshots bound to a numeric platform user ID. Likewise, the
existing Luna v1/v2 canaries remain single-synthetic-input capability lanes and are not batch runners.

The reported-text identity is a campaign-scoped opaque candidate reference plus campaign, candidate-row, and exact
text SHA-256 values. A model-reported platform user ID is optional diagnostic data. It is always either absent or
`model_mediated_unverified`, never participates in the observation/item ID, and cannot establish X account or person
identity.

## Four closed artifacts

| Contract | Owner | Boundary |
| --- | --- | --- |
| `x.reported_profile_text.observation.v1` | reported-text materializer | opaque refs, exact text/hash and permanently unverified source/ID status |
| `x.reported_profile_text.semantic.batch_request.v1` | offline batch caller | ordered denominator, campaign/list hashes, closed semantic policy and technical kill ceilings |
| `x.reported_profile_text.semantic.model_output.v1` | untrusted synthetic model-output fixture | exact item/text bindings, closed semantic vocabulary and source spans |
| `x.reported_profile_text.semantic.item_review.v1` | deterministic adjudicator | recomputed proposal IDs, excerpts/hashes, policy roll-up, terminal status and zero authority |

`src/x_first/reported_profile_text_semantic.py` is the only runtime owner. It accepts no provider or transport and
does not write artifacts. `adjudicate_batch(...)` returns a runtime-only closure object; it is deliberately not a
fifth persisted contract. A future live runner must define its own batch result, approval, execution receipt,
retention and deletion contracts rather than treating this in-memory closure as provider evidence.

## Semantic reuse and business meaning

The lane reuses the exact profile semantic v2.2 proposal vocabulary, relation states, reason codes, closed reasons,
confidence states and professional-experience proxy policy. It changes the evidence basis to
`model_mediated_unverified_text_only` so a valid semantic span cannot masquerade as an observed X profile.

The policy-derived verification-queue roll-up remains:

| Text proposal | China | Asia | What it does not mean |
| --- | --- | --- | --- |
| subject-owned China digital-ecosystem professional activity | `strong_proxy` | `strong_proxy` | physical presence, nationality, ethnicity or confirmed account ownership |
| Chinese-language professional/technical content | `weak_proxy` | `weak_proxy` | physical presence, fluency, nationality or ethnicity |
| explicit professional-region statement | no proxy from this roll-up | no proxy from this roll-up | verified physical-region experience |
| claimed organization affiliation | no proxy from this roll-up | no proxy from this roll-up | confirmed employment |

Strong/weak/none can route only a separately governed high-recall verification queue. The review fixes source-bound
profile acceptance, stable identity, physical-region inference, protected-identity inference, discovery/ranking,
eligibility, canonical employment/write and outreach authority to false.

## Deterministic adjudication

Every model proposal must bind one exact Unicode code-point span and excerpt. Runtime checks the closed
`proposal_type + relation_state + reason_code + reason` combination, source basis, confidence and duplicate semantic
identity. It then recomputes:

- candidate reference and observation ID from campaign, candidate-row and text hashes;
- item request ID from batch ID, observation ID and text hash;
- observation, model-output, excerpt and proposal hashes;
- proposal ID from observation/text plus semantic identity;
- strong/weak/none policy contributions and ordering; and
- the complete item review from the batch request, observation, model output and canonical proxy policy.

Missing and malformed model outputs produce a closed `failed` item review with no proposals. They are not converted
to `none`. Invalid or over-ceiling model output is not serialized and hashed again; its failed review records a null
model-output digest. Unknown output keys are rejected rather than attached to the batch. Model output validation also
requires the supplied observation to be the unique strict-equal member of the bound batch.

## Denominator and technical ceilings

One batch observation produces exactly one terminal review. `completed + failed = terminal_reviews = denominator` is
recomputed over the ordered input list. The public validator always performs full deterministic recomputation; only
the adjudicator's private construction helper performs the non-recursive structural check. This prevents failed or
absent outputs from disappearing from proxy rates or a caller bypassing source replay.

After one complete batch validation, batch adjudication builds one unique observation-ID index and performs exactly
one strict-equality lookup per item. It does not rescan or revalidate the complete batch for every model output, so the
internal batch path is linear in observations plus model-output size. One-off public model-output validation still
builds its own trusted index and therefore preserves the same strict membership check without accepting caller-owned
index state.

There is no business candidate, observation or success limit. The technical ceilings protect local validation from
malformed input: 10,000 items, 2,000 characters per text, 20,000,000 total text characters, 32,000,000 canonical
batch bytes, 262,144 canonical model-output bytes, 64 levels, 500,000 JSON nodes, and 12 proposals per item. Hitting a
ceiling invalidates the request; it does not count as a completed review. The regression suite adjudicates 150
synthetic observations to prove that the earlier 25-person exploration shape is not encoded as a product cap.

## Future source-bound upgrade

A later X profile capability must separately bind stable numeric platform user ID, handle history, canonical profile
URL, exact Bio bytes/hash, observation time, content version and a field-level native-X call/result receipt. A Grok
summary alone is insufficient.

Source upgrade creates a new artifact; it never mutates an existing reported-text review:

- exact text hash plus identical semantic assets may link and deterministically replay the prior semantic proposal
  under a new source receipt;
- changed text or unavailable retained output requires a new source-bound semantic request; and
- reported-ID/source-ID disagreement enters reversible identity conflict review and never auto-merges accounts.

Even a source-bound semantic proposal remains distinct from adjudicated physical-region evidence and confirmed
employment.

## Future live boundary

This slice does not authorize Luna. A future runner must add a distinct batch approval domain, one catalog handshake,
at most one POST per exact manifest item, bounded and approval-pinned concurrency/deadline/token ceilings, no retry or
fallback, observed per-item receipts, terminal-total partial failure, private raw artifacts, TTL deletion, crash-safe
unknown-outcome handling, and provider-reported or explicitly `unreported` cost. It must not reuse the single-canary
approval or result arithmetic.

Before any provider call, that new contract-heavy slice requires a pinned non-author `GO`. Green tests here prove only
offline semantics.

## Validation

From `x-first-researcher-sourcing/`:

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python \
  -m unittest tests.test_reported_profile_text_semantic -v

../sourcing-ai-agent/.venv/bin/ruff check \
  src/x_first/reported_profile_text_semantic.py \
  tests/test_reported_profile_text_semantic.py

python3 -m py_compile \
  src/x_first/reported_profile_text_semantic.py \
  tests/test_reported_profile_text_semantic.py
```

The fixtures are synthetic, contain no X/profile URL, and exercise a China digital-ecosystem strong proxy, Chinese
professional-content weak proxy, physical-region proposal, historical affiliation proposal and no-context result.
