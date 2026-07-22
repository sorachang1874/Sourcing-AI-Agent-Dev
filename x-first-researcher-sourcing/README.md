# X-first Researcher Sourcing

> Status: Current package README (slimmed 2026-07-22; frozen narrative in
> docs/README_STATUS_ARCHIVE_2026-07-22.md). 当前工作状态以 workspace 快照为准：
> [../PROGRESS.md](../PROGRESS.md) · [../NEXT_TODO.md](../NEXT_TODO.md)；包规则见
> [AGENTS.md](AGENTS.md)；judge/导出脚本注册表见
> [../sourcing-ai-agent/scripts/README.md](../sourcing-ai-agent/scripts/README.md)。

This standalone sibling explores an X-first, public-professional evidence lane for AI researcher discovery. Its product
goal is compatible with `sourcing-ai-agent`, but its discovery owner, provisional identity, and artifact lifecycle are
separate so X can be evaluated without changing LinkedIn-first canonical person ownership.

## Current status (bounded summary)

> Full status narrative (frozen 2026-07-22): [docs/README_STATUS_ARCHIVE_2026-07-22.md](docs/README_STATUS_ARCHIVE_2026-07-22.md).
> 后续状态更新写 workspace [../PROGRESS.md](../PROGRESS.md)，不再回填本节。

- Mode: `fixture_default + adaptive_live_exploration + no_promotion_claim` — fixture
  validation performs no Grok/X/provider/model/network/DB/CRM/export call.
- Seven-wave bounded campaign: 702 native-X calls, 99 candidate rows / 98 unique
  handles; stop result `insufficient_proof / continue_expansion` (not exhausted).
- v5 private replay binds terminal JSON to source chunks (pinned non-author review
  `5005c75`: P0=0/P1=0/P2=1/GO). Bios/IDs remain model-mediated: hydration, product
  promotion, and supported API batching stay **NO-GO**.
- Luna judge: v1 frozen to semantic v2.1; receipt-first v2 (semantic v2.2) is
  offline-implemented, not yet executed live, requires independent GO.
- The 2026-07-20/21 production judge waves (GDM/TML/OpenAI etc.) ran in the
  sourcing-ai-agent pipeline via the committed script chain (registry:
  [../sourcing-ai-agent/scripts/README.md](../sourcing-ai-agent/scripts/README.md)).

## Population boundary

The discovery population is defined by two independent professional-evidence dimensions: target-lab affiliation
`current|historical` and pretraining experience `current|historical`. All four supported combinations remain in the
experience-recall pool; only evidence-complete `current/current` is in the default business precision tranche.
Ambiguous or unsupported dimensions remain explicit bounded hydration cases. Public professional bios, posts,
mentions, and one-hop graph edges may supply bounded affiliation or technical
evidence, but they can never be used to infer ethnicity, nationality, or another protected identity. Display names
never drive selection. Within that base population, a separately governed high-recall verification queue may treat
subject-owned China digital-ecosystem professional activity as a `strong_proxy` and Chinese-language professional or
technical content as a `weak_proxy` for China and Asia professional experience. These proxies do not establish
physical location or identity and do not themselves authorize final ranking, eligibility, or outreach.

## Artifact boundary

The versioned output contract is `x.grok.collection.v1`. A future adapter may read this artifact, but the sibling does
not import `sourcing_agent`, and fixture output cannot write canonical person/evidence/assertion/CRM state.

## Capability truth table

| Capability | Current state |
| --- | --- |
| AI-native Grok/X retrieval | Proven across seven bounded CLI sessions: 702 raw native-X calls; 98 unique model-mediated leads |
| Full Post metadata | Stage 1 requests stable post id, URL, author id, timestamp and a bounded excerpt; it deliberately does not retain a full body |
| Bio and mention enrichment | The merged campaign retained model-mediated Bio text for 95/98 handles and 274 valid candidate/evidence associations; none is a source-bound profile/Post capability claim. The isolated reported-text semantic contract can deterministically adjudicate supplied model outputs into terminal strong/weak/none verification-queue proposals without upgrading provenance; a real 95-item Luna batch runner is not implemented. Luna semantic v2 remains the source-bound open-text path after trust-bound hydration |
| Candidate value segmentation | Configured target-lab affiliation × pretraining-experience matrix preserves current/historical recall; current/current is the default precision tranche |
| Region-experience classification | Explicit physical-region classifier stays separate; semantic v2.2 adds unverified professional-experience proxy roll-up |
| Multiple query tasks | Seven adaptive waves exercised keyword, semantic, user and thread search with prior-handle exclusion and strategy diversification; deterministic source replay and merge are author-complete and independent review remains pending |
| Large asynchronous search | Interactive adaptive expansion is empirically viable; a supported durable xAI Batch/Responses scheduler is not implemented and still requires API credentials and separate promotion review |
| Source-bound field hydration | Stage 2A offline replay semantics are executable; current CLI-like metadata-only traces fail closed, and a supported live payload-returning transport remains unimplemented |

Display names and handles are retained as raw alias/history, not treated as proof of a real name or region. A
subject-authored Bio may produce separate proposed evidence: explicit role/organization mentions, prior-affiliation
mentions, observed language, and subject-owned China-ecosystem professional activity. Only explicit physical work/
education/research/residence evidence can support a physical region-experience label. Ecosystem/language proposals
feed the distinct strong/weak professional-experience proxy policy and remain unverified until human adjudication.
The executable Bio v1 accepts only canonical lowercase-scheme/lowercase-host `.invalid` fixture profiles, null tool
receipts, extractor versions of at most 100 characters, and canonical policy v1.5. It has
no negative-phrase denylist: ownership and current/previous affiliation must full-match one versioned positive grammar
after NFKC normalization. Any unconsumed prefix, suffix, second clause, arbitrary parenthetical note, or `Cc`/`Cf`
character fails closed. Ownership continuations are bound to each template, so the account-ending
`my {alias} account` form cannot consume a second identifier. A role is accepted only when it equals the exact bounded
role span parsed from the policy's closed role alternatives; open role text, handles, handle prefixes, pure
prepositions, and free substrings are invalid. This deliberately conservative
champion does not chase recall by adding aliases or exceptions; Luna v2 owns semantic recall. Unicode-scalar plus
depth/node preflights keep malformed nested JSON terminal-total. V1 cannot be relabelled as proof that Grok returned a
Bio or stable account identity.
See `docs/X_SEARCH_TRANSPORT_AND_SCALE_DECISION.md` for the CLI/API split, Bio/mention evidence model, batch topology,
and scale gates. See `docs/PROFILE_BIO_SIGNAL_CONTRACT.md` for the executable offline Bio proposal boundary.

Compact discovery and profile hydration now share one operator raw-session replay boundary over the same six Grok
artifacts used by recall-pool evidence. Projection builders derive source/transcript hashes, exhaustive native-X
start/completion pairs, event digests, and the post-tool contract-valid terminal byte range from immutable bytes;
strict terminal parsing rejects duplicate keys, and closed per-tool argument validation rejects malformed calls.
The shared shape requires the unique user update first, before thought/tool/completion/assistant updates, and applies
a depth-64/node-50,000 lexical preflight before recursive assistant JSON decoding.
An immutable owner precommit binds expected session/request/model/reasoning-effort values, exact prompt bytes, the
exact five-row user-visible chat prefix bytes/digest, and system/context hashes. The final chat file is not called a
precommit: its model-generated suffix is retained and hashed only as raw transcript evidence. A versioned closed
raw-session registry accepts only the five observed update kinds, validates the generic `XSearch` start envelope,
binds its call id/title to the later concrete native-X completion, and reconciles post-prefix reasoning/tool/assistant
chat rows with the ledger and selected terminal. The event ledger separately enforces the observed
start/loop/phase/first-token/completed-turn order. Unknown update, event, or chat-row shapes fail closed;
operator execution facts are retained separately and cannot be copied from a forged result receipt during replay.
Caller-authored receipt dataclasses are not accepted without those bindings and replay. The compact union routes
same-handle provisional evidence into a generalized one-or-more-candidate unresolved sidecar without rewriting a
stable lead or adding hydration work, requires explicit lookup-alias resolution for renamed stable ids, and emits
closed hydration identity tuples. The normal hydration handoff does not accept the serialized union mapping: it
retains every typed compact projection and its immutable raw sources in a typed merge envelope, replays every source,
reruns the deterministic merge, and compares the complete union and merge summary before deriving hydration work.
The envelope's union/summary hashes are diagnostics rather than caller authority; recomputing those hashes cannot
make a deleted or rebound identity sidecar acceptable. Before replay, the boundary snapshots only exact built-in
plain-JSON values and exact dataclass/tuple fields; subclasses, Boolean/integer aliases, non-finite numbers, and cyclic
or over-depth structures fail before caller equality or copy hooks can run. Result and summary comparisons use
canonical bytes built from those operator-owned snapshots, so a caller mapping cannot mutate the fresh reconstruction.
Hydration fails closed on a stable platform-id
mismatch and returns candidate-free stable errors for malformed nested envelopes. See
`docs/GROK_COMPACT_DISCOVERY_AND_PROFILE_HYDRATION.md`.

## Commands

From this folder:

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m unittest discover -s tests -v
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m x_first.contracts
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m x_first.capability_probe
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m x_first.profile_bio_signals fixtures/profile_bio_evidence_fixture_v1.json --policy configs/profile_bio_signal_policy.v1.json
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python scripts/generate_openai_fixture.py --check
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python scripts/generate_capability_probe_fixtures.py --check
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m unittest tests.test_luna_live_canary -v
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m unittest tests.test_luna_live_canary_v2 -v
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m unittest tests.test_grok_cli_exploration -v
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m unittest tests.test_compact_grok_discovery -v
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m unittest tests.test_grok_profile_hydration -v
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m unittest tests.test_reported_profile_text_semantic -v
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python scripts/generate_stage2_field_capability_fixture.py --check
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m unittest tests.test_stage2_field_capability -v
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m x_first.source_neutral_mapping
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m unittest tests.test_source_neutral_mapping -v
```

Regenerate the deterministic fixture only when the contract intentionally changes:

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python scripts/generate_openai_fixture.py --write
```

The Grok CLI exploration evaluator now uses query-policy descriptor/registry v2: raw queries plus one random 256-bit
key and nonce stay only in an owner-only `0600` receipt, while tracked files retain domain-separated HMAC commitments.
The immutable registry now binds an append-only issuance lineage and rejects cross-run reuse of either key or nonce id.
The offline migration requires an owner-UID `0700` non-symlink root, serializes prepare/evaluate/purge under one lock,
binds canonical source/target hashes in a durable receipt, fsyncs files and directories, and emits a deletion tombstone.
Evaluation/hydration v1 has no 25-candidate, 32-call, or five-task business cap; byte/node/depth/deadline envelopes are
technical safety limits governed by one absolute monotonic deadline, and duplicate reported numeric X IDs are
quarantined from unique/Recall/Precision metrics. Base-axis queries/supporting spans also pass a governed multilingual
protected-category/value boundary; China/Asia professional-experience proxy evidence remains a distinct lane.
See `docs/X_FIRST_EVALUATION_CONTRACT.md` for migration, replay, deletion, and Git-history privacy residuals.

## Next gate

The failed-closed Stage 1 runner, initial eight-lead experiment, and seven-wave adaptive recall campaign are separately
recorded under `docs/live-evidence/`. Native search and high-recall multi-strategy expansion are empirically proven;
profile-field completeness, original-source owner-only retention, researcher role/function adjudication, replayable
Post bodies, and durable task accounting are not. Exploration is paused at 98 unique leads to move capacity to
hydration, not because a volume cap fired or formal exhaustion was proved; the replay evaluator remains
`insufficient_proof / continue_expansion`. The offline reported-text semantic contract and adjudicator are ready for
supplied model outputs while preserving that trust level; they have not run the 95 real model-mediated texts and do
not include a Luna transport. They cannot replace the next gate of source-bound account/Bio/Post hydration.
The Stage 2A offline contract defines what source binding must prove. The reviewed OAuth Grok CLI runner proves bounded
native-X execution and session-argument reconciliation, but Grok CLI 0.2.101 still does not retain native-X result
bodies or query-to-lead attribution. The immediate engineering gate is independent review of the compact discovery
contract, deterministic multi-shard union, ledger-gated profile-hydration contract, and runner recovery repair. The
adaptive replay of the 49 rejected profile inputs may continue as explicitly labelled exploration; neither the
96-lead diagnostic union nor that replay is a formal promotion gate. A future supported transport that exposes
native result payloads is still needed for replayable source binding and true per-query yield. Provider-costing batch
promotion and precision/conditional-coverage measurement remain separately review-gated. Workflow evaluation and
champion/challenger rules are defined in `docs/X_FIRST_EVALUATION_CONTRACT.md`.
