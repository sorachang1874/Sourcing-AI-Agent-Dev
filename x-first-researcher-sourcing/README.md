# X-first Researcher Sourcing

This standalone sibling explores an X-first, public-professional evidence lane for AI researcher discovery. Its product
goal is compatible with `sourcing-ai-agent`, but its discovery owner, provisional identity, and artifact lifecycle are
separate so X can be evaluated without changing LinkedIn-first canonical person ownership.

## Current status

`fixture_default + adaptive_live_exploration + no_promotion_claim` — fixture validation still performs no Grok, X,
provider, model, credential, network, database, CRM, export, or outreach call. A seven-wave bounded campaign has now
mechanically observed 702 native-X calls and retained 99 candidate rows / 98 case-insensitive unique handles. It used
no business candidate, observation, answer-length, or per-wave call ceiling; the operator paused after a current-team
frontier and a residual coverage audit fell to `5/104` then `1/70` new unique handles per raw call. A private v3 replay
recomputed all seven raw sessions, bound each emitted assistant terminal JSON and system prompt, and mapped every
terminal object's byte span to its source assistant chunks. It requires the chunk containing the terminal JSON's
opening `{` to occur after every native-X start/completion event and keeps every candidate field explicitly
`model_mediated_unverified`. The `0600`
merged artifact has SHA-256
`d8392f12011701b740f7cec9666919ad6b99a50e1b9af6b11d61cb65f7aea474`. Its formal stop result is still
`insufficient_proof / continue_expansion`, not exhausted. Exact Post bodies remain unreplayable, Bios/IDs are
model-mediated rather than source-bound, and the original `~/.grok` copies remain below the owner-only contract;
therefore hydration, product promotion, and supported API batching remain `NO-GO`. A
corrected live `gpt-5.6-luna` request also validated the intended
China/Asia professional-experience proxy semantics on four cases, but it is feasibility evidence rather than a formal
semantic-v2.2 live gate. The formal Luna canary now has a separate receipt-first v2 offline implementation: legacy v1
validation is frozen to semantic v2.1 assets, while v2 binds current semantic v2.2 and persists exact observed
route/model/payload/timing/HTTP-or-failure receipts. It has not been executed live and still requires independent GO.

The first vertical slice covers OpenAI with:

- 24 completely synthetic external accounts;
- 96 synthetic observations;
- eight explicit query families;
- stable platform user IDs and handle history;
- opaque provisional person IDs;
- raw observations and evidence proposals only;
- explicit non-exhaustiveness and zero protected-trait output.

All URLs use the reserved `.invalid` domain. No real person, handle, post, or X URL appears in the fixture.

An additional Stage 1 offline slice now validates synthetic capability-probe request/result envelopes. It remains
account-level and fixture-only: its only positive verdict is `fixture_contract_validated`, and it explicitly records
`x_native_access_proven=false`. See `docs/ARCHITECTURE.md` and `docs/STAGE1_CAPABILITY_FIXTURE_CONTRACT.md`.

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
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m unittest tests.test_reported_profile_text_semantic -v
```

Regenerate the deterministic fixture only when the contract intentionally changes:

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python scripts/generate_openai_fixture.py --write
```

## Next gate

The failed-closed Stage 1 runner, initial eight-lead experiment, and seven-wave adaptive recall campaign are separately
recorded under `docs/live-evidence/`. Native search and high-recall multi-strategy expansion are empirically proven;
profile-field completeness, original-source owner-only retention, researcher role/function adjudication, replayable
Post bodies, and durable task accounting are not. Exploration is paused at 98 unique leads to move capacity to
hydration, not because a volume cap fired or formal exhaustion was proved; the replay evaluator remains
`insufficient_proof / continue_expansion`. The offline reported-text semantic contract and adjudicator are ready for
supplied model outputs while preserving that trust level; they have not run the 95 real model-mediated texts and do
not include a Luna transport. They cannot replace the next gate of source-bound account/Bio/Post hydration.
Provider-costing batch review and precision/conditional-recall measurement remain separately review-gated. Workflow
evaluation and champion/challenger rules are defined in
`docs/X_FIRST_EVALUATION_CONTRACT.md`.
