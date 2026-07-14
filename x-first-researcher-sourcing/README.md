# X-first Researcher Sourcing

This standalone sibling explores an X-first, public-professional evidence lane for AI researcher discovery. Its product
goal is compatible with `sourcing-ai-agent`, but its discovery owner, provisional identity, and artifact lifecycle are
separate so X can be evaluated without changing LinkedIn-first canonical person ownership.

## Current status

`fixture_default + bounded_canaries + no_scale_claim` — fixture validation still performs no Grok, X, provider,
model, credential, network, database, CRM, export, or outreach call. The one approved Stage 1 Grok/X attempt was
executed and failed closed before any recognized X-search call (`unknown` tool event); its approval is consumed and it
did not prove native-X capability. A separate chshapi relay canary now tests only whether the exact
`gpt-5.6-luna` model can review one synthetic `.invalid` Bio through the semantic-v2 contract; independent review and
its one-shot live result are still pending. No live post, Bio, profile, researcher, or batch-search result has been
accepted.

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

The population is defined only by current lab affiliation and `PRETRAIN_CORE` or `PRETRAIN_ADJACENT` professional
evidence. Public professional bios, posts, mentions, and one-hop graph edges may supply bounded affiliation or technical
evidence, but they can never be used to infer or proxy ethnicity, nationality, or any protected trait. Display names
never drive selection. Directly observed language or regional-professional ecosystem terms may open a verification
lead for broader evidence retrieval, but cannot establish eligibility, ranking, region experience, or identity.

## Artifact boundary

The versioned output contract is `x.grok.collection.v1`. A future adapter may read this artifact, but the sibling does
not import `sourcing_agent`, and fixture output cannot write canonical person/evidence/assertion/CRM state.

## Capability truth table

| Capability | Current state |
| --- | --- |
| AI-native Grok/X retrieval | Intended transport is hosted `x_search`; installed CLI support is evidenced offline, live access is not yet proven |
| Full Post metadata | Stage 1 requests stable post id, URL, author id, timestamp and a bounded excerpt; it deliberately does not retain a full body |
| Bio and mention enrichment | Fixture-only deterministic baseline v1.5 separates observed Chinese content, full-window positive-grammar China-ecosystem leads, exact positive current/previous organization proposals and graph edges. Its complete regex-AST and template-specific continuation mapping are policy/hash pinned, and the loaded grammar interpreter is implementation-digest bound. It is a conservative regression oracle/guardrail, not the recall classifier; the Luna-native semantic-review v2 lane is the primary open-text path. Both reject native/live profile claims until a separately reviewed profile-capability trust root exists |
| Region-experience classification | Hardened evidence-backed classifier is committed; non-author follow-up review is still pending |
| Multiple query tasks | Synthetic Stage 2 fixtures cover eight query families; no live scheduler exists yet |
| Large asynchronous search | Not implemented. Official xAI Batch/Responses APIs are the planned scale transport and require supported API credentials |

Display names and handles are retained as raw alias/history, not treated as proof of a real name or region. A
subject-authored Bio may produce separate proposed evidence: explicit role/organization mentions, prior-affiliation
mentions, observed language, and China-ecosystem activity such as a declared public channel. Only explicit physical
work/education/research/residence evidence can support a physical region-experience label; ecosystem/language evidence
remains a verification lead and is measured separately for incremental recall and false positives.
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
```

Regenerate the deterministic fixture only when the contract intentionally changes:

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python scripts/generate_openai_fixture.py --write
```

## Next gate

The consumed Stage 1 Grok/X attempt is recorded in
`docs/live-evidence/2026-07-14-stage1-grok-x-handshake.md`; a retry or CLI tool-family compatibility probe requires a
new owner decision. The separate Luna relay path is specified in `docs/LUNA_RELAY_LIVE_CANARY_CONTRACT.md` and cannot
prove X/profile access. A stable author platform id remains a hard prerequisite for any Stage 2 owner review. Live
researcher mapping remains `NO-GO` until that later decision and contract are complete. Workflow evaluation and
champion/challenger rules are defined in `docs/X_FIRST_EVALUATION_CONTRACT.md`.
