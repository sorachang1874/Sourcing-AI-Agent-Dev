# X-first Researcher Sourcing

This standalone sibling explores an X-first, public-professional evidence lane for AI researcher discovery. Its product
goal is compatible with `sourcing-ai-agent`, but its discovery owner, provisional identity, and artifact lifecycle are
separate so X can be evaluated without changing LinkedIn-first canonical person ownership.

## Current status

`fixture_default + live_probe_hardening + no_scale_claim` — fixture validation still performs no Grok, X, provider,
model, credential, network, database, CRM, export, or outreach call. A separate v2 runner implements one
owner-approved account-level Grok native-X capability probe, but it has not passed independent review or executed.
No live post, Bio, profile, researcher, or batch-search result has been accepted yet.

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
| Bio and mention enrichment | Designed as a later profile/mention evidence lane; no live collector or confirmation flow exists yet |
| Region-experience classification | Evidence-backed classifier exists, but its first review found source-binding gaps; follow-up is pending |
| Multiple query tasks | Synthetic Stage 2 fixtures cover eight query families; no live scheduler exists yet |
| Large asynchronous search | Not implemented. Official xAI Batch/Responses APIs are the planned scale transport and require supported API credentials |

Display names and handles are retained as raw alias/history, not treated as proof of a real name or region. A
subject-authored Bio may produce separate proposed evidence: explicit role/organization mentions, prior-affiliation
mentions, observed language, and China-ecosystem activity such as a declared public channel. Only explicit physical
work/education/research/residence evidence can support a physical region-experience label; ecosystem/language evidence
remains a verification lead and is measured separately for incremental recall and false positives.
See `docs/X_SEARCH_TRANSPORT_AND_SCALE_DECISION.md` for the CLI/API split, Bio/mention evidence model, batch topology,
and scale gates.

## Commands

From this folder:

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m unittest discover -s tests -v
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m x_first.contracts
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m x_first.capability_probe
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python scripts/generate_openai_fixture.py --check
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python scripts/generate_capability_probe_fixtures.py --check
```

Regenerate the deterministic fixture only when the contract intentionally changes:

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python scripts/generate_openai_fixture.py --write
```

## Next gate

The Stage 1 live v2 contract and runner are described in `docs/STAGE1_LIVE_CAPABILITY_CONTRACT.md`. The user approved
one bounded execution, while independent review and working OAuth remain runtime prerequisites. The runner must prove
an actual completed `x_search` tool call; generic web search does not count. A stable author platform id is an
additional hard prerequisite for any Stage 2 owner review. Live researcher mapping remains `NO-GO` until that later
decision and contract are complete. Workflow evaluation and champion/challenger rules are defined in
`docs/X_FIRST_EVALUATION_CONTRACT.md`.
