# X-first Researcher Sourcing

This standalone sibling explores an X-first, public-professional evidence lane for AI researcher discovery. Its product
goal is compatible with `sourcing-ai-agent`, but its discovery owner, provisional identity, and artifact lifecycle are
separate so X can be evaluated without changing LinkedIn-first canonical person ownership.

## Current status

`fixture_only` — no Grok, X, provider, model, credential, network, database, CRM, export, or outreach call is implemented
or permitted.

The first vertical slice covers OpenAI with:

- 24 completely synthetic external accounts;
- 96 synthetic observations;
- eight explicit query families;
- stable platform user IDs and handle history;
- opaque provisional person IDs;
- raw observations and evidence proposals only;
- explicit non-exhaustiveness and zero protected-trait output.

All URLs use the reserved `.invalid` domain. No real person, handle, post, or X URL appears in the fixture.

## Population boundary

The population is defined only by current lab affiliation and `PRETRAIN_CORE` or `PRETRAIN_ADJACENT` professional
evidence. Public professional bios, posts, mentions, and one-hop graph edges may supply bounded affiliation or technical
evidence, but they can never be used to infer or proxy ethnicity, nationality, or any protected trait. Name, language,
region, school, and community signals are never discovery or ranking features.

## Artifact boundary

The versioned output contract is `x.grok.collection.v1`. A future adapter may read this artifact, but the sibling does
not import `sourcing_agent`, and fixture output cannot write canonical person/evidence/assertion/CRM state.

## Commands

From this folder:

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m unittest discover -s tests -v
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m x_first.contracts
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python scripts/generate_openai_fixture.py --check
```

Regenerate the deterministic fixture only when the contract intentionally changes:

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python scripts/generate_openai_fixture.py --write
```

## Next gate

The next allowed step is a bounded Grok capability probe after the decisions in
`docs/GROK_CAPABILITY_GATE.md` are approved. It must prove actual canonical X post access; generic web search does not
count. Live researcher mapping remains `NO-GO` until that gate and independent review are complete.
