# AGENTS.md

## Scope

This file applies to the standalone `x-first-researcher-sourcing` sibling.

## Safety and ownership

1. Discovery and ranking may use only lab, current professional affiliation, role/function, publication/research
   evidence, and pretraining relevance.
2. Never infer or proxy ethnicity, nationality, race, citizenship, religion, gender, or protected identity from name,
   language, region, school, community, biography, posts, mentions, or graph position.
3. Fixture mode is the default and only implemented mode. No network, provider, model, API key, OAuth token, or live X
   access may occur in fixture validation.
4. X external-account identity is a stable platform user ID plus handle history. It is not a canonical person identity.
5. Provisional people use opaque `pp_x_<ULID>` IDs. Cross-source links remain reversible proposals and require human
   review; no name/model merge is allowed.
6. Outputs are raw observations and evidence proposals only. `assertions` must remain empty. Do not call or write
   PersonAsset, CRM, projection, export, outreach, or `sourcing-ai-agent` runtime owners.
7. Integration with `sourcing-ai-agent` is through a versioned artifact adapter, not direct runtime imports.
8. Live capability probes require the explicit gates in `docs/GROK_CAPABILITY_GATE.md` and a separate owner decision.

## Verification

- Use the standard library test command documented in `README.md`.
- Keep fixtures deterministic and use only reserved `.invalid` URLs.
- Add a regression test for every contract or guardrail change.
