# AGENTS.md

## Scope

This file applies to the standalone `x-first-researcher-sourcing` sibling.

## Safety and ownership

1. Discovery and ranking may use only lab, current professional affiliation, role/function, publication/research
   evidence, and pretraining relevance.
2. Never infer or proxy ethnicity, nationality, race, citizenship, religion, gender, or protected identity from name,
   language, region, school, community, biography, posts, mentions, or graph position.
3. Fixture mode remains the default. Fixture validation must never perform network, provider, model, credential, or
   live X access. Live entrypoints are closed and separately reviewed: the Stage 1 Grok/X v2 capability runner and the
   chshapi Luna semantic canary each require an explicit `--execute-live` flag and a distinct durable one-shot ledger.
   The Luna canary uses only a synthetic `.invalid` profile and is not an X/profile capability path.
4. X external-account identity is a stable platform user ID plus handle history. It is not a canonical person identity.
5. Provisional people use opaque `pp_x_<ULID>` IDs. Cross-source links remain reversible proposals and require human
   review; no name/model merge is allowed.
6. Outputs are raw observations and evidence proposals only. `assertions` must remain empty. Do not call or write
   PersonAsset, CRM, projection, export, outreach, or `sourcing-ai-agent` runtime owners.
7. Integration with `sourcing-ai-agent` is through a versioned artifact adapter, not direct runtime imports.
8. The user approved one bounded Stage 1 Grok/X capability probe on 2026-07-14; that approval was consumed by its
   recorded failed-closed attempt. The user separately approved one bounded chshapi Luna semantic canary on
   2026-07-14. Retries, researcher mapping, Stage 2, provider fallback, product integration, and scale-up still require
   their own owner decision and gate.

## Verification

- Use the standard library test command documented in `README.md`.
- Keep fixtures deterministic and use only reserved `.invalid` URLs.
- Add a regression test for every contract or guardrail change.
