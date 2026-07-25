# AGENTS.md

## Scope

This file applies to the standalone `x-first-researcher-sourcing` sibling.

## Safety and ownership

1. Base researcher discovery may use only target-lab affiliation (current or historical), role/function,
   publication/research evidence, and pretraining experience/relevance (current or historical). After that base
   population is established, a separately governed high-recall
   verification queue may use source-bound China/Asia professional-experience proxies; it must preserve the proxy
   strength and provenance and cannot rewrite the base population or become final eligibility/ranking.
   Candidate value must keep target-lab affiliation temporality and pretraining-experience temporality as independent
   configured dimensions. All evidence-supported current/historical combinations remain in the experience-recall pool;
   only evidence-complete current/current enters the default precision tranche. Historical experience is not a
   negative or an automatic hydration reason.
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
8. The user approved bounded live Grok/X researcher-search and chshapi Luna semantic exploration on 2026-07-14. Each
   live run still needs explicit technical emergency ceilings, a deadline, no-fallback policy, private receipt, and
   kill boundary. Candidate, observation, or native-X-call totals are not business success caps; expansion/convergence
   is evaluated from strategy coverage and mechanically reconciled marginal yield. The approval does not authorize
   uncontrolled cost/runtime, provider fallback, product integration, canonical writes, or outreach, and it does not
   replace the independent-review gate for promotion or milestone claims.

## Verification

- Use the standard library test command documented in `README.md`.
- Keep fixtures deterministic and use only reserved `.invalid` URLs.
- Add a regression test for every contract or guardrail change.
