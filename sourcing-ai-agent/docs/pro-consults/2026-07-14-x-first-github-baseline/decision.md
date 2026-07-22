# Decision

> Status: Reference (consult transcript, 2026-07-14 era). Decisions were promoted into their owning docs; do not treat dated numbers here as current.

## Authority

ADVISORY_ONLY — not an independent-review artifact or formal GO.

## Local disposition

Overall disposition: `adjust`.

Post-run contract audit: this legacy capture is **not Connector-grounded decision evidence**. It lacks durable per-file
Connector citations, the original marker envelope, a schema-valid raw verdict, and a hash-bound redaction preflight.
The response remains historical advisory input only; every disposition below is independently supported by local
contracts, tests, and non-author review rather than by Connector authority.

### Accepted

- Start offline with synthetic fixtures, zero provider/model calls, zero external cost, and no database writes.
- Treat X as an evidence source, never a hidden fallback or replacement canonical source of truth.
- Preserve `search -> fetch/verify -> adjudicate -> materialize`; the first slice stops before materialization.
- Keep `assertions=[]`; do not call PersonAsset, CRM, projection, export, outreach, or canonical writer paths.
- Discovery/ranking population is only lab, current affiliation, role/function, publication/research evidence, and
  pretraining relevance. Ethnicity, nationality, and proxies such as name, language, region, school, community, bio,
  posts, mentions, or graph position are prohibited.
- Any provider registration, shared schema field, ranking change, canonical writer, or live probe requires a separate
  owner decision and gate.

### Adjusted

- Folder ownership: use the standalone sibling `x-first-researcher-sourcing/`, not
  `sourcing-ai-agent/fixtures/x_first/`. The user requested a separate line, and local repository mapping shows the
  sibling should exchange versioned artifacts rather than become a long-term runtime dependency.
- Identity: use stable X platform user ID plus handle history for the external account. Use an opaque provisional
  `pp_x_<ULID>` subject separately. Never use handle or name as a canonical person ID.
- Cross-source linking: emit only reversible identity-link proposals; no automatic merge. LinkedIn-first canonical
  ownership remains unchanged.
- Contract: use a typed `x.grok.collection.v1` envelope with task/query provenance, bounded evidence, coverage ledger,
  terminal-total states, budgets, errors, and explicit non-exhaustiveness rather than the response's smaller fixture
  field list.
- Fixture scale: target 24 synthetic accounts, 96 observations, eight query families, `.invalid` URLs, and no real
  names/handles/X URLs. This gives meaningful coverage and dedupe tests without live access.

### Rejected

- Directly placing the vertical slice inside the existing service repository's fixture/runtime tree.
- Treating a social profile/post as authoritative current-employment or identity truth.
- Reusing LinkedIn-specific discovery, candidate writer, Harvest/profile-fetch, CRM/export, or legacy
  `ethnicity_background` paths.
- Any language/community annotation in the first slice. It is deferred rather than used as a discovery proxy.

### Deferred gates

- Grok/X capability probe, including proof of canonical `x.com/{handle}/status/{post_id}` retrieval.
- Legal/terms/privacy review, retention/deletion/opt-out policy, cost/rate controls, and kill switch.
- Live X collection, provider registration, ranking, canonical identity link, CRM/export, or outreach.
- Repository formal independent review. ChatGPT Pro output cannot satisfy that gate.

### Local evidence

- The ChatGPT session reported retrieval of the pinned repository and six files at commit
  `5ab9798b5e8e1935cc1520abc82204c9261f5fef`, but the persisted bundle has no per-file citations and cannot mechanically
  prove that report. The raw branch lookup outcome was `not_found`, not `connector_unavailable`; the branch was
  provenance-only, while the immutable commit was the requested content scope.
- Local repository mapping identified reusable source-neutral contracts in
  `MODEL_NATIVE_SEARCH_PROVIDER_CONTRACT.md`, `PERSON_ASSET_EVIDENCE_ASSERTION_CONTRACT.md`, search-provider artifact
  types, coverage contracts, runtime-environment fail-closed rules, and company/lab aliases.
- The response file hash is `644b59af20904cd7bdd54453f0f242d1ca6428d66b77369a8e61ddfca660b977`.
- Under the subsequently hardened consultation skill, the response is complete but schema-invalid because it omitted
  a raw `keep|adjust|pivot` verdict and direct per-file connector citations. It remains useful advisory evidence.
- Under consultation contract v2, the computed classification is `legacy_advisory_unverified`,
  `consultation_valid=false`, and `usable_for_advisory_decision=false`.

## Follow-up

Keep this capture as retention-safe historical context only. Use a new contract-v2 consultation against an immutable,
committed diff for any Connector-grounded recommendation, and run the repository independent-review gate separately.
