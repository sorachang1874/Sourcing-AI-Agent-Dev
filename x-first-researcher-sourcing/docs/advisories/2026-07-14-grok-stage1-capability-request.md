# ADVISORY_ONLY — Grok offline Stage 1 consultation request

> Reproduction record only. This request does not authorize live X access or Stage 1 implementation.

## Invocation

```text
/Users/changyuyi/.grok/bin/grok \
  --cwd "/Users/changyuyi/projects/Sourcing AI Agent Dev/x-first-researcher-sourcing" \
  --disable-web-search \
  --no-memory \
  --no-subagents \
  --permission-mode plan \
  --max-turns 12 \
  --output-format plain \
  --single <PROMPT_BELOW>
```

The command completed with exit code `0`. A preceding attempt added `--check`; the CLI rejected that combination
before any model call because `--check` cannot be used with `--no-subagents`. The successful invocation omitted
`--check` and retained the single-agent boundary.

## Exact prompt

```text
ADVISORY_ONLY. Review the local X-first Stage 0 fixture contracts and propose a scalable but fail-closed Stage 1 capability-probe plan. Read only AGENTS.md, README.md, docs/GROK_CAPABILITY_GATE.md, docs/X_FIRST_FIXTURE_CONTRACT.md, configs/labs.v1.json, configs/query_families.v1.json, contracts/x.grok.collection.v1.schema.json, src/x_first/contracts.py, scripts/generate_openai_fixture.py, and tests/test_x_first_fixture_contract.py. Do not modify files. Do not use web search, X, any network retrieval other than the model session itself, real people, credentials, OAuth, provider APIs, canonical writes, outreach, or protected-trait inference/proxies. Treat the current live verdict as NO-GO and do not claim X-native capability. Audit Stage 0 for architecture, identity, lifecycle, provenance, state, budget, retention, privacy, failure, and test gaps. Then specify the smallest future Stage 1 probe: owner decisions, capability handshake, exact input/output envelope, one official lab account only, one query/page/execution, at most five public technical posts, stable IDs and provenance requirements, terminal-total statuses, fail-closed stop rules, cost/rate/deadline/kill switch, retention/deletion, artifact adapter boundary, observability, deterministic fixtures/tests, promotion criteria, and explicit deferred work. Keep population to lab plus current professional affiliation plus pretraining relevance. Never infer ethnicity, nationality, race, citizenship, religion, gender, or protected identity from names, language, region, school, community, bios, posts, mentions, or graph position. Output a self-contained Markdown advisory beginning exactly with ADVISORY_ONLY — Grok offline design review; not proof of X access, not implementation approval, and not a formal GO. Include sections Verdict, Stage 0 findings by severity, Proposed Stage 1 contracts, Owner/source-of-truth matrix, State machine and fail-closed rules, Probe runbook, Validation matrix, Promotion gates, Deferred decisions, and Exact local evidence reviewed. Distinguish facts observed in files from recommendations.
```

## Scope correction

The phrase `Do not use ... provider APIs` excludes external data providers and integrations. The Grok model-backend
call required to obtain the advisory did occur; no X, web-search, OAuth, credential, or other data-provider access did.
