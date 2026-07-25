# Model-Native Search Provider Contract

> Status: Guardrail contract drafted 2026-06-04. No normal-path implementation exists yet. Read with `DURABLE_EXECUTION_RUNTIME_CONTRACT.md`, `PRE_AGENT_CONTRACT_REVIEW.md`, and `archive/PUBLIC_WEB_SEARCH_PRODUCTIZATION_TODO.md` before adding model-native web search, model tool calls, or relay-provided search to Public Web, intent planning, company assets, or candidate enrichment.

## Purpose

Model-native Search can be useful as supplemental public evidence, but it is a separate provider source. It must not become a hidden fallback for DataForSEO, browser search, document fetch, or deterministic search-provider chains.

The current product runtime uses bounded chat completions for structured adjudication. The model receives repository-owned evidence and returns JSON judgement. It does not call native search/tools.

## Current State

- Provider id reserved: `model_native_search`.
- Default config: disabled.
- Normal runtime behavior: fail closed if `model_native_search` appears in `SEARCH_PROVIDER_ORDER`.
- Experimental mode name reserved: `experimental_evidence_only`.
- No model-native search provider owner, command owner, activity owner, or API implementation is registered yet.

## Allowed Future Shape

A future experiment may add model-native Search only as an explicit evidence collection source:

- It must run through a typed command/activity owner, not through Public Web reader fallback or model adjudication fallback.
- It must emit normalized evidence records with `source_provider=model_native_search`, model id, tool/search mode, query, URL/title/snippet/citation refs, cost metadata, and call id.
- It must feed downstream `documents.fetch -> evidence.adjudicate -> signals.materialize` like other evidence sources.
- It must never directly promote email/homepage/X/GitHub/Scholar/Substack assertions.
- It must never replace DataForSEO evidence silently. Side-by-side A/B validation is allowed; hidden fallback is not.

## Required Owner Matrix Before Enablement

| Field / behavior | Required owner | Source of truth | Consumers | Fallback status |
| --- | --- | --- | --- | --- |
| Provider registration | Search provider registry plus durable command owner | `build_search_provider(...)` and command owner registry | Public Web source collection, company research, future Agent tools | Fail closed until implemented |
| Enablement | Settings contract | `SEARCH_PROVIDER_ENABLE_MODEL_NATIVE_SEARCH` plus `SEARCH_PROVIDER_MODEL_NATIVE_SEARCH_MODE=experimental_evidence_only` | Provider builder and live validation runner | Disabled by default |
| Cost budget | Operation/provider budget owner | Operation budget and provider activity metadata | Live validation, Agent approval, signoff | Required before live calls |
| Evidence provenance | Activity/attempt/entity-delta owner | `workflow_activity_attempts`, `workflow_entity_deltas`, artifact refs | Public Web detail, audit/export, manual review | Required; no raw-only UI signal |
| Search result quality | Evidence adjudication owner | AI adjudication output plus document fetch evidence | `person_public_web_signals`, `PersonEvidence` | Raw model-search links stay artifact-only |
| Export treatment | Export owner | selected assertions/promotions/evidence policy | Projection/CRM Public Web exports | Not default-exportable unless promoted or explicitly selected |

## Fast Preflight

Fast checks must run before any live model-native Search experiment:

- `tests/test_search_provider.py::SearchProviderTest::test_model_native_search_provider_order_fails_closed_without_contract`
- `tests/test_settings.py::SettingsRuntimeOverrideTest::test_model_native_search_settings_are_explicit_and_disabled_by_default`
- `tests/test_settings.py::SettingsRuntimeOverrideTest::test_model_native_search_settings_require_explicit_env_or_secret`
- `tests/test_pre_agent_contract_review.py::test_model_native_search_contract_is_fail_closed_before_implementation`

The preflight goal is to prove that a config-only change cannot silently make model-native Search participate in normal provider chains.

## Live Experiment Acceptance Criteria

Before normal product use, run a small reviewed Public Web A/B:

- DataForSEO-only vs DataForSEO plus model-native Search.
- Same CRM record ids, same evidence window, same adjudication model, and no fallback.
- Report wrong-link rate, useful-reviewable-signal rate, promotion correctness, cost, latency, timeout/circuit behavior, and exportability.
- Independent review gate must return GO or have explicit accepted NO-GO exceptions.

## Non-Goals

- Do not call Codex/Claude Code as a production Public Web provider.
- Do not let model-native Search write CRM, projection, person asset, company asset, or export tables directly.
- Do not use model-native Search to bypass DataForSEO item-level retry/cost controls.
- Do not expose raw model-native Search snippets as user-visible signals without adjudication and provenance.
