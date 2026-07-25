# Track D D0g — Execution-context and external-policy foundation

> Status: author implementation candidate (2026-07-15). This is an additive, non-live policy/type foundation and
> requires fresh pinned independent review before any scope-local promotion. It does not activate a provider/model
> route, issue credentials, reserve or settle cost, add storage/schema, serve an Agent tool, or authorize an effect.

## 1. Engineering outcome

D0g closes the request-side ownership gap left after D0f without widening runtime behavior:

- `ModelTurnBudget` is the immutable token, USD-ceiling, and persisted UTC wall-deadline value for one model turn;
- `ModelTurnExecutionContext` physically carries the full five-field execution prefix, the six causal identities,
  route/snapshot/policy pins, owner identity, attempt/idempotency identity, budget, reservation ref, and optional
  approval ref;
- `request_for_model_turn_execution_context(...)` derives every overlapping `ToolTurnRequest` field from that context
  and retains the context for fail-closed mirror validation;
- `EffectiveModelRouteSettings` is the typed, credential-free `settings.load_settings` owner input, and
  `issue_effective_model_route_snapshot(...)` is the one explicit deterministic snapshot issuer;
- `ExternalExecutionPolicyHeader` provides one immutable full-PFX header while `ModelExecutionPolicy` and
  `ProviderOperationPolicy` remain separate policy variants with separate compilers;
- the context-to-envelope mirror checks every jointly owned field, including all six causality pins.

The existing D0a parser/replay builder remains as an explicitly labelled compatibility path. It does not synthesize
missing durable refs, causal ids, budget, reservation, or approval authority.

## 2. Effective route snapshot owner

Snapshot issuance has two explicit values. `EffectiveModelRouteSettings` first captures only the credential-free
settings owned by `settings.load_settings`:

```text
schema_version, settings_owner=settings.load_settings, settings_policy_revision,
provider_family, live_gate_provider_name, endpoint_identity_digest,
request_timeout_ms, pricing_class, circuit_policy_id
```

The value has an exact canonical digest. It cannot name a different settings owner, contain a raw endpoint or secret,
or silently omit the settings-policy revision and the low-level live-gate provider identity. D0g does not call
`load_settings`; a later integration must exact-copy its server-owned values into this type.

The sole snapshot issuer is:

```text
model_route_registry.issue_effective_model_route_snapshot
```

Its immutable, non-secret content record is exactly:

```text
schema_version, owner, route_id, route_revision, provider, model, api_style,
budget_class, circuit_key, settings_owner, settings_policy_revision, settings_digest,
provider_family, live_gate_provider_name, endpoint_identity_digest,
request_timeout_ms, pricing_class, circuit_policy_id
```

The issuer resolves the checked-in `ModelRouteSpec`, exact-compares the caller's route revision, accepts only the typed
settings value, derives all route fields from the registry row, and embeds the exact settings provenance/digest. It
performs no settings/environment lookup. The endpoint is represented only by a SHA-256 identity, never a credential
or raw URL. The ref is deterministic and content addressed:

```text
model-route-snapshot:v1:<snapshot_digest>
```

`ModelTurnExecutionContext` accepts only that exact ref/digest relation. A test fixture digest or placeholder URI
therefore cannot masquerade as a D0g durable snapshot ref. Persistence remains the future action/approval/command
owner's responsibility; D0g adds no table or writer.

## 3. Budget and execution context

`ModelTurnBudget` has one canonical record:

```text
schema_version=model_turn_budget_v1
budget_class
max_input_tokens
max_output_tokens
max_total_tokens = max_input_tokens + max_output_tokens
monetary_ceiling (canonical non-negative USD string, at most 12 fractional digits)
currency_code=USD
deadline_at (timezone-aware UTC, canonical microsecond Z form)
```

Its digest is canonical JSON SHA-256. It is evidence of a selected ceiling, not proof that money was reserved.

`ModelTurnExecutionContext` is frozen and exact-digestable. It includes:

| Contract band | Physical fields |
|---|---|
| Route | `route_id`, `route_revision`, `effective_route_snapshot_ref`, `effective_route_snapshot_digest` |
| Full PFX | `runtime_namespace`, `provider_mode`, `workspace_id`, `scope_digest`, positive `coordination_plan_review_id` |
| Actor/policy | `actor_id`, `permission_scope`, `prompt_policy_version`, `permission_scope_revision`, `outbound_policy_revision`, `model_safe_schema_revision` |
| Six causality pins | `operation_run_id`, `turn_id`, `step_id`, `workflow_command_id`, `activity_run_id`, `activity_attempt_id` |
| Attempt/cost | positive `attempt`, `idempotency_key`, `budget`, `budget_reservation_ref`, nullable `approval_ref` |

`live|simulate|scripted` are data values, not permissions. Missing/noncanonical fields reject at construction. The
factory exact-compares snapshot-to-route and budget-class-to-route before returning a context. It also derives
`idempotency_key=model-turn:v1:<digest>` from route/snapshot identity, the complete five-field PFX, all six causal ids,
and positive attempt. Namespace/mode or causal drift with the old key is therefore rejected at context construction;
the caller cannot carry a scripted/simulate idempotency identity into a live context.

## 4. Request derivation and compatibility boundary

The canonical D0g request path is:

```text
owner-issued ModelTurnExecutionContext
  -> request_for_model_turn_execution_context(context, route, transcript_digest)
  -> derived frozen ToolTurnRequest mirror
```

Route, revision, snapshot digest, output-token ceiling, tenant/actor/scope, policy revisions, namespace, and mode are
derived from the context. Any later `dataclasses.replace` drift on one of those fields rejects because the retained
context is exact-compared in `ToolTurnRequest.__post_init__`.

`with_provider_mode(...)` operates only on context-less legacy fixtures. A context-bound request rejects this helper:
`provider_mode` is physical PFX/idempotency identity, so another mode requires a newly owner-constructed context and a
new derived idempotency key rather than mutation of an existing request.

`request_for_model_route(...)` remains only for the characterized D0a scripted/simulate parser and transcript tests.
It has no context and is not upgraded by inventing causal ids or owner refs. There are no production call sites for
either builder in this batch. The request/envelope mirror invokes the stronger context/envelope mirror whenever a
context-bound request is supplied; legacy fixtures preserve their existing narrower D0e mirror behavior.

## 5. Shared external policy header and strict variant split

`ExternalExecutionPolicyHeader` has the exact full PFX:

```text
(runtime_namespace, provider_mode, workspace_id, scope_digest, coordination_plan_review_id)
```

plus `schema_version`, a closed `policy_variant`, explicit `transport_kind`, explicit `evidence_variant`, and the
common policy fields:

```text
policy_id, policy_revision, provider_family, allowed_provider_modes,
retry_owner, budget_policy_id, live_gate_provider_name
```

`allowed_provider_modes` is a canonical immutable tuple and must contain the current physical `provider_mode`; this is
still policy evidence, not permission. Both the header and the domain-separated PFX have canonical SHA-256 identities.
The closed triples are exact:

```text
model_execution_v1    -> model_tool        -> model_tool_v1
provider_operation_v1 -> provider_operation -> provider_operation_evidence_draft_v1
```

The variants are deliberately not aliases:

| Variant | Compiler | Variant-only identity | Initial authority |
|---|---|---|---|
| `ModelExecutionPolicy` | `compile_model_execution_policy(...)` | exact context/PFX digests, route/snapshot/model/API/budget/policy pins; provider family and live-gate name are derived from the exact typed-settings-backed snapshot; header transport/evidence = `model_tool` / `model_tool_v1` | `execution_authorized=false` |
| `ProviderOperationPolicy` | `compile_provider_operation_policy(ProviderOperationPolicyInput(...))` | complete typed PFX/policy/provider-operation input, operation kind/schema, request-schema digest, budget class, outbound revision; header transport/evidence = `provider_operation` / `provider_operation_evidence_draft_v1`; no model fields | `execution_authorized=false` |

The provider-operation policy is only a shared policy shape. Its generic `provider_operation` transport family and
explicitly draft evidence value do **not** ratify or name the future D3 dispatch-exposure evidence schema for
HarvestAPI/provider search. It cannot be compiled from a model-variant header, and the closed triple rejects
`model_tool_v1` under a provider policy. Consequently Harvest/provider search cannot masquerade as model transport,
while later provider work can reuse the same PFX discipline without fabricating requested/effective model fields.

For the model compiler, a caller-supplied header is accepted only when it exactly equals the header derived from the
context. Snapshot/route/budget mismatch, namespace/mode/workspace/scope/review mismatch, or a cross-variant header all
fail closed before a policy value is returned.

## 6. Context-to-envelope closure

`validate_model_turn_execution_context_envelope_mirror(...)` exact-compares all fields physically shared by the
context and `ModelInvocationEnvelopeV1`:

```text
route id/revision
effective snapshot ref/digest
runtime namespace/provider mode/workspace
actor/permission scope
prompt/permission/outbound/model-safe revisions
operation/turn/step/command/activity-run/activity-attempt ids
```

It intentionally does not compare fields that are not the same contract:

- context `scope_digest` and `coordination_plan_review_id` remain external-policy/repository PFX inputs rather than
  being fabricated into the v1 envelope schema;
- a budget reservation ref is not a physical-call cost exposure ref;
- approval and budget digests are not present in `ModelInvocationEnvelopeV1`.

Passing the mirror proves evidence coherence only. It does not accept/consume a result slot or authorize an action.

## 7. Zero-activation boundary

D0g adds no imports or calls to HTTP, settings, environment, credentials, control-plane storage, provider task runtime,
Harvest, or the low-level live gate. Neither policy compiler has a production caller. Existing invariants remain:

- `model_route_registry_manifest().live_enabled == false`;
- the allowed executable D0 modes remain exactly `simulate|scripted`;
- `assert_d0a_route_execution_allowed(..., provider_mode="live")` still rejects before input consumption;
- `ToolTurnResult` still rejects `provider_mode=live`;
- `D0A_EFFECT_AUTHORIZATION_AVAILABLE` and `EXTERNAL_EXECUTION_AUTHORIZATION_AVAILABLE` are both false.

## 8. Explicit non-closure

D0g does not implement:

- model or provider transport, streaming HTTP, credentials, live gate activation, route canary/active transitions;
- cost reservation/exposure tables, pricing registries, Decimal/settlement, grant or approval persistence;
- result-slot acceptance/consumption, response/failure receipts, quarantine, retry, circuit transitions, or effects;
- a provider-operation registry, Harvest schema/owner/served predicate, D3 dispatch-exposure variant, or provider
  registry migration;
- Agent Session/D2, served D1 tools, migrations, API/frontend behavior, W6/nightly, or paid/manual validation.

## 9. Local validation

Run from `sourcing-ai-agent/` without provider/model credentials:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_external_execution_policy.py \
  tests/test_model_tool_runtime.py \
  tests/test_model_invocation_contract.py

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_d0f_model_invocation_envelope_repository.py \
  tests/test_d0f_model_invocation_envelope_postgres.py \
  tests/test_model_tool_runtime.py \
  tests/test_model_invocation_contract.py \
  tests/test_external_execution_policy.py
```

Run scoped Ruff, focused mypy over the three source files, and `git diff --check` before pinning the candidate. Author
evidence cannot substitute for a fresh non-author independent review and must not be reported as formal `GO`.
