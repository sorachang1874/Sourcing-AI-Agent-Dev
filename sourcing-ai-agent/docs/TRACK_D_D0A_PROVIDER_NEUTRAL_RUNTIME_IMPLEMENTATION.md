# Track D D0a — Provider-neutral model tool runtime substrate

> Status: Author implementation record (2026-07-14). D0a is a non-live, additive substrate only. It is not a
> complete D0/D1 implementation, independent-review `GO`, live-provider approval, or product activation.

## 1. Engineering outcome

D0a establishes the deterministic seam needed before any model transport or Agent owner can be connected:

- discriminated message, tool-call, usage, event, and terminal-result types;
- a bounded OpenAI-compatible chat SSE **parser** with arbitrary-chunk reassembly;
- schema-validated synthetic tool calls and stable duplicate-call occurrence ordinals;
- one provider-neutral immutable five-field usage value shared with the existing model provider;
- one canonical request hash covering route/config/policy/message/tool/tenant/runtime-mode identity;
- byte-content- and identity-bound scripted replay whose buffered and streamed projections share one terminal result;
- a checked-in product route registry whose entries are content-revisioned and all remain `draft`;
- an explicit D0a execution predicate that accepts only `simulate|scripted` and rejects `live`, `replay`, missing,
  or unknown modes before transcript parsing.

The implementation does not change the existing `ModelClient` protocol or any of its consumers. It imports no HTTP
client, reads no credentials or environment switches, emits no provider request, writes no durable state, and exposes
no API route.

## 2. Source-of-truth and ownership matrix

| Contract | Owner / source of truth | Allowed values and derivation | Normal D0a consumer | Forbidden consumer / fallback | Migration or deletion condition | Fast preflight |
|---|---|---|---|---|---|---|
| Product model routes | `model_route_registry.py::DEFAULT_MODEL_ROUTE_SPECS` | Unique `route_id`; content-derived SHA-256 revision; `fallback_policy=fail_closed`; D0a requires `rollout_state=draft`; exported lookup is a read-only `MappingProxyType` | request factory and scripted session | Reviewer routing, CRM product-model lock, runtime env, client-supplied model strings, or mutation of the public lookup | A later owner-approved route activation may extend the predicate only with typed owner, cost ledger, low-level live gate, and independent review | route revision/draft and public-registry immutability tests |
| D0a execution mode | `assert_d0a_route_execution_allowed` | Exact `simulate|scripted`; `live` and every other value reject | scripted session before any parse | environment-derived fallback or silent mode coercion | Deleted only when a later live adapter replaces it with a strictly stronger gate | `test_d0a_route_predicate_rejects_live_and_unowned_modes` |
| Typed message/tool schema | `model_tool_runtime.py` dataclasses and strict schema subset | Messages, tool count, per-tool schema, total tool fingerprints, and arguments are incrementally bounded canonical JSON; unsupported schema keywords reject rather than being partially interpreted | canonical hash and parser | raw untyped dictionaries treated as authorized commands or unbounded caller iterables | D1 may project registry-owned `ActionRequestSpec`; it must preserve this fail-closed validation boundary | schema/parser limit and sentinel-generator regression cases |
| Model usage value | `model_usage.py::ModelUsage` | Exactly five optional fields; each reported value is an integer, non-boolean, and non-negative; invalid direct construction raises provider-neutral `ModelUsageValidationError` | `model_provider.py` and `model_tool_runtime.py` import the same class; `OpenAIModelUsage` is compatibility alias only | transport/env/settings ownership, duplicate usage classes, duck-typed third representations | Compatibility alias may be removed only after existing provider callers migrate; the canonical class remains provider-neutral | class-owner/type-identity, strict-value, dependency, and deleted-name regressions |
| Canonical request identity | `canonical_tool_turn_request_payload/hash` | SHA-256 over route revision and snapshot digest, provider/model/API style, output/stream controls, tool schema, messages, policy revisions, tenant/actor/scope, transcript digest, runtime namespace, and provider mode | transcript lookup and terminal result evidence | timestamps, credentials, endpoint secrets, caller-supplied partial fingerprints | A durable owner may wrap the hash but must not omit or re-derive its fields | parametrized hash sensitivity tests |
| Stream semantic result | `parse_openai_chat_sse` parser state | Exactly one choice/index 0, stable call/model identity, bounded total/chunks/complete-line count/raw wire-line bytes/frames/deltas, incomplete remainder bound at the same current byte ceiling, explicit finish + `[DONE]`, schema-valid complete calls. Raw line size is measured after splitting before LF, so CR is included | scripted replay | advisory delta events authorizing effects | A later transport adapter must consume this result, not rebuild it from events | arbitrary byte-split, >512 KiB same-byte chunking parity, exact/+1 LF/CRLF line boundaries, and malformed-stream tests |
| Policy-evaluation eligibility | `ToolTurnResult.eligible_for_policy_evaluation` | Requires an explicit provider response call id plus either validated `end_turn` with zero calls or `tool_calls` with one or more calls; missing id and `length|content_filter` are false. `D0A_EFFECT_AUTHORIZATION_AVAILABLE=false` is unconditional | future owner policy gate input (not an effect adapter) | permission, approval, budget, cost, result-slot, CAS, or execution authorization | Durable owner policy + result-slot/consume CAS is required before any real action can be accepted | terminal-shape, missing-identity, and policy-sensitive negative tests |
| Scripted transcript scope | `ScriptedToolTurnTranscript` plus canonical request hash | Exact SHA-256 of concatenated SSE bytes, independent of chunk boundaries; exact request hash; non-synthetic transcript also requires exact workspace | scripted session | caller-declared digest without byte proof, cross-mode/live authorization, or source-controlled real-person transcript | Recording/TTL/redaction governance is a later D0 batch; real data remains prohibited here | content mutation, chunk-split, request-hash, type, and workspace tests |

### 2.1 Canonical provider-neutral usage value

The former provider/runtime duplicate is closed. `model_usage.py` now owns the only five-field immutable class and its
provider-neutral validation exception. Both `model_provider.py` and `model_tool_runtime.py` import that exact class;
the existing `model_provider.OpenAIModelUsage` name remains only as an object-identical import alias for current callers.
The temporary runtime type and its export were deleted, so reflective import policing is no longer the safety mechanism:
there is no second runtime class to acquire. Fast regressions prove one production class owner, provider/runtime type
identity, immutable serialization, strict non-negative integer/non-boolean construction, dependency neutrality, and
zero production references to the deleted name.

This closes only the usage-type predecessor recorded in `docs/NEXT_TODO.md`. It does not activate a transport, route,
product caller, effect authorization, provider credential path, or live mode; all remaining D0 gates still apply.

## 3. Parser state and authorization boundary

```text
bounded SSE bytes
  -> bounded raw wire lines (CR counts before LF; chunk-independent)
  -> complete data frames
  -> one choice / stable response identity
  -> bounded text + indexed tool-call fragments
  -> explicit finish reason + [DONE]
  -> canonical arguments + declared schema validation
  -> ToolTurnResult
  -> terminal event (same result object)
  -> optional future policy evaluation (never effect authorization in D0a)
```

`text_delta`, `tool_call_partial`, `usage`, and `stop` are advisory. D0a has no effect adapter. A parser failure returns
no result; a valid `length` or `content_filter` result is retained but cannot enter action policy evaluation. Even an
eligible result is not permission, approval, budget authorization, cost evidence, result-slot acceptance, or effect
execution. A terminal result without a provider response call id remains inspectable evidence but cannot enter policy
evaluation. `stop` with tool calls and `tool_calls` with zero calls are protocol failures rather than ambiguous fallbacks.

## 4. Route registry state

The owner-approved initial declarations from the Track D design are recorded as non-secret product metadata:

| route | capabilities | simulate mapping | rollout | live behavior in D0a |
|---|---|---|---|---|
| `agent.planner.loop` | stream, tools, usage, identity check | `scripted_tool_turn` | `draft` | rejected |
| `company.identity.adjudicate` | usage, identity check | `scripted_adjudication` | `draft` | rejected |

The second route intentionally fails the D0a tool-session capability predicate because it does not declare `tools`.
Registration is not dispatch permission. The registry contains no endpoint, API key, credential reference, or live
enable switch.

## 5. Mechanism-by-invariant check

1. **Single writer / ownership:** D0a owns immutable in-memory values only; it has no shared state or data writer.
2. **Tenant key:** workspace, actor, permission scope, runtime namespace, and provider mode are in the request hash;
   the terminal result explicitly repeats workspace, actor, runtime namespace, and provider mode, but does not claim a
   separate `permission_scope` result field. Non-synthetic transcripts add an exact workspace check.
3. **Generation / physical fencing:** no durable acceptance exists in D0a. Result-slot generation, control epoch,
   claim/attempt identity, and consume CAS remain mandatory before a real effect adapter.
4. **Lifecycle:** parsing has explicit terminal or exception; transcript replay is an exact tuple of immutable bytes,
   bound to the actual byte digest and exact request hash.
5. **Late / partial result:** missing frame boundary, finish, `[DONE]`, call fragment, or schema validity fails closed;
   truncated/filter terminals cannot enter action policy evaluation; no D0a result authorizes an effect.
6. **Cost honesty:** no physical call and no cost claim exist. Live remains blocked until the cost-ledger owner exists.
7. **Physical identity:** route content revision, effective-route snapshot digest, response model/call id, request hash,
   and terminal outcome digest are explicit. Missing response call id is an explicit policy-ineligible state. Durable
   envelope persistence is deferred.
8. **Provenance / trust:** response model and call id come only from parsed frames and cannot be supplied by tool args;
   model mismatch rejects. Model-safe result adapters are not claimed by this batch.
9. **Self-contained contract:** runtime and registry schemas are defined once in their owning modules; the existing
   `ModelClient` remains unchanged.
10. **Runtime isolation:** namespace and provider mode are hash/result identity; only explicit non-live modes pass.

## 6. Deferred, blocking before live or product use

D0a deliberately does **not** implement or claim:

- HTTP/SSE transport, retries, timeout/deadline enforcement, response close semantics, or circuit accounting;
- `ModelTurnExecutionContext`, durable invocation envelope, cost reservation/exposure ledger, or approval evidence;
- result-slot accept/consume CAS, cancel/supersession fencing, AgentAction creation, or any owner side effect;
- live low-level `assert_live_provider_access_allowed` wiring and credential blanking verification;
- ActionRequestSpec/D1 tool registry, model-safe result schema, dispatch adapter, API serve, or simulate dispatch preflight;
- transcript file recording, redaction, TTL, artifact references, or real-person/CRM data handling;
- `ModelClient` 14+3 facade characterization or transport integration;
- formal independent review, live/W6/manual/product validation, or milestone signoff.

These are additive follow-up batches. No caller should import D0a as a live client or treat
`eligible_for_policy_evaluation=true` as effect authorization; a typed durable owner, permission/approval/budget policy,
cost ledger, and result-slot UoW remain mandatory.

## 7. Local validation

Run from `sourcing-ai-agent/`:

```bash
PYTHONPATH=src .venv/bin/python -m pytest -q tests/test_model_tool_runtime.py
PYTHONPATH=src .venv/bin/python -m pytest -q tests/test_model_provider.py
.venv/bin/ruff check \
  src/sourcing_agent/model_usage.py \
  src/sourcing_agent/model_route_registry.py \
  src/sourcing_agent/model_provider.py \
  src/sourcing_agent/model_tool_runtime.py \
  tests/test_model_tool_runtime.py \
  tests/test_model_provider.py
PYTHONPATH=src .venv/bin/python -m mypy --follow-imports=skip \
  src/sourcing_agent/model_usage.py \
  src/sourcing_agent/model_route_registry.py \
  src/sourcing_agent/model_provider.py \
  src/sourcing_agent/model_tool_runtime.py
```

Author evidence on 2026-07-14 after the provider-neutral extraction: D0a + existing provider suites
`142 passed + 11 subtests`; Ruff passed on the six touched source/test files; focused mypy passed for
`model_usage.py`, `model_route_registry.py`, and `model_tool_runtime.py`. `model_provider.py` still reports the same two
pre-existing mypy findings (missing `requests` stubs and the legacy `int(Any | None)` site), reproduced in a clean
`a0e4e60` worktree, so this extraction added no focused provider type error. This remains author evidence only and does
not replace the required pinned non-author review for the contract-heavy batch.
