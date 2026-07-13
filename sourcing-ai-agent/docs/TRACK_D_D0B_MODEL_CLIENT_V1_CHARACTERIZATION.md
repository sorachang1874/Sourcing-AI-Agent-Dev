# Track D D0b — ModelClient v1 characterize-first freeze

> Status: Author characterization batch (2026-07-14). This is a non-live test and documentation slice, not a D0/D1
> completion claim, independent-review `GO`, live-provider approval, or product activation.

## 1. Outcome and boundary

D0b freezes the existing `ModelClient` contract before any transport integration changes it. It adds no product code,
route, schema, credential path, durable state, API surface, or model/provider execution. The characterized baseline is:

- exactly 17 public methods: 14 business methods plus `provider_name`, `supports_outreach_ai_verification`, and
  `healthcheck`;
- five concrete client surfaces, discovered from the complete facade implementation/inheritance closure and checked against
  every concrete return of `build_model_client`, plus the six-method planning-only delegation boundary of
  `ScriptedLivePlanningModelClient`;
- 25 production consumer modules and 29 semantic call points, including the dynamic
  `supports_outreach_ai_verification` call and the two direct deterministic fallback calls;
- the complete existing OpenAI chat, OpenAI responses, and Qwen responses request shapes, including method, URL,
  authorization/content-type headers, timeout, and body;
- the separation between generic requested-vs-response model identity and the CRM Public Web product-model lock.

This batch deliberately leaves `src/sourcing_agent/model_provider.py`, `model_tool_runtime.py`, and
`model_route_registry.py` unchanged. D0a remains `simulate|scripted` only.

## 2. Characterized owners

| Contract | Existing owner | D0b assertion | Forbidden inference |
|---|---|---|---|
| Public model facade | `model_provider.py::ModelClient` | exact method set, sync/async kind, decorators, parameters, and return annotation | a new method is not compatible merely because one client happens to implement it |
| Concrete surfaces | implementation-root + inheritance discovery and every direct returned-call symbol from `build_model_client` | exact concrete population, complete inherited surface, exact local override set, and fail-closed factory-return parity | neither a hand-maintained class list nor an intersection filter can hide a new/incomplete factory return |
| Scripted planning delegation | `ScriptedLivePlanningModelClient` | runtime spy over all 17 methods: six business delegations, healthcheck's `delegate.provider_name` metadata dependency, and no other delegation | enrichment, outreach, identity judgment, summarization, or incidental side effects do not become live planning calls |
| Consumer inventory | production AST under `src/sourcing_agent/` | exact modules and function-owned call multiset | `serving_projection_migration.py`'s literal `model_client=None` is not a consumer |
| OpenAI chat wire shape | `_call_chat_completions_result` | POST URL, auth/content-type/user-agent headers, timeout, and exact `model/messages/max_tokens/temperature=0` body | no streaming/tool capability is inferred from the legacy payload |
| OpenAI responses wire shape | `_call_responses_api_result` | POST URL, auth/content-type/user-agent headers, timeout, exact body, and current role flattening | no native typed-message or tool schema is present |
| Qwen wire shape | `QwenResponsesModelClient._call_responses_api` | POST URL, auth/content-type headers, timeout, encoded body, conditional `max_output_tokens`, no temperature, urllib transport | Qwen is not a silent tool-calling fallback and has no response-model identity proof today |
| Identity boundaries | `_openai_model_identity_failure` and CRM Public Web caller | generic response identity remains distinct from `CRM_PUBLIC_WEB_PRODUCT_MODEL` | the CRM product lock must not spread to unrelated model calls |

The consumer guard uses parsed Python structure rather than line numbers, variable-name matching, or raw substring
counts. It resolves imported `ModelClient`/concrete/factory symbols, compatible local Protocol annotations, constructor
and factory/helper returns, typed receiver and attribute flow, receiver aliases, callable aliases, direct concrete
instances, and `getattr(receiver, method)` callables. It records the owning function and treats duplicate calls as a
multiset. Non-null `model_client=` handoffs keep propagation-only owners visible; literal `None` does not. The payload
tests intercept synthetic local mocks only; no network operation is possible.

## 3. Mutation sensitivity

The suite fails when any of these semantics drift:

1. a Protocol method is removed, added, renamed, changes sync/async kind or decorators, or changes parameters/return annotation;
2. the discovered concrete/factory-return population changes (including an unknown direct returned-call symbol), a concrete client gains or loses a public override, or
   the scripted-live delegate call graph changes anywhere across all 17 methods;
3. a consumer module/call owner/method/count changes without an intentional golden update, including alias, callable
   alias, `getattr`, helper-return, or direct-concrete paths;
4. chat/responses/Qwen changes HTTP method, URL, authentication/content-type headers, timeout, body, role flattening,
   token key, temperature policy, or transport;
5. an unknown OpenAI-compatible API style reaches transport;
6. generic response identity and the CRM-only product-model lock are conflated.

In-memory mutations prove detection of sync/async/decorator drift, receiver/callable aliases, `getattr`, helper returns,
and direct concrete calls. A runtime monkeypatch adds an otherwise hidden delegate side effect and proves the 17-method
spy graph catches it. These probes do not edit production source files.

## 4. Deferred gates

D0b does not close the remaining D0/D1 obligations. Before any live or product use, the system still needs a typed
durable model-turn owner, complete execution context and invocation envelope, owner-controlled pre-transport fencing,
cost reservation/exposure accounting, result-slot acceptance/consumption or adoption recovery, transcript retention
ownership, low-level live access checks, and a scope-matched independent-review `GO`.

Review is asynchronous and scope-local: once the D0b pinned review request is recorded, later unrelated/non-live work
may continue. A pending or invalid review still freezes D0b live/manual/product/milestone signoff and must never be
reported as `GO`.

## 5. Validation

Run from `sourcing-ai-agent/`:

```bash
PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_model_client_v1_characterization.py \
  tests/test_model_provider.py \
  tests/test_model_tool_runtime.py
.venv/bin/ruff check tests/test_model_client_v1_characterization.py
git diff --check -- \
  tests/test_model_client_v1_characterization.py \
  docs/TRACK_D_D0B_MODEL_CLIENT_V1_CHARACTERIZATION.md \
  docs/NEXT_TODO.md \
  docs/INDEX.md
```

Author evidence on 2026-07-14: the new characterization suite passed 15 tests; the combined characterization, existing
provider, and D0a runtime suites passed `157 tests + 11 subtests`; Ruff passed for the new test file; and the exact
four-path diff check passed. This is author evidence only and does not replace independent review.
