# Track D D0b — ModelClient v1 characterize-first freeze

> Status: Author characterization batch (2026-07-14). This is a non-live test and documentation slice, not a D0/D1
> completion claim, independent-review `GO`, live-provider approval, or product activation.

## 1. Outcome and boundary

D0b freezes the existing `ModelClient` contract before any transport integration changes it. It adds no product code,
route, schema, credential path, durable state, API surface, or model/provider execution. The characterized baseline is:

- exactly 17 public methods: 14 business methods plus `provider_name`, `supports_outreach_ai_verification`, and
  `healthcheck`;
- five concrete client surfaces and the six-method planning-only delegation boundary of
  `ScriptedLivePlanningModelClient`;
- 24 production consumer modules and 27 semantic call points, including the dynamic
  `supports_outreach_ai_verification` call;
- the existing OpenAI chat, OpenAI responses, and Qwen responses request shapes;
- the separation between generic requested-vs-response model identity and the CRM Public Web product-model lock.

This batch deliberately leaves `src/sourcing_agent/model_provider.py`, `model_tool_runtime.py`, and
`model_route_registry.py` unchanged. D0a remains `simulate|scripted` only.

## 2. Characterized owners

| Contract | Existing owner | D0b assertion | Forbidden inference |
|---|---|---|---|
| Public model facade | `model_provider.py::ModelClient` | exact method set and semantic signatures | a new method is not compatible merely because one client happens to implement it |
| Concrete surfaces | five current client classes | complete inherited surface plus exact local override set | inheritance does not authorize a broader scripted-live delegation scope |
| Scripted planning delegation | `ScriptedLivePlanningModelClient` | only normalize/review/refinement/intent/brief/search-plan methods delegate | enrichment, outreach, identity judgment, summarization, and health semantics do not become live planning calls |
| Consumer inventory | production AST under `src/sourcing_agent/` | exact modules and function-owned call multiset | `serving_projection_migration.py`'s literal `model_client=None` is not a consumer |
| OpenAI chat wire shape | `_call_chat_completions_result` | `model/messages/max_tokens/temperature=0` over `requests.post` | no streaming/tool capability is inferred from the legacy payload |
| OpenAI responses wire shape | `_call_responses_api_result` | `model/input/max_output_tokens/temperature=0`, with current role flattening | no native typed-message or tool schema is present |
| Qwen wire shape | `QwenResponsesModelClient._call_responses_api` | `model/input` plus conditional `max_output_tokens`, no temperature, urllib transport | Qwen is not a silent tool-calling fallback and has no response-model identity proof today |
| Identity boundaries | `_openai_model_identity_failure` and CRM Public Web caller | generic response identity remains distinct from `CRM_PUBLIC_WEB_PRODUCT_MODEL` | the CRM product lock must not spread to unrelated model calls |

The consumer guard uses parsed Python structure rather than line numbers or raw substring counts. It resolves direct
`model_client.method(...)`, `self.model_client.method(...)`, and the existing `getattr(..., method)` form, records the
owning function, and treats duplicate calls as a multiset. The payload tests intercept synthetic local mocks only; no
network operation is possible.

## 3. Mutation sensitivity

The suite fails when any of these semantics drift:

1. a Protocol method is removed, added, renamed, or changes parameters/return annotation;
2. a concrete client gains or loses a public override, or the scripted-live delegate scope changes;
3. a consumer module/call owner/method/count changes without an intentional golden update;
4. chat uses responses token keys, responses changes role flattening, or Qwen gains temperature/changes transport;
5. an unknown OpenAI-compatible API style reaches transport;
6. generic response identity and the CRM-only product-model lock are conflated.

An in-memory AST mutation test proves that the Protocol and call-point detectors change on semantic mutations; it does
not edit source files.

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

Author evidence on 2026-07-14: the new characterization suite passed 12 tests; the combined characterization, existing
provider, and D0a runtime suites passed `154 tests + 11 subtests`; Ruff passed for the new test file; and the exact
four-path diff check passed. This is author evidence only and does not replace independent review.
