# Track D D0e — Request-to-invocation-envelope provenance binding

> Status: Author implementation record (2026-07-14). Commit `8a33eca` has scoped pinned advisory review `GO`
> (P0/P1/P2=0/0/0); formal review remains pending. D0e is additive and non-live. It is not a formal independent-review
> `GO`, durable envelope issuance/persistence, provider transport, live approval, or product activation.

## 1. Engineering outcome

D0e closes one request-side provenance gap left explicit by D0c without granting the envelope any new authority:

- `validate_tool_turn_request_envelope_mirror` compares every envelope field that the current `ToolTurnRequest`
  physically owns: route id/revision, provider, API style, requested model, effective-route snapshot digest, runtime
  namespace, provider mode, workspace, actor, permission scope, prompt-policy version, and the permission/outbound/
  model-safe policy revisions;
- the validator recomputes `canonical_request_digest` from the actual request plus the supplied messages and tool
  registry, rather than trusting a caller-supplied digest or a partial field projection;
- the canonical digest therefore continues to bind output controls, fixed stream/tool-choice controls, transcript
  digest, complete message records, and complete tool fingerprints in addition to the directly mirrored provenance;
- after direct provenance passes, messages and tools are one-pass inputs to the validator and are each consumed exactly
  once by the bounded canonical collectors; direct mismatch rejects before consuming them, and callers that need later
  reuse must pass materialized tuples;
- response/effective model identity, provider call id, terminal state, usage, fallback/circuit state, evidence, result/
  artifact/cost identity, durable causality, approval, reservation, and budget authority are deliberately not inferred
  or compared by this request mirror.

No schema or version changed. D0e adds no HTTP client, settings/environment read, credential path, database/storage
writer, migration, API route, provider/model call, or execution/effect gate.

## 2. Owner and source-of-truth matrix

| Contract | Owner / source of truth | Allowed behavior | Forbidden inference or fallback | Extension condition |
|---|---|---|---|---|
| Direct request provenance | `ToolTurnRequest` fields mirrored by `validate_tool_turn_request_envelope_mirror` | Exact equality for route/config/tenant/policy identity physically present in both request and envelope | Deriving response identity, circuit identity, snapshot ref, causality, or result evidence from similarly named request fields | A versioned request/context may add fields only when it physically owns their value and tests make the new equality load-bearing |
| Canonical request identity | `canonical_tool_turn_request_payload/hash` over the actual request, messages, and tools | Recompute the complete SHA-256 and compare it with `ModelInvocationEnvelopeV1.canonical_request_digest` | Trust an envelope digest, hash only direct mirror fields, omit message/tool/control identity, or consume a second divergent iterable | A durable issuer may call this validator before persistence but must preserve the same canonical owner or introduce a versioned migration |
| Fixed request controls | `ToolTurnRequest.__post_init__` | D0a accepts only `tool_choice=auto` and `stream_options={include_usage:true}`; both remain in the canonical hash | Construct alternative values by bypassing frozen validation or treat them as optional aliases | A versioned request schema plus route/capability review is required before expanding either value set |
| Envelope response/durable evidence | Future response, result-slot, cost-ledger, circuit, artifact, and causality owners | Remains unvalidated by the request mirror | Treating mirror success as provider-response proof, result acceptance, approval, budget/cost authorization, or effect permission | Typed owners and their independent acceptance/CAS contracts must validate these fields |
| Execution permission | `assert_d0a_route_execution_allowed` | Exact `simulate|scripted`; schema/mirror may represent coherent `provider_mode=live` evidence without executing it | Inferring live permission from route registration, envelope mode, matching request provenance, or a valid canonical digest | A later low-level live gate may replace this only with the complete execution context, cost, policy, and review gates |

`ToolSpec.budget_required` remains part of the pre-existing complete tool fingerprint. Binding that declared schema
metadata in the canonical request digest is not proof of a budget reservation, approval, cost exposure, or permission.
D0e does not compare or synthesize any such authority.

## 3. Exact fail-closed boundary

The direct equality set is intentionally closed:

```text
route_id
route_revision
provider
api_style
requested_model
effective_route_snapshot_digest
runtime_namespace
provider_mode
workspace_id
actor_id
permission_scope
prompt_policy_version
permission_scope_revision
outbound_policy_revision
model_safe_schema_revision
canonical_request_digest := hash(request + messages + tools)
```

Any direct mismatch is reported as a deterministic sorted field list before message/tool consumption; a canonical
digest mismatch reports its single owned field. Wrong request/envelope object types reject. Invalid, oversized,
duplicate, or noncanonical message/tool inputs retain the existing bounded canonical-collector failures; D0e does not
normalize them into a different request identity.

The validator does **not** call the route execution predicate. This separation is intentional: a coherent request and
envelope can carry `provider_mode=live` as data, while the current D0a execution gate still rejects `live` before any
provider interaction. Mirror success is evidence coherence only.

## 4. Adversarial coverage

The scoped regressions make both sides of the authority boundary load-bearing:

- a positive fixture proves a complete request/messages/tools digest mirrors successfully;
- each of the fifteen directly owned fields plus the canonical digest is independently mutated on the envelope and
  must fail closed;
- request-only output controls, transcript digest, message content, and tool fingerprint mutations must fail via the
  recomputed canonical digest;
- a direct provenance mismatch rejects before touching sentinel message/tool iterables, while a positive one-pass
  generator fixture proves the canonical collectors do not perform a second consumption;
- alternative `tool_choice` and `stream_options` values remain mechanically rejected by request construction rather
  than being introduced only to exercise a hash branch;
- one valid envelope mutates response/effective model, snapshot ref, circuit, complete causality, provider outcome,
  usage, fallback, evidence, result/artifact, and cost fields together and still passes, proving that the request mirror
  does not claim those owners;
- coherent `provider_mode=live` request/envelope data passes the mirror while the D0a execution predicate rejects the
  same route/mode;
- wrong object types fail closed; ordinary fixtures use materialized tuples, with only the dedicated one-pass generator
  regression exercising consumable iterable semantics.

## 5. Explicitly deferred

D0e does not implement or claim:

- durable envelope issuer, persistence UoW, result-slot accept/consume CAS, generation, or control-epoch fencing;
- `ModelTurnExecutionContext`, approval evidence, budget reservation, cost exposure/reconciliation, or circuit owner;
- response/effective identity, terminal/usage/result evidence issuance, artifact/evidence-bundle ownership, or failure
  envelopes for pre-call/transport/protocol outcomes;
- HTTP/SSE transport, retries/deadlines, credentials, route activation, low-level live access, or product callers;
- AgentAction creation, tool execution, storage/schema migration, transcript recording/redaction/TTL, or any effect;
- formal independent review, live/W6/manual/product validation, or milestone signoff.

## 6. Local validation

Run from `sourcing-ai-agent/` without provider credentials:

```bash
PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_model_invocation_contract.py \
  tests/test_model_tool_runtime.py
PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_model_provider.py \
  tests/test_model_client_v1_characterization.py
.venv/bin/ruff check \
  src/sourcing_agent/model_tool_runtime.py \
  tests/test_model_invocation_contract.py \
  tests/test_model_tool_runtime.py \
  tests/test_model_provider.py \
  tests/test_model_client_v1_characterization.py
.venv/bin/ruff format --check \
  src/sourcing_agent/model_tool_runtime.py \
  tests/test_model_invocation_contract.py
PYTHONPATH=src .venv/bin/python -m mypy --follow-imports=skip \
  src/sourcing_agent/model_tool_runtime.py
git diff --check -- \
  src/sourcing_agent/model_tool_runtime.py \
  tests/test_model_invocation_contract.py \
  docs/TRACK_D_D0E_REQUEST_ENVELOPE_BINDING_IMPLEMENTATION.md \
  docs/TRACK_D_D0C_MODEL_INVOCATION_CONTRACT.md \
  docs/TRACK_D_D0A_PROVIDER_NEUTRAL_RUNTIME_IMPLEMENTATION.md \
  docs/TRACK_D_D0_MODEL_TOOL_RUNTIME_DESIGN.md \
  docs/NEXT_TODO.md \
  docs/INDEX.md
```

Author evidence on 2026-07-14: invocation-contract + runtime suites `176 passed`; provider + D0b characterization
`60 passed + 11 subtests`; scoped Ruff rules passed; both changed Python files are Ruff-format-clean; focused mypy
passed with no findings; and the exact scoped diff check passed. These author results do not replace the required
formal project review for this contract-heavy batch; the scoped pinned advisory review at `8a33eca` returned `GO`
with no P0/P1/P2 findings.
