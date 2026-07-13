# Track D D0c — Model invocation evidence contract

> Status: Author implementation record (2026-07-14). D0c is additive and non-live. It is not an independent-review
> `GO`, a durable writer/persistence implementation, a provider transport, live approval, or product activation.

## 1. Engineering outcome

D0c closes the schema ambiguity between the provider-neutral D0a result and the future D3 durable result slot without
crossing into transport or storage:

- `model_tool_runtime.py::ModelInvocationEnvelopeV1` is the only production class that owns the immutable physical
  result-side envelope schema;
- `schema_version=model_invocation_envelope_v1` and an exact top-level record keyset make additions/deletions explicit;
- `envelope_digest` is the canonical sorted-key compact-JSON SHA-256 of every envelope field except itself, so it has no
  self-cycle and every field is digest-sensitive;
- strict construction and round-trip parsing verify the digest over the exact incoming unsigned record before any
  normalization, require canonical round-trip equality, and reject empty/padded identifiers, wrong types, invalid
  SHA-256 values, unknown enums, partial causality, partial artifact identity, unknown record keys, and opaque or
  noncanonical usage payloads;
- physical snapshot, response-call-id, evidence-bundle, artifact, and cost references use typed `None` when no issuer or
  physical evidence exists; D0c never fabricates a reference for simulate/scripted fixtures;
- `length`, `content_filter`, missing response call-id evidence, and unavailable usage remain representable evidence, so a
  quarantined/non-authorizable outcome is not discarded or rewritten into success;
- the result/envelope mirror validator compares only fields both objects authoritatively share and fails closed on
  mismatch, including runtime-mode pollution;
- the checked-in route manifest now has a mechanical exact-key/unique-route/unique-circuit/draft-only/non-live
  preflight that runs at import and is directly mutation-tested.

No model/provider call, network access, credential read, environment/settings read, database schema, storage write,
migration, API route, product caller, or live predicate was added.

## 2. Canonical field groups

The exact canonical list remains in `TRACK_D_D0_MODEL_TOOL_RUNTIME_DESIGN.md` §2.2 and in the owning dataclass. The
groups below describe ownership; they are not a second schema definition.

| Group | Fields and rule |
|---|---|
| Schema and route | Explicit schema version; route id and content revision; provider, API style, requested/response/effective model identity and provider-response provenance |
| Effective configuration | Snapshot ref plus required snapshot digest, circuit identity; a missing ref is explicit absence, not permission to derive one later |
| Tenant and policy | Runtime namespace, provider mode, workspace, actor, permission scope, prompt-policy version, permission/outbound/model-safe revisions |
| Durable causality | Operation, turn, step, workflow command, activity run, and activity attempt are all present or all absent |
| Provider outcome | Terminal-result-only provider response identity; optional response call id, terminal reason, canonical provider-neutral usage and status; quarantine terminals and missing call-id evidence remain records |
| Fallback/circuit evidence | Typed fallback status, circuit state, and optional evidence-bundle SHA-256 |
| Result/cost evidence | Canonical result SHA-256, paired optional artifact ref/digest, optional cost exposure ref |
| Request and envelope identity | Canonical request SHA-256 plus derived envelope SHA-256 over the exact record excluding itself |

The record has no credential, authorization header, API key, endpoint secret, raw wire payload, arbitrary metadata map,
or other opaque dictionary. A future schema change requires a new explicit version and consumer migration; silently
accepting extra keys is forbidden.

## 3. Owner and source-of-truth matrix

| Contract | Current owner / source of truth | Allowed behavior | Forbidden inference or fallback | Later owner requirement |
|---|---|---|---|---|
| Physical envelope shape and digest | `ModelInvocationEnvelopeV1`, `MODEL_INVOCATION_ENVELOPE_RECORD_KEYS`, exact `to_record/from_record` | Immutable in-memory value; deterministic round trip; typed absence; preserve non-authorizable evidence | A second envelope class/schema, caller-supplied digest trust, fake refs, raw payload/secret bags, or field derivation from similarly named fields | Durable result-slot owner chooses issuance/persistence transaction and stronger live presence rules |
| Provider-neutral usage | `model_usage.py::ModelUsage` | Exact shared immutable five-field value | Dict/`Any` usage or transport-owned duplicate type | Existing D0a owner remains unchanged |
| D0a result mirror | `validate_tool_turn_result_envelope_mirror` | Compare exact shared route/tenant/runtime/request/model/provider-terminal/usage/result fields | Treating a passing mirror as permission, budget, cost, result-slot acceptance, durable ownership, or effect authority | Durable owner must add its own accept/consume CAS and authority checks |
| Product route records | `model_route_registry.py::DEFAULT_MODEL_ROUTE_SPECS` | Exact record fields, content-derived revision, globally unique route id and circuit key | Runtime mutation, client-selected model, reviewer/CRM route reuse, silent fallback | Product owner must explicitly approve each rollout transition |
| Route manifest | `model_route_registry_manifest` plus `validate_model_route_registry_manifest` | Exact manifest keysets and exact equality with checked-in route records; all current routes draft; live disabled | Canary/live pollution, missing/extra keys, duplicate identities, or a manifest-only route override | A later activation batch must replace this draft-only invariant with an owner-approved stronger gate |
| D0a execution permission | `assert_d0a_route_execution_allowed` | Only canonical `simulate|scripted` | Inferring permission from envelope `provider_mode`, route registration, or manifest presence | Live adapter must have typed owner, execution context, cost ledger, low-level live gate, and independent review |
| Physical refs and hashes | Future snapshot/evidence/artifact/cost owners; no owner is invented in D0c | `None` until an actual owner-issued object exists; required digests must be canonical SHA-256 | Minting placeholder URIs/hashes to satisfy a schema or treating absence as success | Define issuer, storage identity, tenant/mode namespace, retention, and acceptance CAS before live |

## 4. Fail-closed state and identity rules

1. Required text is an exact non-empty, non-padded string; boolean and other coercible values reject.
2. Route revision, effective-snapshot digest, canonical result digest, canonical request digest, envelope digest, and any
   present evidence/artifact digest are lowercase 64-character SHA-256 values.
3. Provider mode is a canonical data enum (`simulate|scripted|live`), independent of D0a execution permission.
4. Terminal, usage, fallback, and circuit states are closed enums. Reported usage cannot be empty; unavailable usage
   cannot contain token values.
5. The six durable causality identifiers are either a complete tuple or an absent tuple. D0c does not synthesize a
   partial durable identity around an in-memory D0a result.
6. Result artifact ref and digest appear together or not at all. Snapshot, provider-call, evidence-bundle, artifact,
   and cost evidence can be absent where no physical object/call exists.
7. Exact deserialization rejects any missing or extra top-level field, verifies the digest over the exact incoming
   unsigned record before normalization, constructs the value, and then requires canonical record equality. Explicit
   `null` aliases for omitted usage fields therefore fail even when supplied with a recomputed digest.
8. Mirror comparison is deliberately narrower than envelope validation. Fields that D0a does not own—such as route
   revision, policy revisions, causality, evidence bundle, artifact, and cost refs—cannot be validated by mirroring and
   must be supplied and checked by their future owner.
9. This v1 envelope represents terminal results only. `provider_call_id=None` means the observed provider response did
   not provide a usable call id; it never means no provider call occurred. Pre-call, transport, and protocol failures
   remain deferred to a future closed attempt-outcome discriminator or separate attempt artifact, and must not be
   forced into this schema with a fabricated terminal reason, response identity, or result digest.

## 5. Route-registry mechanical preflight

The preflight validates both checked-in specs and the emitted manifest:

- `route_id` and `circuit_key` are independently unique;
- spec records, manifest root, and manifest route entries each have an explicit exact keyset;
- manifest schema version, allowed non-live modes, and `live_enabled=false` are exact;
- every emitted route equals the complete checked-in record plus its derived revision;
- every checked-in route remains `draft`, with no canary/live route accepted by this batch.

This is configuration coherence only. It does not query settings, resolve credentials, select an endpoint, call a
provider, evaluate a canary, or enable a route.

## 6. R8 obligations and deferred owners

D0c narrows one ambiguity but does not close the remaining owner decisions:

- **OB-2.2:** the future PG cost-ledger reservation/exposure identity and every release/consume CAS must include the
  workspace tenant key before any paid live path. `cost_exposure_ref` is only evidence space, not that owner.
- **OB-4.3:** transcript redaction/recording/TTL owner and lifecycle remain deferred. D0c stores no transcript and does
  not weaken the current real-person-data prohibition for scripted fixtures.
- **OB-10.3:** cost-ledger rows and reconciliation must be isolated by runtime namespace and provider mode in addition
  to tenant identity before live. The envelope carrying those values does not make the ledger compliant by itself.
- **OB-10.4:** the future `ModelTurnExecutionContext` must explicitly carry runtime namespace and provider mode and
  bind them into request/result-slot identity. D0c adds them to the physical result schema only; it does not implement
  or silently repair the request-side context.

Also deferred: durable envelope issuer/persistence, result-slot accept/consume CAS, generation/control-epoch fences,
approval and budget authority, effective-snapshot owner, evidence-bundle/artifact/cost issuers, transport timeout/retry/
circuit accounting, low-level live gate, AgentAction creation, product caller integration, transcript governance, and
independent review. A passing serializer, mirror, registry preflight, or author test is not a formal `GO`.

## 7. Local validation

Run from `sourcing-ai-agent/` without live/provider credentials:

```bash
PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_model_invocation_contract.py \
  tests/test_model_tool_runtime.py
PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_model_provider.py \
  tests/test_model_client_v1_characterization.py
.venv/bin/ruff check \
  src/sourcing_agent/model_route_registry.py \
  src/sourcing_agent/model_tool_runtime.py \
  tests/test_model_invocation_contract.py \
  tests/test_model_tool_runtime.py
PYTHONPATH=src .venv/bin/python -m mypy --follow-imports=skip \
  src/sourcing_agent/model_route_registry.py \
  src/sourcing_agent/model_tool_runtime.py
git diff --check -- \
  src/sourcing_agent/model_route_registry.py \
  src/sourcing_agent/model_tool_runtime.py \
  tests/test_model_invocation_contract.py \
  docs/TRACK_D_D0C_MODEL_INVOCATION_CONTRACT.md
```

The D0c test fixtures are synthetic. The schema-only `live` value verifies representability and digest sensitivity; it
does not execute a route or call a provider.

Author evidence on 2026-07-14: D0c contract + existing runtime suites `145 passed`; provider + D0b characterization
`60 passed + 11 subtests`; Ruff rules passed across both D0c source files, the new contract test, existing runtime/
provider tests, and D0b characterization; Ruff format passed for all three D0c Python files; focused mypy passed for
both D0c source files; exact scoped `git diff --check` passed. The untouched `tests/test_model_tool_runtime.py` is not
Ruff-format-clean at current `b9edb00`; the exact format-check failure reproduced in a clean detached `b9edb00`
worktree, so D0c did not rewrite it merely to hide baseline drift. These are author results and never substitute for the
required pinned non-author review.
