# Track D D1n F3 Agent Tool Registry Fixed-Forward

> Status: Implementation/decision record (Track D increment). Review state and current authority are routed via docs/INDEX.md Tier 3; scheduled for module distribution (reorg R3+).

Date: 2026-07-17

Status: fixed-forward author candidate. A fresh pinned non-author review is required before any live-provider,
W6/nightly, manual signoff, serving activation, or milestone claim. This document is implementation evidence only;
it is not a review verdict or serving authorization.

## Outcome and boundary

F3 remains a pure immutable declaration registry. It does not invoke tools, persist release state, select a served
tool version, issue capabilities, reserve budget, authorize a subject, or make any provider call. The default
registry still declares zero tools and exposes no serving or activation method.

This fixed-forward closes the four P1 findings in the advisory `NO-GO` (`0/4/0/0`) within the F3 scope:

1. Query routes now bind exact query-owner, workspace/actor-binder, and adapter owner pins. Every pin includes owner
   id, owner revision, and contract digest; query manifests never emit null binder or adapter identities.
2. Each retained historical spec continues to fingerprint its exact release-owner pin. Separately, every non-empty
   registry requires one explicit `current_release_owner`. That pointer can rotate without reconstructing or
   rewriting historical specs, and the registry never infers a current tool version from order or version text.
3. Version validation is owner-specific. Request-schema and tool-spec history retain their historical grammar;
   F1 result schema, serializer, query-owner, and validator revisions use `[a-z][a-z0-9_]*_v[1-9][0-9]*`.
   Current F1 results require the `InternalToolValidatorSpec.validate_input` owner under interpretation contract v3;
   the retained v2 `ToolSpec.validate_input` owner remains accepted only through the explicit v2 contract entry.
4. Budget reservation and provider capability requirements are legal only for `command_backed_action`. Read-only
   queries and commandless actions reject both requirements. A live capability additionally requires a parent budget
   reservation and the existing dispatch-acceptance checkpoint.
5. `read_only` describes effects rather than registry kind. It is valid for a query or for an action-backed canonical
   read adapter such as `filter_projection`; both forms require zero command exposure and forbid human approval,
   budget reservation, and provider capability. This preserves the 15-row action identity without falsely recording
   a read as a commandless write.

## Exact scope

- `src/sourcing_agent/agent_tool_registry.py`
- `tests/test_d1n_agent_tool_registry.py`
- this document

No registry is populated, no production contract owner is activated, and no adjacent F0/F1/V1/V2/V3/F4 file is
changed by this fixed-forward.

The later F3-A batch now populates a separate four-tool isolated-local declaration registry without changing this
module's empty `DEFAULT_AGENT_TOOL_REGISTRY`; see
`TRACK_D_D1N_F3A_LOCAL_CANARY_TOOL_POPULATION_IMPLEMENTATION.md`. Presence there remains non-serving.

## Contract invariants

- Historical lookup remains exact and closed on `(tool_name, tool_spec_version, tool_spec_digest)`.
- Same-name version digest drift, route drift, kind drift, route collision, release-key collision, and release-key
  drift continue to fail closed.
- A release-owner rotation changes registry identity but not any retained tool-spec identity.
- `current_release_owner` identifies only the current external release authority. It is not a current tool pointer,
  release decision, activation epoch, or `served=true` claim.
- The empty default registry has `current_release_owner=null`, zero declared tools, zero historical specs, and no
  ambiguity because there is no tool population to govern.
- Query binder and adapter pins are fingerprinted identically to action route pins, while remaining query-specific
  owner declarations rather than an assumed action-registry lookup.
- Every action declaration requires one non-empty request schema version/digest plus a distinct full action-contract
  digest and exact action route. There is no schema-less `AgentToolSpec` variant and no API that converts the empty
  default registry or `served=0` into readiness. F0 historical lookup and its complete-roster/schema-bridge hosted
  evidence gate must still prove those structural pins against trusted persisted state before any serving projection.

## Author validation

The focused suite covers adversarial missing/invalid query route pins, exact manifest identities, digest binding,
release-owner rotation, missing current-owner rejection, closed historical lookup, owner-specific version grammars,
current and retained validator ownership, the provider requirement effect-kind matrix, and the valid live
command-backed combination.

Final command counts and formatter/type-check status are reported in the implementation handoff. Green author tests
do not replace the required pinned independent review.

```text
tests/test_d1n_agent_tool_registry.py    57 passed
```
