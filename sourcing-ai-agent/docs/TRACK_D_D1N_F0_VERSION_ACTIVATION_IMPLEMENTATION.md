# Track D D1n F0-A Agent Contract Activation Foundation

> Status: Pure implementation candidate with author evidence. Fresh pinned non-author review is required. This is not
> a formal `GO`, a PostgreSQL activation owner, a served-tool declaration, or authorization for a provider/model/live
> call.

Date: 2026-07-17

## Outcome and boundary

F0-A defines the immutable pins and fail-closed state-transition contract needed before one action or query tool can
be activated. It deliberately performs no persistence, registry mutation, catalog projection, CAS write, invocation
copy, provider call, or served decision.

The production state dimensions are separate:

```text
request: disabled -> shadow -> current -> disabled
tool:    disabled -> shadow -> hosted  -> disabled
```

Only a later integration owner may derive `served=true`. A structurally valid `hosted` snapshot in this module is a
candidate declaration, not runtime authorization. F0-A only defines the typed shape. F0-B is the production trust
boundary that must independently verify and persist review and hosted-gate receipts; a caller-constructed Python
value is never trusted evidence. F0-C/F4 must recheck the exact persisted policy, review receipt, hosted-gate receipt,
pin, runtime-scope, provider-mode, request window, and tool window facts at each served checkpoint. Scripted and paid
local canaries use a separate, expiring local-harness authorization. `local_canary` is intentionally not accepted as
a production request or tool release state. The served population remains zero.

## Immutable pin set

`AgentContractRequiredPinSet` canonically sorts and digests three retained histories:

- `ActionContractPin`: action type, request schema version/digest, and a distinct full action-contract digest. The
  latter must eventually bind binder, adapter, approval, command, display, and other behavior outside the JSON input
  schema.
- `ActionResultContractPin`: canonical tool name, exact route (`tool_kind` plus action type or exact query-owner pin),
  result schema version/digest, serializer owner/revision, and serializer-contract digest.
- `AgentToolContractPin`: canonical tool name, the same exact route identity, and exact tool-spec version/digest.

The iterable constructor materializes generators but rejects mappings before key collapse. Exact duplicates and the
same name/version with different immutable identity fail closed. Every history is scoped to one exact activation
identity: an action history cannot contain another action's request pins, a query cannot contain request pins, and
result/tool histories cannot mix tool names, tool kinds, action owners, or query owners. A same-name pin from a
foreign route is rejected. Each history is capped at 128 entries and the canonical retained-pin record is capped at
131,072 UTF-8 bytes.

Historical pins are monotonic within a validated transition: a transition may not remove a retained pin, including
during disable-only backout. This immutable Python value is not the durable audit ledger, however. F0-B must store
append-only pin, policy, and verified-receipt rows before changing current pointers. A later fully-disabled staging
transition may move a current pointer, but it must not erase the earlier durable rows.

All result, tool-registry, and activation layers import one canonical Agent tool-name validator. Action types and
query-owner ids retain their separate owner grammars; no alias transformation is allowed between result and tool
identity. Version validation is owner-specific rather than one shared permissive grammar: request pins retain the
current request owner grammar (`[A-Za-z0-9][A-Za-z0-9._:-]{0,127}`); result-schema and serializer revisions use the F1
canonical form (`[a-z][a-z0-9_]*_v[1-9][0-9]*`); tool-spec and tool-owner revisions retain the Agent tool registry's
historical grammar. Thus numeric-leading request/tool history remains representable while it cannot enter an F1
result or serializer version.

## Policy pins, review claims, and verified receipts

`AgentContractActivationPolicyPin` binds the policy owner, exact release Git object, runtime namespace, closed
provider-mode allowlist, optional workspace/requester allowlist-manifest digests, and exact owner/revision/digest pins
for the independent-review verifier, hosted gate, and schema-bridge/R-029 gate. It also binds the generated complete
production-action roster digest and expected action count, so a smaller served subset cannot redefine the migration
denominator. These are immutable inputs for F0-B and F0-C; this module does not execute those gates, generate the
roster, or interpret an allowlist manifest.

`AgentContractReviewEvidence` is deliberately only an untrusted artifact claim. It accepts a structurally canonical
`GO` declaration and binds the reviewed Git object, artifact id/digest, review-scope digest, claimed verifier-contract
digest, and review time. Raw/self-reported evidence cannot be placed in an active snapshot.

An active request or tool instead requires an `AgentContractReviewVerificationReceipt`. The receipt structurally
binds the activation key, complete required-pin-set digest, complete activation-policy digest, raw evidence, exact
verifier owner pin, verification artifact id/digest, and verification time. The formal
`review_evidence.review_scope_digest` remains the independent review runner's digest of its actual pinned Git scope;
it is not, and must not be rewritten as, the required-pin-set digest. The verified receipt binds those two distinct
facts: nested evidence preserves the actual review scope while `required_pin_set_digest` records the pin set that the
F0-B verifier proved was covered by that scope. The snapshot requires the receipt to match the policy's exact release
and verifier. This raises the contract boundary but is not a signature or a Python security boundary: callers can
construct dataclasses. F0-B must be the only production writer and may mint/persist a receipt only after the existing
independent-review verifier proves the pinned review scope covers the required pins. F0-C/F4 must trust only that F0-B
persisted receipt, never a request payload or caller-constructed object.

`AgentContractSchemaBridgeGateDecisionReceipt` is the separate typed complete-population input to hosted serving. It
must exact-match the activation policy's release, schema-bridge gate owner, production roster digest, and expected
action count. An `ALLOW` shape is valid only when every production action is schema-defined, the schema-less count and
release-window compatibility-hit count are both zero, the observation epoch/window is valid, and the separately
deployed constraint-validation artifact is present. This makes the complete action denominator explicit even while
the public served-tool population is zero. The value remains untrusted until F0-B verifies those artifacts and
persists it.

`AgentContractHostedGateDecisionReceipt` structurally carries the exact activation key, pin-set and policy digests,
hosted-gate owner pin, exact schema-bridge receipt digest, exact request/tool review-receipt digests, decision epoch,
artifact id/digest, `ALLOW` decision, and decision time. F0-A can validate and fingerprint this shape but does not run
the hosted gate or make either receipt authentic. F0-B verifies the gate outputs and persists both typed receipts in
the activation CAS unit of work. F0-C/F4 rechecks their digests, epochs, owners, bound review receipts, policy/pins,
complete-roster identity, and time ordering before catalog projection and invocation acceptance.

`AgentContractActivationSnapshot` also binds entry identity, exact current pins, required history, activation policy,
verified receipt references, schema-bridge and hosted-gate receipts, activation epoch, update revision, transition
actor/reason, and independent request/tool activation windows. Any non-null current pin must already be in the
required set, even while disabled and being staged. `request_state=current` always requires a bounded request window;
`tool_state=hosted` always requires a bounded tool window plus exact schema-bridge and hosted-gate receipts. The
schema-bridge decision must precede the hosted decision, which must precede tool activation. An action tool cannot be
hosted unless its request is current; a query has `request_state=not_applicable` and can never carry an action request
pin, request window, or request review receipt. Request `current` and tool `shadow` remain legal without a
schema-bridge receipt and never imply serving.

All timestamps normalize to one UTC representation: zero microseconds omit the fraction and non-zero fractions strip
trailing zeroes. Review precedes verification; request verification precedes request activation; tool verification
and the hosted-gate decision precede tool activation; and each expiry is strictly later than its own activation. The
request and tool windows remain separate facts and F0-C rechecks both against server time, so `request=current` is not
a timeless authorization. F0-B must also compare every receipt and activation timestamp with authoritative server
time before writing, because a pure constructor has no clock. Activation epoch, hosted-gate decision epoch, update
revision, and expected CAS revision are bounded to PostgreSQL `BIGINT` (`0..9223372036854775807`).

## Pure transition preflight

`validate_activation_transition(...)` validates a future repository CAS without writing state:

1. exact expected revision and prior snapshot digest;
2. immutable activation identity;
3. update revision and activation epoch increment by exactly one;
4. at most one request/tool state dimension changes;
5. the transition is present in the closed forward/backout graph;
6. active pins, verified receipts, activation windows, hosted-gate decision, and activation policy do not drift;
7. retained required pins are never removed.

A no-state-change staging transition is accepted only while the request is disabled/not-applicable and the tool is
disabled, and it must change a material pin, policy, receipt, gate, or activation-window field. Revision/epoch increments plus
actor/reason text alone are rejected as semantic no-op CAS churn. Backout disables new use; F0-B's append-only rows and
already-copied runtime pins, not the mutable current-pointer snapshot alone, preserve drain/audit history.

## Explicit non-claims and next batches

F0-A does not implement:

- the F0-B PostgreSQL activation migration (now `0011` or the next available number after F4a `0010`), activation
  tables, row locks, atomic CAS, or replica/brownfield behavior;
- historical lookup against `ActionRequestSchemaRegistry`, `ActionResultRegistry`, or `AgentToolRegistry`;
- independent-review scope-coverage verification, hosted-gate execution, receipt provenance/authenticity,
  server-time checks, or append-only rows;
- copying active pins into AgentAction, invocation, approval, dispatch, or result-slot rows;
- execution/authenticity verification of provider mode, workspace/requester allowlists, hosted/R-029 artifacts, the
  catalog/served predicate, local harness authorization, or request/tool expiry polling;
- action-contract digest construction is now owned by the separate F0-A2 fixed-forward implementation; F0-B must
  consume its exact versioned pins rather than minting a second fingerprint format.

F0-B owns the PostgreSQL activation/repository, review-scope coverage verification, hosted-gate invocation, trusted
review/gate receipt minting and persistence, server-clock checks, historical lookup, and append-only retention. F0-C/F4
owns one-UoW exact pin/policy/review/gate/window copy into touched action/invocation paths and the exact served
predicate, including rechecking both expiries. All rows remain absent/disabled until those batches and their
scope-matched reviews are complete.

## Author evidence

```text
tests/test_d1n_action_contract_activation.py    67 passed
ruff check / format check                       green
targeted mypy (identity + activation)           no issues
git diff --check                                green
```

These are author/local test results only. They do not replace the required pinned non-author review.
