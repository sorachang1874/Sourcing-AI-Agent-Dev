# Track D D1n F1 Action Result Schema Implementation

> Status: Implementation/decision record (Track D increment). Review state and current authority are routed via docs/INDEX.md Tier 3; scheduled for module distribution (reorg R3+).

Date: 2026-07-17

Status: fixed-forward author candidate; fresh pinned non-author review is required. This document is implementation
evidence only. It is not a review verdict, serving activation, or live-provider authorization.

## Outcome

F1 supplies the immutable, model-safe result-contract owner used by the future unified Agent tool registry. It does
not populate that registry or make any action served. `DEFAULT_ACTION_RESULT_REGISTRY` remains empty.

The fixed-forward closes the prior pinned and follow-up Ultra findings in the F1 scope:

- provider-visible `ToolSpec` and `ToolCallRecord` now use the same canonical Agent tool-name grammar;
- result variants validate through the distinct internal-only `InternalToolValidatorSpec`, whose owner identity is
  pinned by interpretation-contract version rather than masquerading as an Agent tool. Both types call the same
  schema-freezing and input-validation engine, so their bounded JSON-schema behavior cannot drift independently;
- interpretation v2 and v3 records are literal, retained, immutable registry entries. A spec freezes the exact
  record and digest at construction; v2 retains its historical validator-owner pin, while new specs use v3;
- v3 combines field provenance and string-value roles into one closed authority matrix. Externally controlled
  values cannot become controls or opaque artifact references. Externally controlled identifiers require an
  explicit path exception plus a bounded schema pattern;
- v3 uses positive transport-neutral validation for display text and identifiers, rejects scheme-free/path-shaped
  locator values, and recognizes artifact locator fields by structural tokens instead of substring heuristics;
- the registry keeps exact historical `(tool_name, result_schema_version, result_schema_digest)` lookup and permits
  a stable query owner to publish a new revision/digest without permitting route reassignment.

The seven follow-up F1 blockers are closed as follows:

| Finding | Fixed-forward closure |
| --- | --- |
| cross-layer tool-name drift | one shared canonical grammar at `ToolSpec`, `ToolCallRecord`, and `ActionResultSpec`; internal diagnostic names use a separate non-provider type |
| provenance and role declarations independent | closed provenance x role matrix, closed control schemas, explicit bounded exceptions for externally controlled identifiers, and no external opaque refs |
| finite raw-path denylist | v3 positive display/identifier contracts plus explicit `web_url` and opaque-ref roles |
| scheme-free artifact locator bypass | ancestor-aware structural locator rejection and separator-free noncanonical identifiers |
| mutable interpretation pin | exact v2/v3 record lookup, normalized version, per-spec frozen record/digest, and retained historical lookup |
| query owner revision overconstraint | stable route uniqueness uses `(query, owner_id)` while exact historical specs retain owner revision/digest |
| artifact `ref` substring false positive | exact structural token matching preserves lifecycle metadata such as refresh status and preference |

## Exact scope

- `src/sourcing_agent/model_tool_runtime.py`
- `src/sourcing_agent/action_result_schema.py`
- `tests/test_model_tool_runtime.py`
- `tests/test_d1n_action_result_schema.py`
- this document

`src/sourcing_agent/acquisition_plan_preview.py` consumes the F1 contract in the separate V1 batch. It is not part
of this F1 commit/review scope.

## Contract boundary

An `ActionResultSpec` binds all of the following into `result_schema_digest`:

- stable action/query route identity and retained query-owner revision evidence;
- result schema version, closed terminal variants, serializer owner/revision/contract, and canonical validator owner;
- literal interpretation-contract record and digest;
- exact field provenance and value-role maps, including explicit external-identifier exceptions;
- byte/item/depth limits and canonical opaque artifact schemes.

The serializer accepts only an exact closed owner-output object and returns canonical UTF-8 JSON suitable for
`ToolResultMessage`. It never reads arbitrary workflow-command results, discovers a serializer, authorizes an
effect, selects a current version, or derives `served=true`.

## Validation and review

The focused F1 test set covers cross-layer name grammar, shared schema-engine parity, retained v2/v3 behavior, exact
version rejection, frozen digest identity, the full external-provenance/role authority matrix, raw/scheme-free
locator bypasses, artifact-name false positives, schema/payload bounds, and historical registry behavior. Current
author evidence is `331 passed` from:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv-tests/bin/python -m pytest -q \
  tests/test_d1n_action_result_schema.py tests/test_model_tool_runtime.py
```

The pinned F1 artifact
`runtime/reviews/20260717T033016Z_Track_D_D1n_F1_revisioned_action_result_contracts.md` is a valid scope-local
`NO-GO` against `f7351ae1f26e738050a3fa165c6f4697823150d6` with blocking totals `P0/P1/P2/P3=0/4/3/0`.
The later combined F1/V1 turn against `ce3ffecc98ca39ac476fc6d008298da5922ee52a` supplied additional exact finding
evidence but did not produce a complete durable gate artifact/effective-config receipt, so it remains advisory rather
than a formal verdict. Neither review is evidence for this uncommitted fixed-forward. After commit, F1 must receive a
fresh pinned non-author review against only the exact files above.

## Remaining dependencies

F1 alone cannot create a usable Agent tool. F0 must supply trusted activation/history state, V1/V2/V3 must provide
the concrete request/result/query contracts, F3 must bind those pins into one route-complete `AgentToolSpec`, and the
result occurrence/terminal-winner path must serialize the exact accepted owner result. Paid or hosted use remains
outside this scope.
