# Track D D0d — Canonical tool-session projection

> Status: Author fixed-forward record (2026-07-14). The pinned review of D0d commit `0f8d249` returned `NO-GO` on an
> exact-type/identity boundary and a malformed ownership-table row. Both findings are addressed in `01220dc`, whose
> scoped pinned advisory re-review returned `GO` (P0/P1/P2=0/0/0); formal review remains pending. D0d remains additive
> and non-live. It is not a formal independent-review `GO`, provider transport, complete `ModelTurnExecutionContext`,
> live approval, or product activation.

## 1. Engineering outcome

D0d closes the duplicated buffered/streamed projection path in the D0a scripted runtime:

- `model_tool_runtime.py::ToolCallingSessionBase` is the single abstract owner of the two public session projections;
- every implementation supplies exactly one `_parse_tool_turn(...) -> ParsedToolTurn` canonical outcome;
- the base rejects any `_parse_tool_turn` return whose concrete type is not exactly `ParsedToolTurn` before either
  public projection reads its attributes;
- `run_tool_turn(...)` returns that outcome's `terminal_result` directly;
- `stream_tool_turn(...)` yields that outcome's complete `advisory_events` tuple, whose final event carries the same
  terminal-result object;
- `ParsedToolTurn` requires an exact tuple of exact registered event dataclasses, one final exact `TerminalEvent`, an
  exact `ToolTurnResult`, and object identity between `TerminalEvent.result` and `terminal_result`;
- `ScriptedToolTurnSession` now implements only `_parse_tool_turn`; it cannot independently override or drift the
  buffered and streamed projections;
- the existing scripted route fence still runs before caller-owned message/tool iterables are consumed, and transcript
  content, request, workspace, and terminal-request identities remain exact.

This is a structural non-live refactor. It adds no HTTP client, provider/model call, credential or environment read,
settings dependency, storage write, migration, API route, effect adapter, or live execution predicate.

## 2. Ownership and projection contract

| Contract | Owner / source of truth | Allowed behavior | Forbidden fallback | Deletion or extension condition |
|---|---|---|---|---|
| Canonical parsed outcome | Each session subclass's `_parse_tool_turn`, guarded by `ToolCallingSessionBase._validated_parse_tool_turn` | Produce one concrete `ParsedToolTurn`; the base exact-type checks it before projection | Duck-typed namespace, subclass substitution, or unchecked attribute projection | A live subclass may provide a stronger context-aware parser, but must preserve the exact one-outcome rule |
| Buffered projection | `ToolCallingSessionBase.run_tool_turn` | Validate the concrete outcome, then return `ParsedToolTurn.terminal_result` directly | Rebuild from events, copy fields, infer missing identity, or authorize an effect | Remains the canonical public projection |
| Streamed projection | `ToolCallingSessionBase.stream_tool_turn` | Validate the concrete outcome, then yield its exact `advisory_events` tuple, including the final terminal event | Drop/replace the terminal event, turn partial calls into actions, or synthesize a separate result | A future transport may change event timing, not terminal semantic identity |
| Terminal coherence | `ParsedToolTurn.__post_init__` | Exact tuple/event/result types; one final terminal event; `TerminalEvent.result is terminal_result` | Empty/list/subclass/duck events, early terminal, equal-but-distinct or mismatched terminal payload | Can only be changed through a versioned event/result contract |
| Scripted preparation | `ScriptedToolTurnSession._parse_tool_turn` | Non-live route fence, bounded message/tool collection, transcript/request/workspace checks, parser invocation, terminal request-hash check | Live/unknown mode, iterable pre-consumption, digest normalization, or caller-trusted identity | Replaced only by a strictly stronger replay owner |

`ToolCallingSessionBase` accepts the current provider-neutral `ToolTurnRequest`; this is not the complete live admission
context from D0 design §2.3. No live subclass may be activated until the typed `ModelTurnExecutionContext`, budget and
approval owner, cost ledger, effective-route snapshot owner, and low-level live gate are all present.

## 3. Fail-closed invariants

1. Parser state is the only canonical construction source. Events are an advisory projection and are never used to
   reconstruct a terminal result.
2. A parsed turn accepts only an exact tuple containing exact members of the closed event union and an exact
   `ToolTurnResult`; list/tuple subclasses, event subclasses, and duck-typed values reject.
3. Exactly one terminal event is permitted at the end, and its `result` must be the same object—not merely equal—to
   `ParsedToolTurn.terminal_result`.
4. Both public base projections exact-type check the subclass return before reading `terminal_result` or
   `advisory_events`.
5. The scripted live/unknown-route rejection remains ahead of any message/tool iterable consumption.
6. Transcript byte digest, canonical request hash, non-synthetic workspace binding, route fields, and terminal request
   hash remain exact and fail closed.
7. Arbitrary legal SSE chunking changes neither the canonical result nor terminal semantics.
8. `eligible_for_policy_evaluation` remains evidence shape only. D0d has no permission, approval, budget, result-slot,
   CAS, cost, AgentAction, or effect authority.
9. `simulate|scripted` remain the only D0a execution modes. Route registration and an envelope `provider_mode` value do
   not grant execution permission.

## 4. Regression and mutation coverage

The focused regressions make the structural boundary load-bearing:

- direct construction of `ToolCallingSessionBase` fails because `_parse_tool_turn` is abstract;
- both public projections reject a duck-typed `SimpleNamespace` and a `ParsedToolTurn` subclass before attribute use;
- a probe subclass proves the buffered result is the exact canonical result object and the streamed tuple is the exact
  advisory projection;
- AST-independent method-identity assertions prove `ScriptedToolTurnSession` inherits both public projections rather
  than reintroducing duplicate implementations;
- list and tuple-subclass containers, duck-typed and subclassed events, and duck-typed results are rejected;
- an early terminal, a missing terminal, and an equal-but-distinct terminal result are rejected;
- existing scripted parity tests cover buffered/streamed terminal equality across bytewise replay;
- existing sentinel iterables prove live rejection occurs before caller input consumption;
- existing content/request/workspace/route mutation tests continue to fail closed.

These tests kill the reviewer exploits and main unsafe mutations: unchecked duck-outcome projection, value equality in
place of result identity, untyped event members, reconstructing a result from events, dropping/replacing the terminal,
overriding one scripted public projection, or moving the route fence after iterable consumption.

## 5. Explicitly deferred

D0d does not implement or claim:

- `ModelTurnExecutionContext` or `ModelTurnBudget` field/value semantics;
- durable envelope issuance/persistence, result-slot accept/consume CAS, or generation/control-epoch fencing;
- cost reservation/exposure/reconciliation or R8 OB-2.2/OB-10.3 closure;
- the OB-10.4 request/result-slot binding for runtime namespace and provider mode;
- HTTP/SSE transport, deadline/retry/close behavior, circuit accounting, or provider capability dispatch;
- low-level live access, credentials, route activation, model fallback, or product integration;
- transcript recording/redaction/TTL ownership (OB-4.3);
- D1 tool registry/schema/dispatch adapters or any AgentAction/effect path.

## 6. Local validation

Run from `sourcing-ai-agent/` without provider credentials:

```bash
PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_model_tool_runtime.py \
  tests/test_model_invocation_contract.py
PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_model_provider.py \
  tests/test_model_client_v1_characterization.py
.venv/bin/ruff check \
  src/sourcing_agent/model_tool_runtime.py \
  tests/test_model_tool_runtime.py \
  tests/test_model_invocation_contract.py
.venv/bin/ruff format --check \
  src/sourcing_agent/model_tool_runtime.py \
  tests/test_model_tool_runtime.py \
  tests/test_model_invocation_contract.py
PYTHONPATH=src .venv/bin/python -m mypy --follow-imports=skip \
  src/sourcing_agent/model_tool_runtime.py
git diff --check -- \
  src/sourcing_agent/model_tool_runtime.py \
  tests/test_model_tool_runtime.py \
  tests/test_model_invocation_contract.py \
  docs/TRACK_D_D0D_TOOL_SESSION_BASE_IMPLEMENTATION.md \
  docs/TRACK_D_D0_MODEL_TOOL_RUNTIME_DESIGN.md \
  docs/TRACK_D_D0A_PROVIDER_NEUTRAL_RUNTIME_IMPLEMENTATION.md \
  docs/NEXT_TODO.md \
  docs/INDEX.md
```

Fixed-forward author evidence on 2026-07-14: runtime + invocation-contract suites `179 passed`; existing provider + D0b
characterization suites `60 passed + 11 subtests`; scoped Ruff rules passed; focused mypy passed with no findings; and
the exact scoped diff check passed. The runtime suite includes direct reviewer-exploit cases and a seven-column
ownership-row regression. Ruff format passed for both changed source/contract-test files. The sole format finding in
`tests/test_model_tool_runtime.py` is the pre-existing multiline generator expression near the split helper; the
identical finding reproduced from the pinned baseline through stdin, so this fixed-forward did not rewrite unrelated
baseline formatting. These author results and the scoped pinned advisory `GO` do not replace the pending formal
project review.
