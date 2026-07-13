# Track D D0d — Canonical tool-session projection

> Status: Author implementation record (2026-07-14). D0d is additive and non-live. It is not an independent-review
> `GO`, a provider transport, a complete `ModelTurnExecutionContext`, live approval, or product activation.

## 1. Engineering outcome

D0d closes the duplicated buffered/streamed projection path in the D0a scripted runtime:

- `model_tool_runtime.py::ToolCallingSessionBase` is the single abstract owner of the two public session projections;
- every implementation supplies exactly one `_parse_tool_turn(...) -> ParsedToolTurn` canonical outcome;
- `run_tool_turn(...)` returns that outcome's `terminal_result` directly;
- `stream_tool_turn(...)` yields that outcome's complete `advisory_events` tuple, whose final event carries the same
  terminal result;
- `ScriptedToolTurnSession` now implements only `_parse_tool_turn`; it cannot independently override or drift the
  buffered and streamed projections;
- the existing scripted route fence still runs before caller-owned message/tool iterables are consumed, and transcript
  content, request, workspace, and terminal-request identities remain exact.

This is a structural non-live refactor. It adds no HTTP client, provider/model call, credential or environment read,
settings dependency, storage write, migration, API route, effect adapter, or live execution predicate.

## 2. Ownership and projection contract

| Contract | Owner / source of truth | Allowed behavior | Forbidden fallback | Deletion or extension condition |
|---|---|---|---|---|
| Canonical parsed outcome | Each session subclass's `_parse_tool_turn` returning `ParsedToolTurn` | Produce one validated terminal result plus its advisory projection | Construct one result for buffered mode and a second result from stream events | A live subclass may provide a stronger context-aware parser, but must preserve the one-outcome rule |
| Buffered projection | `ToolCallingSessionBase.run_tool_turn` | Return `ParsedToolTurn.terminal_result` directly | Rebuild from events, copy fields, infer missing identity, or authorize an effect | Remains the canonical public projection |
| Streamed projection | `ToolCallingSessionBase.stream_tool_turn` | Yield `ParsedToolTurn.advisory_events` unchanged, including the final terminal event | Drop/replace the terminal event, turn partial calls into actions, or synthesize a separate result | A future transport may change event timing, not terminal semantic identity |
| Terminal coherence | `ParsedToolTurn.__post_init__` | Require a final `TerminalEvent` whose result equals `terminal_result` | Empty streams, terminal omission, or mismatched terminal payload | Can only be strengthened by a versioned contract |
| Scripted preparation | `ScriptedToolTurnSession._parse_tool_turn` | Non-live route fence, bounded message/tool collection, transcript/request/workspace checks, parser invocation, terminal request-hash check | Live/unknown mode, iterable pre-consumption, digest normalization, or caller-trusted identity | Replaced only by a strictly stronger replay owner |

`ToolCallingSessionBase` accepts the current provider-neutral `ToolTurnRequest`; this is not the complete live admission
context from D0 design §2.3. No live subclass may be activated until the typed `ModelTurnExecutionContext`, budget and
approval owner, cost ledger, effective-route snapshot owner, and low-level live gate are all present.

## 3. Fail-closed invariants

1. Parser state is the only canonical construction source. Events are an advisory projection and are never used to
   reconstruct a terminal result.
2. A parsed turn is invalid unless its last event is a terminal event carrying the same result as
   `ParsedToolTurn.terminal_result`.
3. The scripted live/unknown-route rejection remains ahead of any message/tool iterable consumption.
4. Transcript byte digest, canonical request hash, non-synthetic workspace binding, route fields, and terminal request
   hash remain exact and fail closed.
5. Arbitrary legal SSE chunking changes neither the canonical result nor terminal semantics.
6. `eligible_for_policy_evaluation` remains evidence shape only. D0d has no permission, approval, budget, result-slot,
   CAS, cost, AgentAction, or effect authority.
7. `simulate|scripted` remain the only D0a execution modes. Route registration and an envelope `provider_mode` value do
   not grant execution permission.

## 4. Regression and mutation coverage

The focused regressions make the structural boundary load-bearing:

- direct construction of `ToolCallingSessionBase` fails because `_parse_tool_turn` is abstract;
- a probe subclass proves the buffered result is the exact canonical result object and the streamed tuple is the exact
  advisory projection;
- AST-independent method-identity assertions prove `ScriptedToolTurnSession` inherits both public projections rather
  than reintroducing duplicate implementations;
- deleting the terminal event or substituting a different terminal result is rejected by `ParsedToolTurn`;
- existing scripted parity tests cover buffered/streamed terminal equality across bytewise replay;
- existing sentinel iterables prove live rejection occurs before caller input consumption;
- existing content/request/workspace/route mutation tests continue to fail closed.

These tests kill the main unsafe mutations: reconstructing a result from events, dropping or replacing the terminal
event, overriding one scripted public projection, or moving the route fence after iterable consumption.

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

Author evidence on 2026-07-14: runtime + invocation-contract suites `148 passed`; existing provider + D0b
characterization suites `60 passed + 11 subtests`; scoped Ruff rules passed; focused mypy passed with no findings; and
the exact scoped diff check passed. Ruff format passed for both changed source/contract-test files. The sole format
finding in `tests/test_model_tool_runtime.py` is the pre-existing multiline generator expression near the split helper;
the identical finding reproduced from the unmodified current `HEAD` through stdin, so D0d did not rewrite unrelated
baseline formatting. These are author results and do not replace the required pinned non-author review for this
contract-heavy batch.
