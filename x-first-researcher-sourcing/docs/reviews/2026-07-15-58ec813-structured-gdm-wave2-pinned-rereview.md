# Structured Grok transport and Google DeepMind wave2-v2 pinned rereview

## Evidence header

- Reviewer: independent non-author subagent `/root/gdm_wave2_pinned_rereview`; adversarial rereview against pinned Git
  objects only.
- Overall feature base: `96c07c578c6a7641c20a71d8df50c3b56b1627e8`.
- Previously reviewed head: `024ca34a3420a712188e3eabd90d26c4996abb1c`.
- Remediation head: `58ec8135d8391665f2e226276361b60a75db6500`.
- Both bases are ancestors of the remediation head. This rereview used only the
  `x-first-researcher-sourcing/` subtree.
- Remediation scoped binary-diff SHA-256:
  `a393568958b223a95f39014655db629e4b22347e02cfe1ea130263f2234af133`
  (`8 files`, `177 insertions`, `16 deletions`; this includes the 127-line prior review artifact).
- Detached clean worktree: `/private/tmp/xfirst-review-58ec813`.
- The main working-tree implementation and any private pending request/grant were not used as implementation evidence.
  The only main-tree write made by this reviewer is this artifact.
- The retained, already-completed 197-event Google DeepMind wave1 transcript was used only as a Grok 0.2.101
  protocol fixture. No OAuth bytes or native-X result bodies were read. No Grok/X/model/network call, grant issuance,
  provider execution, commit, promotion, or product write occurred.

## Validation

- Targeted runner plus KPI suites: `59/59` passed in `10.924s` (`11.29s` wall).
- Complete pinned X-first suite: `343/343` passed in `37.186s` (`38.58s` wall).
- Whole pinned X-first Ruff check: passed.
- `PYTHONPATH=src ... -m x_first.contracts`: `status=valid`, `errors=[]`, synthetic precision/recall `1.0/1.0`.
- `git diff --check 96c07c5..58ec813 -- x-first-researcher-sourcing`: passed; detached status remained clean.
- Google DeepMind wave2-v2 prompt SHA-256 recomputed as
  `b8f7a6e4a47b850d12a8756141fcab9d060f11c03a749d95e389fbcb94432f52`, matching its effective-policy row.
- Pinned-code replay of the retained pre-headless Google DeepMind live bundle returned `[]` using both absolute and
  normalized relative run-root paths. This preserves legacy plain-command replay without relabelling the old
  noncompliant run as successful.
- Direct protocol replay of the retained transcript accepted the observed
  `retry_state -> user -> thought -> pre-tool progress -> first native-X call` prefix and the final assistant JSON
  suffix. It mechanically reconstructed `197` events, `94/94` started/completed native-X calls, and tool counts
  `50 x_keyword_search + 39 x_user_search + 5 x_semantic_search`; the final event remained an
  `agent_message_chunk`.

Severity totals: `P0=0`, `P1=0`, `P2=1`.

## Prior-finding reconciliation

### P1-1 `[closed]` — exact Grok 0.2.101 pre-tool progress is now admitted without weakening completion proof

`src/x_first/adaptive_grok_wave_runner.py:3350-3359` admits an assistant progress chunk after the single user event
whenever no tool is outstanding, including before the first tool. The global proof still requires a non-empty native-X
call set, exact started/completed equality, one prompt identity, and assistant output. The headless branch additionally
requires an exact final assistant suffix equal to the strict outer envelope text and, when no legacy terminal exists,
requires the last event to be the final assistant chunk (`:3373-3401`).

The regression at `tests/test_adaptive_grok_wave_runner.py:1180-1275` now includes the missing pre-tool progress event,
two completed native-X calls, an inter-tool progress event, and a final suffix. The direct 197-event replay demonstrates
that the production parser accepts the exact retained 0.2.101 ordering rather than only the synthetic approximation.

### P1-2 `[closed]` — model provenance disagreement is diagnostic, not KPI admission authority

`src/x_first/adaptive_grok_wave_kpis.py:171-181` now admits a pair only when operator-projected local totals reconcile
with candidate structure and the verified session ledger; it no longer requires
`native_x_tool_provenance.tool_calls_reported` to equal the ledger. The new regression at
`tests/test_adaptive_grok_wave_kpis.py:297-323` proves that model provenance `0` versus operator ledger `1` remains an
accepted bundle and is surfaced as a nonzero discrepancy. Operator-owned throughput therefore remains usable even
when the model miscounts calls.

### P2-1 `[closed]` — evidence wording matches the persisted transport boundary

The wave2-v2 prompt now consistently calls retained rows `public-X-referenced, model-mediated` evidence rather than
source-bound evidence (`prompts/live-exploration/2026-07-15-google-deepmind-pretraining-recall-wave2-v2.md:7-8,25-28,89-92`).
The live evaluation document uses the same wording, and the recomputed prompt bytes match the policy hash. Nothing in
this remediation upgrades model-organized rows into native-payload-backed or campaign-admissible evidence.

## Finding

### P2-2 `[new]` — part of the discrepancy report labels operator projections as model-local diagnostics

`src/x_first/adaptive_grok_wave_kpis.py:277-299` reads `counts` and `local_reconciliation` from `sanitized.json`, but
those fields have already been overwritten by the operator projection. It nevertheless exposes them as
`model_result_count`, `model_local_validated`, and `model_local_reported`. The new regression makes the original model
local call claim `0`, then intentionally expects `model_local_reported=1` because the sanitized value is the operator
ledger projection (`tests/test_adaptive_grok_wave_kpis.py:303-322`). Candidate, evidence, Post-URL, local-call, and
local-tool deltas in this section are consequently guaranteed operator-equality checks, not model-versus-ledger
diagnostics.

This does not block the controlled exploration: `model_provenance_reported` is still the preserved typed model
diagnostic, its delta is correct, and all admitted throughput/evidence totals remain operator-owned. A bounded follow-up
should either rename the projected fields to `operator_projected_*` or bind the original raw model-local claims into a
typed receipt diagnostic before reporting them as model values.

## Accepted residuals and gate consequence

- Native-X result bodies remain absent. Candidate rows, excerpts, temporal labels, topology, and per-query yield are
  public-X-referenced but model-mediated and cannot enter campaign/product truth.
- Discovery-matrix completion and the three-expansion convergence rule remain prompt-governed. The ledger proves tool
  names and arguments, not X population exhaustion or native per-query marginal yield.
- The provider request ID and optional provider cost remain raw-artifact diagnostics. Operator session, strict outer
  usage, computed cost, grant, deadline, no-fallback, private retention, and cleanup remain the admitted owners.
- P2-2 is a diagnostic-naming debt. It does not change provider admission, operator totals, the model-provenance delta,
  or the one-shot live boundary.
- This verdict releases exactly one independently bounded Google DeepMind wave2-v2 exploration after its ordinary
  private request/hash, one-shot grant, deadline, emergency ceilings, no-fallback, and cleanup checks pass. It does not
  approve candidate/source truth, campaign admission, product promotion, ranking, canonical identity, export,
  outreach, or a claim of population exhaustion.

GO
