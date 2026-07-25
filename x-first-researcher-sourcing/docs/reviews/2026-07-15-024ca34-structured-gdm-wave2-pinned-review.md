# Structured Grok transport and Google DeepMind wave2-v2 pinned review

## Evidence header

- Reviewer: independent non-author subagent `/root/gdm_wave2_pinned_review`; adversarial review against pinned Git
  objects only.
- Base: `96c07c578c6a7641c20a71d8df50c3b56b1627e8`.
- Reviewed head: `024ca34a3420a712188e3eabd90d26c4996abb1c`.
- Ancestry: the base is an ancestor of the reviewed head. The intervening commits are unrelated
  `sourcing-ai-agent` work; this review used only the `x-first-researcher-sourcing/` subtree diff.
- Reviewed scoped binary-diff SHA-256:
  `10c3260beb4197e30961b8aee53cf9daee29a9e67e064d34a98dce95e4d25a56`
  (`7 files`, `1,002 insertions`, `129 deletions`).
- Detached clean worktree: `/private/tmp/x-first-024ca34-structured-gdm-review`.
- The main working-tree implementation and any later remediation were not read or used as implementation evidence.
  The only main-tree write made by this reviewer is this artifact.
- The retained, already-completed Google DeepMind wave1 transcript was used only as an empirical Grok 0.2.101
  protocol fixture. No OAuth bytes or provider result bodies were read. No Grok/X/model/network call, grant issuance,
  provider execution, commit, promotion, or product write occurred.

## Validation

- Targeted runner suite: `54/54` passed in `11.756s`.
- Complete pinned X-first suite: `342/342` passed in `38.945s`.
- Whole pinned X-first Ruff check: passed.
- `PYTHONPATH=src ... -m x_first.contracts`: `status=valid`, `errors=[]`, synthetic precision/recall `1.0/1.0`.
- `git diff --check 96c07c5..024ca34 -- x-first-researcher-sourcing`: passed; detached status remained clean.
- Google DeepMind wave2-v2 prompt SHA-256 recomputed as
  `df61bbb210840d646a621a2a869dcf63f0e809e72292b693f3c4db58c42c3ba8`, matching its effective-policy row.
- Pinned-code replay of the retained pre-headless Google DeepMind live bundle returned `[]` by both absolute and
  relative run-root paths. This validates the intended legacy command-policy compatibility without relabelling that
  old noncompliant run as successful.
- Adversarial 0.2.101 protocol replay used the retained 197-event log and a synthetic strict headless envelope. Its
  event order is `retry_state(0)`, `user_message_chunk(1)`, `agent_thought_chunk(2)`,
  `agent_message_chunk(3)`, then the first `tool_call(4)`. The pinned parser raised
  `session_assistant_causality_invalid`; this reproduces P1-1 below without a provider call.

Severity totals: `P0=0`, `P1=2`, `P2=1`.

## Findings

### P1-1 `[new]` — the structured path still rejects the exact observed pre-tool progress event

`src/x_first/adaptive_grok_wave_runner.py:3350-3353` accepts an `agent_message_chunk` only after at least one tool has
started and every started tool has completed. The retained real Grok 0.2.101 transcript emits a progress message
before its first tool call, while all later progress chunks occur between completed tool batches. The new regression
at `tests/test_adaptive_grok_wave_runner.py:1180-1220` covers a retry and interleaved progress only **after** the first
tool completes, so all 54 tests pass while the production dialect fails.

Changing stdout from plain mode to the strict headless envelope does not change this session-event order. A new live
wave can therefore finish its provider work and valid outer/inner JSON yet still lose `SessionProof`, terminate as
`provider_evidence_invalid`, and remain ineligible for KPI analysis. This directly blocks the reviewed one-shot
Google DeepMind wave2-v2 execution.

The bounded correction is to admit a well-formed progress `agent_message_chunk` after the single user event and while
no tool is outstanding, including before the first tool. Keep the final global requirement that a successful live
result has at least one completed native-X call, all started calls complete, a final assistant suffix exactly equal
to outer `text`, and no event after a terminal. Add the exact
`retry -> user -> thought -> progress -> first tool` ordering as a regression and replay the retained event-shape
fixture through the parser.

### P1-2 `[new]` — KPI validation still treats model-reported call count as ledger truth

The reviewed runner intentionally keeps model provenance diagnostic while replacing local reconciliation with
operator facts (`src/x_first/adaptive_grok_wave_runner.py:1280-1320`). Its new regression proves a completed bundle may
legitimately contain `model tool_calls_reported=0` and `session ledger=2`
(`tests/test_adaptive_grok_wave_runner.py:1234-1290`). This matches the already observed class of model discrepancy in
wave1 (`97` reported versus `94` replayed).

The downstream KPI owner was not updated. `src/x_first/adaptive_grok_wave_kpis.py:171-183` still requires
`native_x_tool_provenance.tool_calls_reported == session_proof.completed_tool_calls` and raises
`result_receipt_reconciliation_mismatch` otherwise. Consequently the newly accepted operator/model disagreement
cannot pass either pair or full-bundle KPI analysis, and the later `model_reported_vs_ledger_discrepancies` metric can
never observe the nonzero discrepancy it was designed to report. This blocks the requested end-to-end performance
review even after P1-1 is corrected.

The KPI validator should require only operator-owned candidate/evidence/Post-URL/local tool totals to equal the
receipt/session ledger. Model provenance remains a closed, typed, nonnegative diagnostic and should feed the delta
output without becoming an admission predicate. Add a KPI regression using the completed mismatch bundle already
constructed by the runner test.

### P2-1 `[new]` — wave2-v2 calls model-mediated rows “source-bound” despite the persisted transport boundary

The prompt says prior updates and newly retained rows need “source-bound” evidence
(`prompts/live-exploration/2026-07-15-google-deepmind-pretraining-recall-wave2-v2.md:7-8,24-26,86-90`), and the live
evaluation doc repeats the phrase. However, the same pinned documentation records that Grok 0.2.101 persists only
tool names/arguments and not native-X result bodies
(`docs/live-evidence/2026-07-15-grok-cli-profile-and-performance-evaluation.md:350-357`). The compiled operator suffix
correctly says these are model-organized, non-replayable discovery leads.

This wording cannot cross the blocked campaign bridge or upgrade candidate truth, so it is not an additional live
execution blocker. Replace it with “public-X-referenced, model-mediated unverified evidence” (or equivalent) to keep
the prompt, receipt, KPI, and hydration vocabulary aligned.

## Confirmed closures

- The live argv now binds `--output-format json` and the exact result-v2 `--json-schema`; the outer envelope is closed,
  session-bound, duplicate-key/nonfinite checked, and owns `EndTurn`, turns, and usage.
- Model candidate/evidence arrays are validated before operator projection. Candidate, evidence, Post-URL, native-X
  call, and per-tool totals in `sanitized.json` are then recomputed from local structure/session facts; the original
  model diagnostics remain in hash-bound raw stdout.
- Optional provider `total_cost_usd` must be nonnegative and, when floating-point, finite; it is retained only in raw
  stdout and cannot override budget accounting derived from outer token usage plus request-pinned pricing.
- Legacy plain bundles select their replay-only command digest and actual argv, while new grant issuance binds only
  the structured command policy. The retained real live bundle and relative path normalization replayed cleanly.
- The effective-prompt policy binds the exact Google DeepMind target and wave2-v2 bytes. Prior handles remain
  casefolded SHA-bound exclusions/material-update baselines and do not become evidence or a business stop condition.
- The discovery-first prompt materially improves the large-lab method: affiliation and pretraining remain independent;
  incomplete lab leads remain in recall; Top/Latest, historical shards, Post, Reply, thread, project/function and
  official-graph surfaces precede selective one-profile-per-handle hydration; there is no candidate/evidence/call
  business cap.

## Accepted residuals and gate consequence

- Native-X result bodies are still absent. Candidate rows, excerpts, relationship/topology labels, and per-query yield
  remain model-mediated and cannot enter campaign/product truth; this review does not change that boundary.
- The discovery matrix and three-expansion convergence rule are prompt-governed, not mechanically proven. The session
  ledger can audit exact calls and arguments, but without result payloads it cannot prove marginal yield or population
  exhaustion. A future live run may be evaluated as an exploration, never as exhaustion proof.
- The provider request ID and optional provider cost stay raw-artifact diagnostics. Operator session, usage, computed
  cost, grant, deadline, no-fallback, private retention, and process cleanup remain the admitted owners.
- P1-1 and P1-2 must be corrected in a new pinned commit and independently re-reviewed. Until then, fixture work and
  unrelated non-live development may continue, but the Google DeepMind wave2-v2 grant/live execution, performance
  closeout, campaign admission, candidate truth, product promotion, ranking, canonical identity, and outreach remain
  blocked.

NO-GO
