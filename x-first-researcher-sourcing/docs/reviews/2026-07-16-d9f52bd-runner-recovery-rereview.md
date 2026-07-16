# Runner recovery pinned adversarial re-review — `d9f52bd`

## Pinned evidence header

- Review type: non-author, adversarial, read-only Git-object re-review.
- Repository: `/Users/changyuyi/projects/Sourcing AI Agent Dev`.
- Commit: `d9f52bdf9213575331d3d5fab5584ac2352de0dc`.
- Parent: `b306be6f79b918e96537b106b85ffe18de13ff3a`.
- Commit tree: `77ec3dafa78206fd11d01658bdd8fd26c7418c9d`.
- Read source: independent `git archive` snapshots under
  `/private/tmp/x-first-d9f52bd-review.ptMU73`; mutable working-tree implementation files were not used as review
  evidence.
- Exact five-file scope:
  - `x-first-researcher-sourcing/contracts/x.grok.adaptive_recall_wave.intent.v2.schema.json`
  - `x-first-researcher-sourcing/contracts/x.grok.adaptive_recall_wave.operator_receipt.v3.schema.json`
  - `x-first-researcher-sourcing/docs/reviews/2026-07-16-b306be6-runner-recovery-rereview.md`
  - `x-first-researcher-sourcing/src/x_first/adaptive_grok_wave_runner.py`
  - `x-first-researcher-sourcing/tests/test_adaptive_grok_wave_runner.py`
- Mechanical diff: 560 insertions, 32 deletions across exactly five files.
- Sorted scope-path digest (`git diff --name-only | LC_ALL=C sort | shasum -a 256`):
  `711bba32ef1191bf8437d21c9f494139c14271a7723101b8eff034845a16cb61`.
- Binary diff digest (`git diff --binary b306be6f79b918e96537b106b85ffe18de13ff3a d9f52bdf9213575331d3d5fab5584ac2352de0dc | shasum -a 256`):
  `a650a093c1d895eccb08396f4a88181830a76a9ef72b6f9235c005658fce662e`.
- Scoped blob and content digests:

  | Path | Git blob OID | SHA-256 of content |
  |---|---|---|
  | `contracts/x.grok.adaptive_recall_wave.intent.v2.schema.json` | `f297d6329136a71b9b9cd695e9d0504c37d085d1` | `b71c3532e6bdba027d97b9714b8acafe458c6eaa51117409d08fb640e15bc238` |
  | `contracts/x.grok.adaptive_recall_wave.operator_receipt.v3.schema.json` | `88262e4240f1ed19558334a3bada88a5a5452d80` | `55865014fc6ac68066dc7ae1d79875844be11f37587982bc194b7fae0714a1e8` |
  | `docs/reviews/2026-07-16-b306be6-runner-recovery-rereview.md` | `90b4290538b1a85ceded2fb6048591c4f153d118` | `aed43641b99ca586fa594a1e23665b054c856f8e897a5dd9b4f0e1489b7cfa07` |
  | `src/x_first/adaptive_grok_wave_runner.py` | `c3a33d0414cde298e6e05299e7d5dca8b82e7277` | `0722281d7188bb866704ed35e70528f8482853781b8b3f2fb23b982b8299b082` |
  | `tests/test_adaptive_grok_wave_runner.py` | `9660eee55853c4e70a1ca6e167e0411d1a899f76` | `1a69064cc2dc92cde1da28e038ef3734dfa5e7bae7bc9ce8fd1a69379bc039c1` |

- Prior artifact replayed completely: `docs/reviews/2026-07-16-b306be6-runner-recovery-rereview.md`, including
  prior P1-1, P1-2, and P2-1.
- Finding totals for this re-review: P0 0, P1 2, P2 0, P3 0.

## Findings

### P1-1 — new: the direct-parent process-evidence shape is classified as legacy and rejected during rolling replay and recovery

The repair correctly makes the new command-policy digest independently select the current process-evidence
generation (`adaptive_grok_wave_runner.py:2373-2393,2470-2486`). However, the direct parent `b306be6` already wrote
`process_evidence_generation` in the intent, `process_result_journal_sha256` in the receipt, and a spool-bound v2
process journal while still using the pre-repair command-policy digest. The new classifier maps that digest to
`legacy`. Runtime then requires a legacy intent to omit the generation key (`:6247-6288`), requires a legacy receipt
to omit the journal hash (`:6963-6976`), and flags either current-shaped key on a legacy policy during bundle replay
(`:7215-7250`). Recovery invokes `_intent_valid` before it can inspect or terminate a recorded process
(`:7994-7999`).

A provider-free cross-version terminal probe generated an unchanged fixture bundle from the archived parent code.
That bundle carried command-policy digest
`e15e1b9e75b775de0d1cd341a4ae7575186570fcc91c0c112eb84ce60d82a535`, the intent generation key, and the receipt
journal hash. Both repaired JSON Schemas accepted the parent intent and receipt shapes. Under the pinned current
runtime, however, the policy classified as `legacy`, `_intent_valid` returned false, receipt validation returned
`receipt_legacy_process_evidence_shape_mismatch`, and bundle validation returned
`intent_invalid`, `receipt_legacy_process_evidence_shape_mismatch`, and
`legacy_process_evidence_shape_mismatch`.

A second provider-free cross-version probe created an incomplete live-shaped parent run with synthetic auth, binary,
and executor material, then faulted after executor return while session evidence was being measured. The parent run
retained its ephemeral home and active-use claim as intended. Current recovery stopped at `intent_invalid`; it did
not reach recorded-process termination or publish a terminal receipt, and the home and claim remained. A rolling
upgrade can therefore strand exactly the direct-parent runs that the recovery protocol is intended to seal.

The pre-generation objects and the `b306be6` transitional objects share the old command-policy digest but have
different evidence shapes. Compatibility must explicitly recognize that bounded transitional generation (or migrate
it under an independently replayable binding) rather than treating the old digest as proof that every object is
keyless. Add terminal-replay and incomplete-recovery tests that produce artifacts with the direct-parent code and
consume them with the current code.

### P1-2 — new: recognized keyless legacy incomplete runs synthesize a current-shaped receipt and fail their own recovery validation

Recovery recognizes the bounded set of legacy command-policy digests and validates a keyless legacy intent
(`adaptive_grok_wave_runner.py:8005-8024`). But its receipt builder unconditionally emits
`process_result_journal_sha256`, even when the recovered run has no process journal (`:8481-8495`). Receipt validation
then requires the legacy artifact key set, which excludes that field (`:6963-6976`), and recovery raises
`recovery_receipt_invalid` before publication (`:8519-8528`). The normal execution path already removes the field
for a legacy generation (`:6194-6195`); the recovery path lacks the equivalent generation-dependent projection.

A provider-free incomplete-run probe transformed fixture-owned evidence into a recognized, keyless
pre-process-evidence legacy intent/policy and removed the process journal. `_intent_valid` returned true, proving that
the intended legacy input boundary was reached. `recover_incomplete_run(...)` then raised
`recovery_receipt_invalid`, and no receipt was published. The added pre-generation compatibility test covers
terminal schema/runtime replay, not recovery of an incomplete legacy run
(`tests/test_adaptive_grok_wave_runner.py:5291-5345`). Thus every genuinely keyless legacy incomplete run that
otherwise reaches receipt construction remains unsealable.

Build the recovery artifact map from the independently classified generation: omit the journal hash for keyless
legacy evidence and require the hash for the current generation. Add an exact legacy incomplete-recovery regression
that reaches terminal receipt publication and full bundle replay.

## Prior-finding dispositions and residuals

- Prior P1-1 is closed for bundles newly generated by this commit: the process-evidence policy version is included in
  the independently replayed command-policy digest, and deletion of the current intent key, receipt hash, journal, or
  spool binding is rejected (`adaptive_grok_wave_runner.py:2373-2393,2470-2486,7215-7250`). The direct-parent
  transitional shape is a distinct rolling-compatibility blocker recorded as new P1-1 above.
- Prior P1-2 is closed for its exact durable-spawn ownership boundary. `persist_spawn` transfers cleanup ownership to
  recovery immediately after ledger publication and before target release/executor return (`:5814-5854`); ownership
  remains deferred through journal, session, and spool sealing (`:5883-5964`). The exact post-spawn/pre-return and
  post-return fault tests preserve the home, active claim, ledger, and source transcript.
- Prior P2-1 is closed at the JSON Schema boundary: each unchanged schema ID now explicitly accepts both documented
  property sets, and both parent-shaped objects validated. The added terminal pre-generation replay test also passes.
  End-to-end incomplete-run compatibility is not closed: direct-parent transitional evidence is rejected before
  recovery and keyless legacy recovery constructs an invalid receipt. Those service-level failures are represented by
  P1-1 and P1-2 above.
- Residual: direct-parent transitional evidence cannot be treated as equally strong as the newly policy-bound
  generation. If accepted for compatibility, it needs an explicit bounded legacy label and retention/migration rule;
  it must not silently acquire the stronger current trust level.
- Residual: current-generation journal/spool integrity, durable-spawn cleanup ownership, and fault preservation are
  materially stronger and pass their exact regression cases. They do not compensate for an upgrade path that cannot
  consume already persisted runs.

## Validation

- Exact pinned runner suite:
  `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src /Users/changyuyi/projects/Sourcing\ AI\ Agent\ Dev/sourcing-ai-agent/.venv/bin/python -m unittest tests.test_adaptive_grok_wave_runner -v`
  — 108 tests in 21.897 seconds, all passed.
- Exact pinned Ruff scope:
  `/Users/changyuyi/projects/Sourcing\ AI\ Agent\ Dev/sourcing-ai-agent/.venv/bin/ruff check src/x_first/adaptive_grok_wave_runner.py tests/test_adaptive_grok_wave_runner.py`
  — all checks passed.
- Both changed schema files parsed as JSON. The direct-parent intent and receipt shapes also validated against the
  repaired schemas.
- Exact five-file pinned diff check returned zero whitespace errors.
- The exact current-generation regressions for journal/hash binding, durable-spawn ownership, and two post-return
  session-sealing failures passed. The terminal pre-generation schema/runtime compatibility regression also passed.
- Three additional cross-version/legacy probes used only archived code, synthetic fixture material, temporary private
  directories, and provider-free executors. No credential source, provider/model call, private live artifact,
  candidate data, or mutable working-tree implementation file was accessed.

## Final verdict

NO-GO
