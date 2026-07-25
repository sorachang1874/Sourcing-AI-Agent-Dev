# Runner recovery pinned adversarial re-review — `c3ae046`

## Pinned evidence header

- Review type: non-author, adversarial, read-only Git-object re-review.
- Repository: `/Users/changyuyi/projects/Sourcing AI Agent Dev`.
- Commit: `c3ae04651a2fbc22df06c1d1037f608de05278fd`.
- Git first parent: `8744285b95d04f65bc4665e771d00507ee704b7e`.
- Commit tree: `692d69042f8bf530bfff80eebf4f0c337c9d78f1`.
- Parent tree: `6ad714bfaa076d3096f4ece8fcc2f4a87ad3d6b2`.
- Read source: independent `git archive` snapshots under
  `/private/tmp/x-first-c3ae046-review.sS2B4G`; mutable working-tree implementation files were not used as review
  evidence.
- Exact three-file scope:
  - `x-first-researcher-sourcing/src/x_first/adaptive_grok_wave_runner.py`
  - `x-first-researcher-sourcing/tests/test_adaptive_grok_wave_runner.py`
  - `x-first-researcher-sourcing/docs/reviews/2026-07-16-8744285-runner-recovery-rereview.md`
- First-parent scoped diff: 361 insertions, 46 deletions across exactly three files.
- Sorted scope-path digest (`git diff --name-only 8744285 c3ae046 -- <exact scope> | LC_ALL=C sort |
  shasum -a 256`): `00267d97cb848d3faa0783b2a860076a76aa3e47e5fd513be2910adb098fc6c0`.
- Binary scoped diff digest (`git diff --binary 8744285 c3ae046 -- <exact scope> | shasum -a 256`):
  `58606d88973f586765302e6050b698bb9b399787438d4caae8b0b543a48fb5ae`.
- Scoped blob and content digests:

  | Path | Git blob OID | SHA-256 of content |
  |---|---|---|
  | `src/x_first/adaptive_grok_wave_runner.py` | `d5bd8e3173350f5551d761590dd044b6ec88eb72` | `7adb38b9c1fff45ffbc86cabeff03c9fae67f534da0ce6b8a4c00d0ce3b4cfab` |
  | `tests/test_adaptive_grok_wave_runner.py` | `c726e933e5cb9d4dd86221161b3323fb5a85de7c` | `5ba0a8b943ffb9e2e8899b8f890156a7758ac6332daca835ba56ad45e92ea76d` |
  | `docs/reviews/2026-07-16-8744285-runner-recovery-rereview.md` | `30d1e155bf385916dc5e3f442c4780d98deaa1ce` | `5d449168643c91db34035ccf6e2de7cd55a8a796387bb56e30c548277ec39ed8` |

- Prior artifact replayed completely, including both P1 findings and their requested actual-producer boundaries.
- Finding totals for this re-review: P0 0, P1 1, P2 0, P3 0.

## Findings

### P1-1 — new: deleting an already-consumed grant ledger is treated as an unconsumed recovery until after ownership evidence is destroyed

The new early replay correctly validates a consumption record when one is present, but the loader returns `None` on
`FileNotFoundError` without classifying whether the run demonstrably crossed consumption
(`adaptive_grok_wave_runner.py:4564-4584`). Recovery then validates the grant at `intent.started_at` when the record is
absent (`:8119-8148`) and accepts an existing active claim after checking only its shared run/request/lease/grant
bindings (`:8149-8173`). It never requires consumption to exist when that claim's `claim_origin` is
`live_consumption`, nor when the bound process journal and ledger prove that process spawn was attempted. The missing
record therefore enters the same branch as a legitimate crash before consumption.

That ambiguity is not harmless cleanup. Recovery proceeds through process handling (`:8176-8248`), publishes retained
spool/session artifacts, audits and taints copied auth, deletes the ephemeral home, and resolves the active claim
(`:8335-8379`). Only receipt construction leaves `approval.consumption_sha256` unset (`:8458-8460`); the receipt
validator then rejects the spawned live process without consumption and raises `recovery_receipt_invalid`
(`:7037-7040,8615-8618`). The failure is reported after the evidence and ownership mutations it was required to
precede.

A provider-free probe used an incomplete live run produced by the actual archived
`b306be6f79b918e96537b106b85ffe18de13ff3a` implementation. Before tampering it had a mutually bound v2 process
journal, process ledger, ephemeral auth/session home, active claim with `claim_origin=live_consumption`, grant, and
consumption. Removing only the consumption file and recovering under `c3ae046` did not fail during grant/consumption
preflight. It called the process-group liveness hook twice, removed the home and active claim, removed both spools,
published raw/stderr/session/sanitized artifacts, and added an auth-taint marker. It finally raised
`AdaptiveWaveValidationError: recovery_receipt_invalid` without publishing a terminal receipt. Thus ledger deletion
turns a previously recoverable, single-use approval chain into a permanently non-terminal evidence gap.

Fail before every process, journal, spool, auth, home, claim, or receipt mutation when an existing exactly bound
active claim has `claim_origin=live_consumption` but its exact consumption record is missing. The same requirement
should apply when durable process evidence proves provider release or spawn. Preserve a separate crash-before-
consumption path only when there is no live-consumption claim, no process ledger, and no journal/spool evidence of
spawn; that path must not be usable as a downgrade for a consumed run. Add an actual-producer deletion regression
that asserts a typed `recovery_grant_consumption_missing` failure, zero process-group/termination calls, byte-identical
home/claim/journal/spools, no taint, and no receipt.

## Prior-finding dispositions and residuals

- Prior P1-1 is closed for the requested cross-version boundary. Actual artifacts generated by `354e979`, `28e7d8b`,
  and `e7d9e045` share the transitional command-policy digest but lack the v2 marker/hash-bound journal shape; current
  intent/receipt/bundle validation rejects them and receipt-producing recovery fails without state mutation. An actual
  `b306be6` v2 terminal bundle validates, and an actual `b306be6` incomplete live run recovers with its journal hash.
  Removing the marker, receipt hash, or journal from the real `b306be6` objects fails closed. Six distinct older
  policy-digest families retain keyless recovery compatibility.
- Prior P1-2 is closed for its exact grant/consumption tamper cases. Against actual `b306be6` incomplete-live objects,
  grant-byte/hash mismatch, a consistently rebound but invalid grant policy/state, and consumption shape/binding
  tamper all fail before process-group inspection and preserve the complete tracked state with no receipt. The new
  finding is the adjacent absence-classification hole: the exact consumption validator is never reached after the
  record is deleted, even though the active claim and process evidence prove it must exist.
- Current and transitional marker/journal mismatch handling remains fail-closed, and the arbitrary unrecognized
  command-policy digest remains rejected. No regression was found in the six intentionally supported older digest
  families.
- A same-owner mutation between the early replay and later terminal bundle replay is still theoretically possible,
  but the auth-digest lock serializes module-owned grant operations. This review does not elevate that external
  same-owner race beyond the concrete missing-ledger blocker above.

## Validation

- Exact pinned runner suite:
  `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src /Users/changyuyi/projects/Sourcing\ AI\ Agent\ Dev/sourcing-ai-agent/.venv/bin/python -m unittest tests.test_adaptive_grok_wave_runner`
  — 114 tests in 35.671 seconds, all passed.
- Exact pinned Ruff scope:
  `/Users/changyuyi/projects/Sourcing\ AI\ Agent\ Dev/sourcing-ai-agent/.venv/bin/ruff check src/x_first/adaptive_grok_wave_runner.py tests/test_adaptive_grok_wave_runner.py`
  — all checks passed.
- Both unchanged intent and receipt Schema files parsed as JSON. The exact three-file first-parent scoped diff had zero
  whitespace errors.
- Actual-producer terminal probes covered the `354e979`, `28e7d8b`, `e7d9e045`, and `b306be6` generations. Actual
  `b306be6` incomplete-live probes covered valid recovery; grant hash/state/policy tamper; consumption tamper; and
  deletion of the consumption record while retaining a `live_consumption` claim and bound process evidence.
- Six provider-free incomplete-run probes covered every distinct older policy family: operator-result v1,
  operator-result v2, normalization-only v3, pre-normalization v3, legacy plain, and legacy structured-result v2.
- No credential source, provider/model call, private live artifact, candidate data, or mutable working-tree
  implementation file was accessed. Synthetic auth, binary, executor, and fixture material remained in private
  temporary directories.

## Final verdict

NO-GO
