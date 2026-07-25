# Adaptive Grok runner recovery pinned final review — `31960cd`

## Pinned evidence header

- Review type: non-author, adversarial, read-only Git-object final review.
- Repository: `/Users/changyuyi/projects/Sourcing AI Agent Dev`.
- Commit: `31960cdca71348a344328e00556828c0c0c6ae28`.
- Git first parent: `fcd560fdd5493417eb7e0e08acdf54594a9ebd65`.
- Commit tree: `2131c1e6116de29039a4f38e8cc28e651358325d`.
- Parent tree: `f005a873cd6e3de756cc6c45c89d54722f9dc232`.
- Read source: an independent `git archive` extraction at
  `/private/tmp/xfirst-31960cd-review.VNSDte`; mutable working-tree implementation and test files were not used as
  review evidence.
- Exact three-file first-parent scope:
  - `x-first-researcher-sourcing/docs/reviews/2026-07-16-c3ae046-runner-recovery-rereview.md`
  - `x-first-researcher-sourcing/src/x_first/adaptive_grok_wave_runner.py`
  - `x-first-researcher-sourcing/tests/test_adaptive_grok_wave_runner.py`
- Scoped diff: 186 insertions, 0 deletions across exactly three files.
- Sorted scope-path SHA-256:
  `6a1a01fed2f119426a03363de56ffee93fc6c98733f43b2d23cfad50e88b9ef9`.
- Binary scoped-diff SHA-256:
  `dea36f7e7e9ba42c5122b6df6d12cabc4e48779463f4e96ca6162da989714878`.
- Scoped blob and content digests:

  | Path | Git blob OID | SHA-256 of content |
  |---|---|---|
  | `docs/reviews/2026-07-16-c3ae046-runner-recovery-rereview.md` | `805cab69206ed5625dcfe8278f94f309d8fb4095` | `1657a6cf5cc4deae088abab17e31107b8a52761ef7162f416218ae9504c77690` |
  | `src/x_first/adaptive_grok_wave_runner.py` | `616498fb34dcea24cd31ca28feaf40f621d4ec21` | `428092d2466a36eacc334716e62b4ccdf0629c541e651cca3f292e4b69ee891e` |
  | `tests/test_adaptive_grok_wave_runner.py` | `b824025ba6cb655a3a0a74713f635f593c1b77f7` | `f1e017aaee19bfe6c94299ee635b5e4eacf4e96b8e77018b466e8e54119ae33a` |

- Finding totals: P0 0, P1 1, P2 0, P3 0.

## Findings

### P1-1 — re-raise: deleting or origin-downgrading the claim still bypasses the consumed-ledger pre-mutation fence

The patch correctly closes its one tested shape: a missing consumption record plus a retained, exactly bound active
claim whose `claim_origin` is `live_consumption` now raises `recovery_grant_consumption_missing` before process or
artifact mutation (`adaptive_grok_wave_runner.py:8172-8183`). An actual-producer synthetic probe confirmed zero
process-group, identity, or termination calls; byte-identical home auth, claim, journal, ledger, and both spools; no
taint; and no receipt.

It does not close the broader requirement from the prior review: durable current process evidence must independently
prove that the run crossed the consumption boundary. A missing consumption file is still normalized to `None`
(`:4564-4584`). When the active claim is also absent, recovery immediately publishes a new `legacy_recovery` claim
(`:8149-8162`) before it even reads the bound process-result journal or process ledger (`:8188-8231`). When the claim
is present but its origin is `legacy_recovery`, the new check is skipped; that origin is schema-valid (`:685,
3897-3911`), and the exact-owner matcher deliberately does not bind origin (`:4022-4038`). In both cases, the
current v2 journal and ledger are accepted and process-group liveness is inspected (`:8231-8260`), spools are
promoted (`:8266-8319`), session evidence is published, auth is tainted/audited, the ephemeral home is deleted, and
the claim is resolved (`:8330-8418`). Only after those mutations does receipt construction omit
`approval.consumption_sha256` (`:8470-8472`) and fail because a spawned live run cannot be valid without consumption
(`:7037-7040, 8627-8630`).

Two provider-free probes used incomplete live artifacts produced by the exact pinned runner with synthetic auth,
binary, and executor. Before perturbation, each had a mutually bound v2 journal, process ledger, copied-auth home,
`live_consumption` claim, and consumption ledger.

1. Deleting both consumption and active claim caused two process-group liveness calls, deleted the home and both
   spools, published raw stdout, stderr, session updates, and sanitized output, added an auth-taint marker, and then
   raised `AdaptiveWaveValidationError: recovery_receipt_invalid` with no receipt.
2. Deleting consumption and rewriting only `claim_origin` from `live_consumption` to the otherwise valid
   `legacy_recovery` value produced the same late failure and evidence mutations, and additionally removed the
   rewritten claim.

This is a repeatable downgrade, not merely an unavailable-artifact condition. A positive lineage control retained
the valid consumption ledger while deleting only the claim; recovery synthesized ownership, completed as
`crash_recovered`, and published a valid receipt. Therefore claim absence is intentionally compatible, but it cannot
be allowed to erase the meaning of a still-bound current journal/ledger when consumption is also absent.

Move the missing-consumption classification ahead of every process call and every auth, claim, spool, journal, home,
session, sanitized-output, taint, and receipt mutation. At minimum, a live run with no exact consumption must raise
the typed pre-mutation error if any independently bound evidence proves it reached or passed consumption: an existing
`live_consumption` claim, a current process-result journal, or a process ledger. Do not trust mutable
`claim_origin=legacy_recovery` to override current process evidence. Keep the no-consumption path only for a genuine
pre-consumption crash with no live-consumption claim, journal, ledger, or process spool evidence. Add actual-producer
regressions for (a) claim plus consumption deletion, (b) consumption deletion plus origin rewrite, (c) a current
no-spawn executor-return journal, and (d) an abrupt pre-consumption crash; assert zero process hooks and byte-identical
home/claim/journal/ledger/spools for every consumed-evidence failure.

## Matrix and residual disposition

- Exact patched case, retained `live_consumption` claim plus deleted consumption: closes correctly and fails before
  mutation. The added regression is useful, but it does not assert zero process hooks or byte-identical ledger/spools.
- Legitimate abrupt crash immediately before `_load_and_consume_grant`: the produced state had no claim,
  consumption, journal, ledger, or spools. Recovery made zero process calls, retained home/auth, created only a
  `legacy_recovery` claim, emitted no taint or receipt, and failed closed with
  `recovery_process_identity_unavailable`. This remains distinct from the consumed-evidence cases and was not used to
  justify the finding.
- Valid consumption plus missing claim: recovers successfully, confirming intended recovery lineage compatibility.
- Direct-parent transitional incomplete live recovery, keyless transitional rejection without journal mutation,
  grant tamper preservation, missing-consumption retained-claim preservation, and clean post-consumption/no-ledger
  behavior all pass their pinned targeted tests. No transitional regression was found; the blocker is the uncovered
  current-evidence downgrade.
- The previous finding's process-evidence clause remains unresolved. The one new conditional narrows the hole to a
  single mutable claim field instead of deriving consumed state from all durable evidence.

## Validation

- Exact pinned runner suite:
  `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src /Users/changyuyi/projects/Sourcing\ AI\ Agent\ Dev/sourcing-ai-agent/.venv/bin/python -m unittest tests.test_adaptive_grok_wave_runner`
  — 115 tests in 37.091 seconds, all passed.
- Five named recovery/tamper/transitional tests — 5 tests in 1.338 seconds, all passed.
- Exact pinned Ruff scope:
  `/Users/changyuyi/projects/Sourcing\ AI\ Agent\ Dev/sourcing-ai-agent/.venv/bin/ruff check --no-cache src/x_first/adaptive_grok_wave_runner.py tests/test_adaptive_grok_wave_runner.py`
  — all checks passed.
- Intent v1/v2 and operator-receipt v1/v2/v3 schemas all passed the runner's strict duplicate-key JSON parser.
- Exact three-file first-parent `git diff --check` passed with no whitespace errors.
- No credential source, provider/model call, private live artifact, candidate data, or mutable working-tree
  implementation/test file was accessed. All runtime probes used synthetic, provider-free material in private
  temporary directories.

## Final verdict

NO-GO
