# GDM37 Wave P private controller exploratory pre-live review

Date: 2026-07-17

## Evidence header

- Review type: non-author, adversarial, provider-free pre-live review of one private controller and its prepared
  campaign root.
- Controller:
  `/Users/changyuyi/.local/state/x-first-researcher-sourcing/direct-grok-diagnostics/gdm-query-family-calibration-20260716T170000Z/gdm37_wavep_controller.py`.
- Controller SHA-256: `13006fda0b4be37f3bfb542f8772b8690925fe45d46ce3077ff353a613eba57f`.
- Prepared root:
  `/Users/changyuyi/.local/state/x-first-researcher-sourcing/direct-grok-diagnostics/gdm37-wavep-20260717T025840Z`.
- Frozen v3 source:
  `/Users/changyuyi/.local/state/x-first-researcher-sourcing/direct-grok-diagnostics/gdm-marginal37-technical-evidence-v3-20260716T165000Z/phase-e-search/campaign-input.private.json`.
- Source SHA-256: `39549a6a8f4af4f01305861bd2f6e73347ec0fdebb11b96b76f8851d6f824901`.
- Frozen source topology: 37 ordered, case-insensitively unique inputs; the v3 source and its declared v2 predecessor
  were byte-identical at review time.
- Input-order SHA-256: `b84a3fecd4d944c35fe9f2289983a188423790ad7a371ef165a3f6ae10abe088`.
- Review boundary: no Grok invocation, OAuth flow, provider/model call, native-X call, generic web request, or other
  network access occurred. No private candidate handle or status ID is reproduced in this artifact.

## Prepared artifact identities

| Artifact | SHA-256 |
|---|---|
| `frozen-input.private.json` | `965c7656bc62186646ef8344a1a366826ed639e4eb81edcd2b03362889c4611e` |
| `query-policy.private.json` | `26dc41c90a27cc09e4ef1659099866bb73e84d96bd371b639df5720540cf4ac4` |
| canonical query-policy value | `aec1a22ee75eef78a7654aa8853ad49c02f4a797008f607258d084be992fc917` |
| `campaign-intent.json` | `6e265d96b5d00a75a903b26ed698a53efc1c40c1336f61705e9a1f18a26b01e9` |
| `offline-self-test.json` | `b1e252dca09774a25f497a16614fe990d5a15b72687dc413beff4a7bd335d06a` |
| `candidate-free-dry-run-summary.json` | `978112df6dc4d38809841bddbe8dab451143097be6fcb47c1649356cd1124d4b` |
| `candidate-free-preparation-receipt.json` | `f238a76e4987160793b981a6f1f03ca480711ffcfa6028f3e65492f6ecc905ff` |

The prepared root contained one directory and six files at review time. The directory was exact mode `0700`; every
file was exact mode `0600`; no symlink was present. The campaign intent bound the controller, frozen input, stored
query-policy file, canonical query-policy value, source digest, and input-order digest.

## Frozen operating point

The initial operating point is three candidates per session and exactly two candidate-major calls per candidate. It
is a calibrated technical operating point, not a candidate, reference, or native-X business cap. All three business
cap fields remain `null`.

Both calls are `x_keyword_search` with `limit="10"` and `mode="Top"`, in this exact order for each input:

1. `from:{handle} (pretrain OR pretraining OR "pre-training" OR tokenizer OR tokenization OR "training data" OR "data mixture" OR scaling OR optimization OR "training stability" OR "distributed training" OR accelerator OR TPU OR multimodal OR "base model" OR "foundation model" OR "model training")`
2. `from:{handle} (dataset OR corpus OR objective OR loss OR "training recipe" OR curriculum OR deduplication OR filtering)`

A full three-input session therefore requires exactly six starts, six matching completions, and six literal terminal
blocks. Each block is delimited by its exact `BEGIN_CALL_NN_STATUS_URLS` and `END_CALL_NN_STATUS_URLS` markers and
contains only zero or more canonical X status URLs. CLI `--json-schema` forcing is absent. The calibrated
`--output-format json` choice wraps the headless stdout envelope; the literal flat terminal and tool ledger are read
from the retained session updates rather than from a model-authored JSON result.

## Provider-free validation

| Check | Result |
|---|---|
| Frozen source and predecessor | required SHA matched; byte identity matched; 37/37 order and casefold uniqueness matched the prepared frozen input |
| Stored and fresh offline self-test | both passed; persisted self-test SHA matched the pinned value |
| Built-in parser probes | 1 positive and 2 negative flat-terminal probes passed |
| Built-in ledger probes | 7/7 adversarial cases rejected with their expected stable errors |
| Independent ledger probes | 8/8 rejected: pretool prose, missing completion, reordered arguments, wrong author/block, extra unmatched start, duplicate completion, post-terminal tool start, and disallowed completed tool |
| Start/completion closure | exact ordered bijection, unique completion IDs, monotonic timestamps, one terminal message, and no semantic event after terminal enforced |
| Stable status handling | stable-ID dedupe passed; cross-input ID ownership and exact URL/author/block binding enforced |
| Saturation signal | retained count equal to the requested limit produced exactly one synthetic saturation signal; accepted saturated calls feed a private lineage queue |
| Invalid-batch behavior | invalid attempts normalized nothing and committed zero records; split topology was exactly `3 -> 2+1 -> 1+1` |
| Partial-attempt resume | two repeated scans preserved the partial source tree byte-for-byte, quarantined it once per scan, retained maximum sequence 7, bound the orphaned path, and selected a fresh `Q01` recovery path |
| Completed rejected resume | two repeated scans reused the same completed rejected path and sequence with zero quarantine and zero orphaning |
| Completed accepted resume | two repeated scans replayed the same accepted path and stable reference count with zero quarantine and zero orphaning |
| Canonical raw copy | all 6/6 files copied byte-for-byte; destination modes were `0700`/`0600`; a one-byte mutation failed hash binding |
| Hard attempt deadline | synthetic hung process hit the one-second test deadline, received `SIGTERM` then `SIGKILL`, remained rejected, and produced no normalized result |
| Headless command policy | read-only sandbox, disabled generic web, disallowed non-champion tools, no subagents, no memory, and no CLI JSON schema were present |
| Candidate-free outputs | four prepared non-private/operator-facing JSON files scanned with zero input-handle, status-ID, or X-URL value leaks |
| Closed replay entrypoint | zero retained live sessions, zero accepted/rejected sessions, zero quarantined/orphaned paths, and zero Grok invocations |

## Authentication and output-format decisions

Cached OAuth is an explicit operational assumption for this exploratory lane. The user manually refreshed the cached
OAuth session before this review. The current process exposed no `XAI`/`GROK` API-key or token environment variable,
and the active Grok configuration exposed no credential override key. That establishes that no environment-key
fallback was observed; it does not independently prove the cached token's identity or freshness. The CLI's explicit
`--oauth` option is a login-flow trigger and is not required on every already-authenticated headless call.

The retained `--output-format json` setting is also intentional. It is the empirically calibrated transport mode for
this exact exploratory lane. The controller does not accept the stdout wrapper as the literal result: acceptance
depends on the exact ordered start/completion ledger and the literal closed flat blocks copied from canonical session
updates.

## Finding totals

| Severity | Count |
|---|---:|
| P0 | 0 |
| P1 | 0 |
| P2 | 2 |
| P3 | 0 |

## Findings

### P2-1 — residual — canonical raw files are captured and hash-bound but are not yet deep-replayed as promotable proof

The controller copies and hashes the canonical six-file Grok set: `summary.json`, `updates.jsonl`, `events.jsonl`,
`chat_history.jsonl`, `system_prompt.txt`, and `prompt_context.json`. It binds the copied updates to the operator copy,
checks the summary's session/model/effort/cwd identity, checks the prompt-context working directory, and rejects any
later byte mutation against the retained manifest.

The controller deliberately stops short of the tracked workflow's full canonical-session replay. An independent
adversarial fixture with malformed `events.jsonl`, malformed `chat_history.jsonl`, and an empty `system_prompt.txt`
still passed this controller's shallow raw-session validator when its per-file hashes, summary identity, cwd, and
updates copy were internally consistent. Request identity, exact prompt/chat prefix, system-message equality, event
terminal state, backend-tool chat binding, and final-assistant/chat equality are therefore not proven here.

This is non-blocking for the bounded exploratory performance run because its accepted observations remain
`model_mediated_unverified` and the performance ledger is derived from the exact retained updates. It is blocking for
promotion, a tracked evidence claim, a milestone gate, or any assertion that the six files have already passed the
project's full `replay_grok_operator_session` boundary. That deeper replay remains deferred to the tracked
controller; this review does not transfer a tracked-proof `GO` to the private exploratory lane.

### P2-2 — residual — the campaign deadline is hard per process but resets across explicit resumes

Each attempt has a hard 900-second ceiling bounded by remaining campaign time. Timeout sends process-group `SIGTERM`,
waits five seconds, and sends `SIGKILL` if necessary; the independent synthetic kill probe exercised both signals and
confirmed zero acceptance.

The 28,800-second campaign timer is based on process-local monotonic time initialized by each `--execute-live`
invocation. A later explicit resume retains completed attempt checkpoints and does not rerun them, but it starts a new
campaign-duration window. Repeated operator resumes can therefore extend aggregate wall-clock time beyond one
28,800-second interval. This is acceptable for the explicitly supervised exploratory run, but it is not an absolute
cross-resume campaign deadline and must not be described as one.

## Scope decision

This review authorizes only the bounded exploratory live performance run against the exact controller SHA and
prepared-root identities above, under the cached-auth and calibrated-output assumptions. Any controller, frozen
input, query policy, or prepared-root binding change requires a new review.

This review does not authorize promotion, product integration, canonical writes, tracked-proof publication,
milestone signoff, or a claim that deep canonical raw-session replay is complete.

## Final verdict

GO_FOR_EXPLORATORY_LIVE_ONLY
