# Google DeepMind wave2-v4 discovery-only pinned review

## Evidence header

- Reviewer: independent non-author subagent `/root/gdm_v4_pinned_review`; adversarial review against pinned Git objects.
- Reviewed head: `bbe36b87bda8bc34559daf078afbaf121d6117dc`.
- First parent: `17e1607fb46b48b45314bbc554a0d7a37830d434`.
- Reviewed scope: exactly the seven `x-first-researcher-sourcing/` files in the parent-to-head diff (`513 insertions`,
  `8 deletions`), with binary-diff SHA-256
  `9720bbc0ea6d9824234988c44e0a1ecd1d2f6deb582aaf29f73172fddb524515`.
- Reproduction environment: detached clean worktree `/private/tmp/x-first-bbe36b8-review` at the exact full SHA. The
  ambient working tree and any later uncommitted remediation were not read as review evidence.
- No Grok/X/model/network call, real OAuth or credential read, real grant issuance, provider execution, repository
  mutation, staging, commit, promotion, or product write occurred. Synthetic grants, binaries, auth bytes, session
  transcripts, and results existed only in automatically deleted temporary directories for mutation probes. This
  artifact is the only shared-working-tree write.

## Pinned validation

- `PYTHONPATH=src .../.venv/bin/python -m unittest tests.test_adaptive_grok_wave_runner -v` — exit `0`, `59/59`
  tests passed in `15.823s`.
- `PYTHONPATH=src .../.venv/bin/python -m unittest discover -s tests -v` — exit `0`, `348/348` tests passed in
  `38.822s`.
- `PYTHONPATH=src .../.venv/bin/python -m x_first.contracts` — exit `0`, status `valid`, precision `1.0`, recall
  `1.0`, predicted/relevant `20/20`, false merges `0`.
- `.../.venv/bin/ruff check src/x_first/adaptive_grok_wave_runner.py tests/test_adaptive_grok_wave_runner.py` — exit
  `0`, `All checks passed!`.
- The tracked v4 prompt hashes to
  `dedbd2044651a3edfdb12442b80eb24b3ce61a361fcfe24806d50c96978dc4ac`, exactly matching its effective-prompt
  registry entry.
- Current-policy normalization independently preserved raw `self`, emitted sanitized `third_party`, and replayed with
  zero bundle errors. Removing the terminal receipt and running crash recovery preserved the same downgrade and again
  replayed with zero errors. The pinned regression also proved that a pre-normalization result-v3 bundle remains
  rejected with raw/sanitized `self`, and the full suite preserved result-v2 replay.

Severity totals: `P0=0`, `P1=2`, `P2=1`.

## Findings

### P1-1 `[new]` — the discovery-only phase and convergence matrix are prompt prose, not an enforced runtime contract

The v4 prompt forbids every `from:<handle>` query, bare-handle/exact-name/known-person `x_user_search`, and per-person
corroboration (`prompts/live-exploration/2026-07-15-google-deepmind-pretraining-recall-wave2-v4-discovery-only.md:9-22`).
It also requires Top and Latest, historical shards, Reply discovery, thread expansion, every strategy family, and three
zero-yield expansions before discovery convergence (`:24-58,75-80`). However, the live transcript parser applies only
the phase-neutral `base_discovery_tool_arguments_allowed` envelope and then records generic tool counts/hashes
(`src/x_first/adaptive_grok_wave_runner.py:3602-3641`). Neither the request, effective-prompt entry, command policy,
grant, intent, receipt, session proof, nor operator projection owns a discovery-only phase or a mechanically reconciled
strategy-coverage state. The new test only asserts that four prose substrings exist
(`tests/test_adaptive_grok_wave_runner.py:685-694`).

Three pinned, synthetic end-to-end probes used the exact production v4 prompt hash and target:

1. A session containing `from:KnownPerson pretraining -filter:replies` sealed `receipt.status=completed`; bundle replay
   returned `[]`.
2. A session containing exact-person `x_user_search {"query":"KnownPerson","count":"50"}` also sealed `completed`;
   bundle replay returned `[]`.
3. A session with only one broad Latest keyword call, no Top, historical, Reply, semantic, user, or thread coverage,
   and no zero-yield convergence sequence sealed `completed` with sanitized `X_SEARCH_OK`; bundle replay returned
   `[]`.

This reproduces the prior performance failure mechanism the split is intended to remove: Grok may still spend the
wave on person hydration, or claim convergence without the required discovery matrix, and the operator will record a
valid completed bundle. The effective prompt hash proves which instructions were sent, not that their phase boundary
or coverage obligations were followed. Consequently the pinned commit cannot support a discovery-allocation,
marginal-yield, or convergence claim and does not clear another provider-costing live run.

Required remediation: make the execution phase and its strategy policy an explicit, versioned owner bound through the
effective-prompt entry, request/compiled prompt, command-policy digest, grant, intent, receipt, and replay. The session
validator must reject `from:` in discovery-only mode; person-shaped user searches need a closed discovery grammar (or
must be disabled until one exists). Reconcile the required Top/Latest, temporal, Reply, semantic/user/thread, and
zero-yield expansion cells from retained tool arguments, and prevent OK/completed discovery claims when those cells
are not satisfied. Add end-to-end regressions for the exact mutations above rather than prompt substring assertions.

### P1-2 `[new]` — the phase-neutral authored-surface downgrade contradicts the v4 discovery status semantics

The prompt says exact profile lookup and person hydration are intentionally out of scope and "must not be counted as
missing discovery work" (`...wave2-v4-discovery-only.md:75-80`). Yet the unchanged operator projection counts every
ambiguous or unsupported pretraining lead without both per-handle authored Post and Reply attempts, changes
`X_SEARCH_OK` to `X_SEARCH_PARTIAL`, and appends a missing-surface limitation
(`src/x_first/adaptive_grok_wave_runner.py:1434-1460`). V4 simultaneously forbids the only query pair that can satisfy
that generic rule.

The first pinned probe above retained one ambiguous lead. The prohibited `from:` call was accepted, the outer receipt
was `completed`, and the sanitized result was forcibly changed to `X_SEARCH_PARTIAL` with
`Operator downgraded ... required per-handle authored Post/Reply coverage is incomplete`, even though the v4 prompt
explicitly excludes that work from this phase. Therefore a compliant discovery-only execution cannot express
discovery convergence for an ambiguous lead, while a noncompliant hydration execution is not rejected. This corrupts
both the phase KPI and user-visible status owner.

Required remediation: make status projection phase-aware using the same explicit owner as P1-1. Discovery-only status
must be determined from discovery coverage/convergence and must not emit the hydration-surface limitation; the later
hydration phase should retain the existing strict per-handle Post/Reply downgrade. Add paired regressions proving the
same ambiguous lead is not penalized in a coverage-complete discovery phase but is downgraded when the bound hydration
phase lacks either surface.

### P2-1 `[new]` — normalization can publish a terminal artifact larger than its replay ceiling

`_parse_structured_stdout` checks the provider JSON byte/node limits before normalization, then appends a caveat and
global limitation and serializes the expanded sanitized copy without rechecking technical limits
(`src/x_first/adaptive_grok_wave_runner.py:3010-3037,3067-3075`; normalization at `:1318-1391`). Bundle replay later
reads `sanitized.json` under the original `max_json_bytes` ceiling (`:5787-5797`).

A pinned fixture probe set a valid request's `max_json_bytes=max_stdout_bytes=1,002,029`, exactly the valid raw result
length. The isolated self-author mismatch was the only model-contract error. The runner normalized it, published
`sanitized.json` at `1,002,574` bytes, and emitted `fixture_complete`; immediate bundle replay returned
`["sanitized_artifact_permissions_invalid"]`. Thus the new transformation can create a terminal bundle that fails its
own validator even though the raw input respected every configured ceiling.

Required remediation: reapply the byte and structural ceilings after normalization and after operator projection,
before publication/status selection. An expanded copy that crosses the bound must take a truthful technical-limit
terminal path, or the request must define a separately bound operator-artifact ceiling used consistently by execution,
recovery, and replay. Add the exact-boundary regression and require every generated terminal bundle to self-replay.

## Accepted residuals and verified non-findings

- The command-policy digest correctly binds `mechanical-evidence-relationship-downgrade-v1`. Current result-v3,
  pre-normalization result-v3, structured result-v2, and legacy-plain schema/policy pairings are kept distinct in
  receipt and bundle replay. No cross-version reinterpretation was reproduced.
- The normalization itself is narrow and monotonic at the model-contract layer: only non-Bio `self` rows with a
  case-insensitively different valid author are changed to `third_party`; malformed subject, URL author, Post ID,
  timestamp, thread relation, or support remains rejected; raw stdout is unchanged; candidate/global audit text is
  added; and no support/state/confidence/evidence row is upgraded. P2-1 concerns the missing post-transform technical
  envelope, not an authority upgrade.
- The v4 prompt has no business candidate, observation, query, or native-X-call cap and its prose breadth covers
  Posts, Replies, mentions, Top/Latest, historical eras, technical functions, semantic/user/thread expansion, and
  non-Bio evidence. P1-1 concerns enforceability and measurement, not missing prompt vocabulary.
- Model-mediated rows remain non-source-bound, source payload replay remains unavailable, and no product/campaign,
  identity, CRM, ranking, export, outreach, or canonical-write authority is granted. Those standing limitations are
  accepted for this exploratory lane and are not raised as new findings.

## Gate consequence

The two P1 findings block Google DeepMind wave2-v4 provider-costing live execution, performance/convergence claims, and
milestone promotion at this pinned commit. Fixes require a new pinned commit, targeted/full validation, and a fresh
non-author rereview. Unrelated fixture-only or other-model-home work remains outside this verdict.

NO-GO
