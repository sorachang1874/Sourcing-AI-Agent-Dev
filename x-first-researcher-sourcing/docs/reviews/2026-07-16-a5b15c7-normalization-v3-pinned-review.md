# Normalization v3 pinned independent review

## Evidence header

- Reviewer: independent non-author subagent `/root/pinned_normalization_v3_review`.
- Reviewed commit: `a5b15c78a22ecaa6ce77bee1c1b9b686cf4b83a2`.
- Exact first parent: `445daa0428fd470cc0784c05e4997a0c5cf9f96d`.
- Reviewed scope: the exact first-parent diff for six files under `x-first-researcher-sourcing`; `503 insertions`,
  `31 deletions`. Binary-diff SHA-256:
  `464b09dc2e29445400029e4e5ba39a52dcec4028f332eb5500f80fb73cac2046`.
- Reproduction tree: detached pinned worktree
  `/private/tmp/xfirst-review-a5b15c7-20260716` at the reviewed commit. Ambient working-tree changes were not review
  inputs.
- A second detached parent worktree at `/private/tmp/xfirst-review-445daa0-20260716` was used only to prove digest
  continuity. Private retained bundles were read only for de-identified replay and aggregate reconciliation. No live
  call, grant issuance/consumption, OAuth mutation, candidate text/handle output, implementation edit, staging,
  commit, or push occurred. This artifact is the reviewer's only repository write.

## Exact validation

- Excerpt boundary and raw preservation:
  - exactly `280` Unicode code points are unchanged;
  - strict-UTF-8 scalar strings of exactly `281..560` code points are replaced only by `value[:280]`;
  - `561+`, embedded NUL, and unpaired-surrogate inputs remain invalid;
  - no NFKC, stripping, ellipsis, semantic-window selection, or paraphrase occurs;
  - the raw headless/session output remains unchanged and hash-bound;
  - each affected candidate receives the deterministic semantic-incompleteness caveat and the result receives an
    exact count/maximum limitation.
- Atomic admission: the transformed copy is returned only when complete result validation succeeds. A different
  invalid URL, a duplicate created by prefix collision, an invalid candidate/limitation, or another remaining result
  error returns the original invalid object. Serialization is then rechecked against the request-frozen JSON byte,
  depth, and node envelope; overflow publishes no oversized `sanitized.json` and remains a typed technical-limit
  result.
- Semantic authority remains bounded. The prefix may omit claim-bearing tail text, which the committed regression test
  exercises explicitly. The affected typed supports, temporal states, confidence, overlap status, and immutable
  source fingerprint are not changed or upgraded; candidate/result audit text states that prefix semantic completeness
  is not guaranteed and linked-X-source review remains required. This is acceptable only within the existing
  model-mediated, non-promotable proposal lane; it is not source verification or a campaign/canonical assertion.
- Policy continuity was recomputed across the exact parent/current modules. For the same synthetic command binding:
  - parent current normalization-v2 digest equals current replay-only v2 digest exactly:
    `732eef849383c06b3e56248266dd3bbade336721b6fa930853f88715e8f82a74`;
  - v1-plus-artifact remains
    `b4e82657f93bfa210a5555a56af9154a82dc2692993e0c2fdae5ac3b8f9f469e`;
  - normalization-only v1 remains
    `9bde73fb529631672def9a7aa0cfdc5eaaaaa6f637381c26bc1c48206bf23570`;
  - pre-normalization result-v3 remains
    `22d92d736f45ccc73612aa2354a106d7f47523eb6c3cb66d0b11f3354da71d50`;
  - legacy plain and result-v2 digests remain
    `d5e04ce3c395519489ec15906dfc10b3a032491bda57a0c872429e2234bfbb14` and
    `45bb511c5a71fad701b455957393c0555b72e72652934644e48e5b7811f42e22`.
- The current v3 digest is used for new static bindings and grants. Receipt validation, command reconstruction, grant
  replay, bundle parsing, transcript-terminal recovery, and crash recovery each select the recorded current/v2/v1
  normalization version rather than silently applying v3 to an old run.
- Targeted normalization/transcript/bundle suite: `5/5` passed.
- Full standard-library suite: `386/386` passed in `52.699s`.
- Contract preflight: `status=valid`, `errors=[]`, precision/recall `1.0/1.0`, predicted/relevant `20/20`, false merges
  `0`. OpenAI, capability-probe, and Stage 2 fixture generators each reported `current`.
- Ruff: `All checks passed!`; Python compile: `45/45`; `git diff --check`: exit `0`, no output.
- Reviewer-only fixture crash probes exercised the newly added recovery routing that is not directly covered by a
  committed test: both normalization v2 and v1 produced `crash_recovered` receipts and then
  `validate_operator_bundle(...) == []` under their own recorded policies.
- Four real retained live bundles replayed with `[]` on pinned code:
  - `grok_wave_live_f41ad0d4206f4435baceb4bfe0b0e924`: legacy v1, `result_contract_invalid`;
  - `grok_wave_live_c4ec7140840942b2b1e62b5d078562f1`: legacy v2, `process_failed`;
  - `grok_wave_live_4324e8bce20c4e04b1a3b6e8a1a69418`: legacy v2, `result_contract_invalid`;
  - `grok_wave_live_2ad660aa20e34e93a92ef7c247ac8417`: legacy v2, `result_contract_invalid`.

## Private diagnostic reconciliation

The v5.1 retained bundle independently reproduced the document's provider and terminal facts: exit `0`, elapsed
`167,567 ms`, empty stderr, no fallback, `40/40` native-X calls (`26` keyword, `5` semantic, `4` user, `5` thread),
`89` events, one model turn, and token counts
`131,913 input + 495,104 cache-read + 20,100 output = 647,117 total`; conservative cost `$1.374634`.

The transcript-proven terminal assistant object contained `17` candidates, `32` evidence rows, `132` reported
observations, and `40` reported tool calls. Its excerpt lengths were min `43`, max `297`, `1/32` over `280`, and `3/32`
over `240`; its complete validator output was exactly `evidence_value_invalid:13:1`. Evidence, relationship, topology,
confidence, and temporal-state aggregate counts all match the committed diagnostic. A reviewer-only in-memory v3
counterfactual changed exactly row `(13, 1)` from `297` to `280`, added one candidate caveat plus one result limitation,
left the original object at max `297`, and produced validator errors `[]`. The sealed historical receipt remains v2 and
was not retroactively promoted.

## Findings and accepted residuals

Severity totals: `P0=0 / P1=0 / P2=1 / P3=1`.

### [P2][residual] Thread-fetch causal origin remains unproven

The inherited transport records typed thread-fetch arguments but not the native-X result body that caused a thread to
enter the turn. It proves a valid thread call, not official/project causal origin or per-thread yield. This is unchanged
from the accepted v5/v5.1 review residual and remains nonblocking because every discovery projection is unverified and
cannot authorize hydration, campaign admission, canonical writes, ranking, export, or outreach.

### [P3][new] Legacy normalization recovery routing has no committed end-to-end regression test

The implementation correctly adds normalization-v2 selection to live execution, bundle replay, receipt/grant replay,
and crash recovery, and reviewer-only v2/v1 crash probes passed. The committed suite, however, directly tests a legacy
v2 completed bundle but not a v2 or v1 incomplete-run recovery through `recover_incomplete_run`. This is a nonblocking
test-coverage gap, not an observed runtime failure. Add a compact parameterized v2/v1 recovery regression when this
runner next changes so policy-version routing cannot regress unnoticed.

No new P0, P1, or P2 finding was identified. This review authorizes only a fresh one-shot, no-fallback, separately
bounded live retry under normalization v3. It does not claim live success, source-bound X payloads, semantic fidelity
of a display prefix, population convergence, recall, precision, product readiness, or authority beyond the existing
model-mediated proposal lane.

GO
