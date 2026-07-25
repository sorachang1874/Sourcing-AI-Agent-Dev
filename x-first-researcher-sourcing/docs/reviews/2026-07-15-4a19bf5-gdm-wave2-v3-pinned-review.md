# Google DeepMind wave2-v3 pinned review

## Evidence header

- Reviewer: independent non-author subagent `/root/gdm_v3_nogo_artifact`; adversarial and read-only against pinned Git
  objects.
- Reviewed head: `4a19bf50e398767e194526fd2c7ddeff00ff7db4`.
- Parent: `ec00f605883e0e899aedd6e8d8ab8622085c6a58`.
- Reviewed scope: only the `x-first-researcher-sourcing/` subtree at the pinned commit. Its parent-to-head diff is
  `10 files`, `1,181 insertions`, `68 deletions`, with binary-diff SHA-256
  `f7425c5a9b278086c64907d9968cd313425c244f84719b03dcc6f9b86c0ead7d`.
- Reproduction environment: a temporary tree materialized from `git archive 4a19bf5`; no current working-tree
  implementation was inspected. The temporary tree was deleted after the probes.
- No Grok/X/model/network call, OAuth or credential read, grant issuance, provider execution, broad test suite,
  staging, commit, promotion, or product write occurred. This artifact is the only working-tree write.

## Validation

- Two targeted Boolean-scope mutation probes ran against the archived pinned objects.
- `from:TargetPerson pretraining -filter:replies OR pretraining` was classified as
  `targetperson/authored_post`.
- `from:TargetPerson pretraining filter:replies OR pretraining` was classified as
  `targetperson/authored_reply`.
- Supplying those two credited attempts for an `ambiguous` pretraining candidate preserved the model's
  `X_SEARCH_OK`; live-result validation returned `[]`.
- No prior-reviewer suite totals are reused in this artifact.

Severity totals: `P0=0`, `P1=1`, `P2=0`.

## Finding

### P1-1 `[re-raise]` — Boolean escape branches can forge the mandatory per-handle Post/Reply completion gate

The query classifier counts operators, but does not prove that every Boolean branch remains inside the sole positive
`from:<handle>` scope (`src/x_first/native_x_evidence_contract.py:85-111`). Consequently each mutation above receives
one of the two required surface credits even though its trailing `OR pretraining` branch is global and may return only
unrelated authors.

That was previously recorded as a non-blocking P2 while authored-surface coverage was diagnostic. At this pinned
commit it crosses a stricter boundary: the wave2-v3 prompt makes both genuinely handle-scoped queries mandatory for
every retained `ambiguous|unsupported` pretraining lead and requires partial status when the pair is incomplete
(`prompts/live-exploration/2026-07-15-google-deepmind-pretraining-recall-wave2-v3.md:82-91,107-110`). The operator then
uses the credited attempt set as the sole condition for deciding whether an unresolved lead is downgraded from
`X_SEARCH_OK` (`src/x_first/adaptive_grok_wave_runner.py:1353-1380`). The existing positive-path test injects already
classified surface labels and therefore cannot detect the Boolean-scope escape
(`tests/test_adaptive_grok_wave_runner.py:1641-1700`).

The pinned mutation proves the end-to-end consequence: two globally escapable searches satisfy both attempt labels,
leave an unresolved candidate at `X_SEARCH_OK`, and pass result validation. This is not merely a KPI naming issue;
it lets a live run claim the prompt's mandatory per-handle evidence audit completed when neither full query is
candidate-scoped.

## Required remediation and gate consequence

- Fail closed unless the complete query belongs to a closed single-handle grammar whose Boolean structure cannot
  escape `from:<handle>`; alternatively reject Boolean operators for credited per-handle hydration attempts.
- Add both exact mutations above as regressions through the transcript-to-attempt classifier and operator projection,
  proving that neither can jointly preserve `X_SEARCH_OK` for an unresolved candidate.
- Re-run the targeted runner suite and complete X-first suite on a new pinned remediation commit, then obtain a fresh
  non-author review artifact.
- Until then, this commit does not clear Google DeepMind wave2-v3 live execution, provider-costing validation, or a
  milestone/performance claim. It also does not change the standing boundary that model-mediated rows are not
  source-bound campaign or product truth.

NO-GO
